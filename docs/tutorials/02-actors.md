# Tutorial: actors

A *task* (see [01-tasks.md](01-tasks.md)) is one-shot: function in, value out, no state. An *actor* is a long-lived stateful subprocess. You define a class, instantiate it remotely, and method calls dispatch FIFO to its dedicated process. State persists between calls.

Actors are the right tool when you need:

- A counter, accumulator, or other mutable shared state across calls.
- Expensive setup (loading a model, opening a connection) you want to amortise across many requests.
- A serialisation boundary — only one method runs at a time, so you don't need locking inside the class.

By the end of this tutorial you'll know:

- Two ways to declare an actor class (and which one mypy likes).
- How method calls work — submission, execution, results.
- Lifecycle: terminate, restart-on-crash, post-budget death.
- Named actors and looking them up (`rayd.get_actor`).
- Pickling actor handles (passing them around as values).
- The cross-driver invocation story.

## Declaring an actor

Two equivalent forms:

**Decorator form** (concise, but mypy-blind):

```python
@rayd.actor
class Counter:
    def __init__(self, start: int = 0) -> None:
        self.x = start
    def increment(self) -> int:
        self.x += 1
        return self.x
```

**Assignment form** (mypy-friendly, recommended):

```python
class _CounterImpl:
    def __init__(self, start: int = 0) -> None:
        self.x = start
    def increment(self) -> int:
        self.x += 1
        return self.x

Counter = rayd.actor(_CounterImpl)  # Counter: ActorClass[_CounterImpl]
```

The assignment form gives you a precisely typed `ActorClass[_CounterImpl]`. The decorator form works at runtime but mypy can't propagate class-decorator return types ([python/mypy#3135](https://github.com/python/mypy/issues/3135)) — it sees `Counter` as `type[_CounterImpl]`, which means `mypy --strict` flags the `.remote(...)` call.

The rest of this tutorial uses the assignment form.

## Creating an actor and calling methods

```python
import rayd

rayd.init()
try:
    handle = Counter.remote(100)         # spawns a subprocess + instantiates _CounterImpl(100)
    print(rayd.get(handle.increment.remote()))  # 101
    print(rayd.get(handle.increment.remote()))  # 102
    handle.terminate()
finally:
    rayd.shutdown()
```

A few things are happening:

- `Counter.remote(100)` spawns a fresh per-actor subprocess (`python -m rayd._actor_worker`), connects to it via a UDS, instantiates `_CounterImpl(100)` inside, and returns an `ActorHandle`.
- `handle.increment.remote()` cloudpickles `((), {})` (the method args), sends an `actor_call` frame over the UDS, and returns an `ObjectRef` for the eventual result.
- The subprocess receives the frame, looks up `increment` by name on the instance, calls it, and seals the return value into shared plasma.
- `rayd.get(...)` blocks on the ref the same way it does for tasks.

**Method calls run FIFO.** Submit `inc()` then `inc()` then `inc()` and they run in that order in the actor's process. State mutations are sequenced, no locking needed inside `_CounterImpl`.

## Lifecycle: terminate

```python
handle.terminate()
```

`terminate()` sends a shutdown frame, joins the reader thread, and waits for the subprocess to exit. Any in-flight method calls finish first; new calls after `terminate()` raise `RuntimeError("actor has been terminated")`.

`terminate()` is **idempotent** — calling it twice is fine. It's also called automatically when the `_ActorSubprocess` is garbage collected, but relying on `__del__` is fragile in Python; prefer explicit `terminate()`.

## When an actor's subprocess crashes

Things that kill an actor subprocess: an unhandled `SystemExit`, a segfault from native code, an OOM kill, `os._exit(1)` somewhere. By default `@rayd.actor` gives an actor a budget of **3 restarts** before it's marked permanently dead.

```python
class _Flaky:
    def increment(self) -> int:
        return 1
    def crash(self) -> int:
        import os
        os._exit(1)  # hard exit; never returns

Flaky = rayd.actor(_Flaky, max_restarts=1)

handle = Flaky.remote()
ref = handle.crash.remote()
try:
    rayd.get(ref)
except rayd.ActorDiedError as e:
    print(f"first call died: {e}")  # "actor subprocess died mid-call"

# After the crash, rayd respawned the subprocess (max_restarts=1 budget).
# Fresh state — increment from a clean instance:
print(rayd.get(handle.increment.remote()))  # 1

# Crash it again — now the budget is exhausted.
ref = handle.crash.remote()
try:
    rayd.get(ref)
except rayd.ActorDiedError:
    pass

# Future calls fail fast.
try:
    handle.increment.remote()
except rayd.ActorDiedError as e:
    print(f"actor permanently dead: {e}")  # "exhausted its restart budget"
handle.terminate()
```

What rayd does on a crash:

1. The driver's reader thread observes the actor's UDS socket close.
2. Every in-flight method's `ObjectRef` gets sealed with an `ActorDiedError` so blocked `rayd.get` calls raise instead of hang.
3. If the budget remains, a fresh subprocess is spawned with the same class and constructor args. State resets — `_CounterImpl` is reinstantiated.
4. If the budget is exhausted, the actor is marked dead. Future `.method.remote()` calls raise `ActorDiedError` immediately rather than queuing.

`max_restarts=0` means "no restart, die on the first crash". Use this for actors whose state isn't safe to reconstruct from a fresh `__init__`.

## Named actors: discoverable by other code

Naming an actor lets unrelated code find it later via `rayd.get_actor("name")`:

```python
# Requires RAYD_GCS_ADDRESS — names are cluster-wide via the GCS.
my_counter = Counter.options(name="global-counter").remote(0)
rayd.get(my_counter.increment.remote())

# Anywhere else that has a GCS connection:
found = rayd.get_actor("global-counter")
print(rayd.get(found.increment.remote()))  # 2
my_counter.terminate()  # also unregisters the name
```

Name uniqueness is enforced by the GCS — re-registering an existing name fails synchronously (`RuntimeError: actor name "X" is already registered`). When the actor terminates (cleanly or after exceeding `max_restarts`) the name is freed and can be reused.

`rayd.get_actor` returns:
- An `ActorHandle` if the actor lives on this driver (same as a fresh `.remote()` produced).
- A `_RemoteActorHandle` if the actor is owned by a different driver — it dials the owner's TCP listener for invocations.

Both expose the same `handle.method.remote(...)` surface; user code doesn't care which it got.

## Pickling actor handles

`ActorHandle` is picklable. Within the same driver, an unpickled handle wraps the same live subprocess:

```python
import pickle

handle = Counter.remote(0)
rayd.get(handle.increment.remote())
rayd.get(handle.increment.remote())

twin = pickle.loads(pickle.dumps(handle))
assert twin.pid == handle.pid          # same subprocess
assert rayd.get(twin.increment.remote()) == 3   # state shared

handle.terminate()
```

You can also `rayd.put(handle)` and `rayd.get(ref)` it back — handles are first-class object-store values. Cross-driver: pickling on the owner driver and shipping the bytes (e.g. via `cloudpickle.dumps`) to a peer driver yields a `_RemoteActorHandle` on the peer that dials back over the actor RPC TCP listener.

## Cross-driver invocation

When the actor is owned by driver A and driver B's `rayd.get_actor("foo")` returns it, B gets a `_RemoteActorHandle` whose `.method.remote()` dials A's TCP listener:

```
driver B                                driver A
  rayd.get_actor("foo")                   actor subprocess (running)
    -> _RemoteActorHandle                       ↑
    h.method.remote(args) ─── TCP ──→ A's RPC listener
                                            -> dispatch over UDS
    rayd.get(ref) ─── cross-node Pull ──→ A's raylet → bytes back
```

Method results are owned by A's driver; the calling driver's `rayd.get` triggers the same cross-node Pull path that any other remote-owned `ObjectRef` uses. See [`python/rayd/tests/test_actor_registry.py::test_cross_driver_get_actor_round_trips_via_rpc`](../../python/rayd/tests/test_actor_registry.py) for the full flow.

Failures propagate cleanly:

- A's actor subprocess crashes mid-call → B's `rayd.get(ref)` raises `ActorDiedError`.
- A's driver itself dies → B's next `.method.remote()` raises `OwnerDiedError` (rayd consults the GCS liveness gate when its TCP connect fails).
- A terminates the actor cleanly → B's next call raises `ActorDiedError("no longer alive")`.

## Subprocess isolation: PID is real

```python
import os

class _PidImpl:
    def my_pid(self) -> int:
        return os.getpid()

PidActor = rayd.actor(_PidImpl)

a = PidActor.remote()
b = PidActor.remote()
print(f"driver pid:  {os.getpid()}")
print(f"actor a pid: {a.pid} (= {rayd.get(a.my_pid.remote())})")
print(f"actor b pid: {b.pid} (= {rayd.get(b.my_pid.remote())})")
# All three pids are distinct — driver, actor A, actor B each have their own process.
a.terminate()
b.terminate()
```

This means CPU-bound or GIL-grabbing actor methods don't block the driver. The cost is one Python interpreter per actor (memory + startup time).

## Common pitfalls

**Decorator form + mypy --strict.** Use the assignment form for typed code; the decorator works at runtime but mypy can't see through it.

**Forgetting `terminate()`.** A leaked actor handle keeps its subprocess alive until the driver process exits. In long-running drivers, leaked actors pile up. Always `terminate()` actors you're done with — `try/finally` is your friend.

**Assuming `terminate()` is async.** It blocks until the reader thread joins and the subprocess reaps. With a long-running method in flight, terminate waits for it. There's a `timeout=` parameter if you need to cap that wait.

**`rayd.get_actor` without a GCS attached.** Named-actor lookup needs the cluster directory — set `RAYD_GCS_ADDRESS` in the env before `rayd.init()`. Without it, `rayd.get_actor` raises `RuntimeError("no GCS connection")`.

**Sharing mutable state inside the actor across method calls.** That's the whole point. But remember: the actor runs as a single thread within its subprocess. Don't add your own threads inside `_CounterImpl` and expect rayd's FIFO to protect them.

## Where to look next

- [03-state-and-errors.md](03-state-and-errors.md) — `get_settled` and partial-success patterns; works the same for actor method results as for task results.
- [`python/rayd/tests/test_actors.py`](../../python/rayd/tests/test_actors.py) — single-driver actor coverage.
- [`python/rayd/tests/test_actor_registry.py`](../../python/rayd/tests/test_actor_registry.py) — named actors + cross-driver flows.
- [`python/rayd/_actor.py`](../../python/rayd/_actor.py) — the implementation. Worth reading for the lock-discipline doc-comments alone.
