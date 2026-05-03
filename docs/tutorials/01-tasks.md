# Tutorial: tasks

A *task* in rayd is a Python function the driver hands off for asynchronous execution in a separate worker process. The result is an `ObjectRef` you can pass around, batch up, and resolve with `rayd.get`.

This tutorial assumes you've installed rayd and skimmed the [README](../../README.md). It walks through the core task surface end-to-end with runnable code.

By the end you should know:

- How to define a remote function and submit work to the worker pool.
- How to fan out and collect results.
- How errors flow back to the driver.
- How to inspect ref state without blocking.
- The inline-vs-plasma threshold and why it matters.
- One thing rayd deliberately does NOT do that Ray does (and how to handle it).

## Setting up

Every example starts with `rayd.init()` and ends with `rayd.shutdown()`. Init brings up the dispatcher pool (4 worker subprocesses by default — override with `RAYD_NUM_WORKERS=N`) and connects to a plasma store (auto-spawned unless `RAYD_PLASMA_SOCKET` is set).

```python
import rayd

rayd.init()
try:
    # ... your code ...
    pass
finally:
    rayd.shutdown()
```

You can also use it without GCS (single-driver, single-host) which is what the rest of this tutorial assumes. For multi-node clusters see the README's *Cross-node clusters* section.

## Defining a remote function

```python
import rayd

@rayd.remote
def square(x: int) -> int:
    return x * x
```

`@rayd.remote` wraps `square` in a `RemoteFunction` object. The original callable is still available as `square.__wrapped__` if you want to call it locally (e.g. in a unit test). To submit work, use `.remote(...)`:

```python
ref = square.remote(7)
```

`ref` is an `ObjectRef` — a 28-byte handle the driver minted for the eventual result. Submission is fast: it cloudpickles `square` plus the args, hands them to the dispatcher, and returns. Execution happens in a worker subprocess.

To get the value, block on the ref:

```python
print(rayd.get(ref))  # 49
```

`rayd.get` raises `TimeoutError` if you pass `timeout=` and the result isn't ready in time:

```python
import time

@rayd.remote
def slow(x: int) -> int:
    time.sleep(2.0)
    return x

ref = slow.remote(1)
try:
    rayd.get(ref, timeout=0.1)
except TimeoutError:
    print("not ready yet")  # this prints
print(rayd.get(ref))  # blocks until ready, then prints 1
```

## Fan-out and batch get

Submitting many tasks looks like a list comprehension:

```python
refs = [square.remote(i) for i in range(10)]
print(rayd.get(refs))  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

Pass a list to `rayd.get` and it returns a list of the same length, in the same order. Order matters — rayd doesn't reorder for you. Workers run tasks in roughly submission order on the dispatcher's UDS socket but actual execution interleaving depends on worker availability.

A common pattern: fire many tasks, then walk the results:

```python
@rayd.remote
def process(item: dict) -> dict:
    item["processed"] = True
    return item

items = [{"id": i, "value": i * 10} for i in range(100)]
refs = [process.remote(item) for item in items]
results = rayd.get(refs)
assert all(r["processed"] for r in results)
```

`process` ran on workers in parallel; the driver waited for the full batch.

## Refs aren't transparent across tasks

**This is a deliberate difference from Ray.** In Ray, you can pass an `ObjectRef` as an argument to another remote function and the worker resolves it for you:

```python
# Ray (NOT rayd):
r1 = ray.remote(add_one).remote(5)
r2 = ray.remote(double).remote(r1)  # auto-resolves r1 on the worker
```

In rayd, the worker receives the `ObjectRef` *as an `ObjectRef`*, not as the underlying value. To chain tasks, resolve refs explicitly on the driver:

```python
@rayd.remote
def add_one(x: int) -> int:
    return x + 1

@rayd.remote
def double(x: int) -> int:
    return x * 2

r1 = add_one.remote(5)
val = rayd.get(r1)
r2 = double.remote(val)
print(rayd.get(r2))  # 12
```

For "wait for several inputs, then submit a downstream task", you typically want one of:

```python
inputs = rayd.get([add_one.remote(i) for i in range(10)])
result = rayd.get(double.remote(sum(inputs)))
```

Or, if downstream task latency dominates, batch via plasma directly with `rayd.put`:

```python
big_input = list(range(1_000_000))
ref = rayd.put(big_input)            # one large object, sealed once
# Pass `ref.hex` or rayd.get(ref) into the downstream task as needed.
```

If you need cross-task ref propagation later, that's planned work — but not yet shipped, and the explicit pattern above is simpler to reason about anyway.

## Errors

A remote function that raises propagates the exception back through `rayd.get`:

```python
@rayd.remote
def maybe_fail(x: int) -> int:
    if x < 0:
        msg = "x must be non-negative"
        raise ValueError(msg)
    return x * 2

ref = maybe_fail.remote(-1)
try:
    rayd.get(ref)
except ValueError as e:
    print(f"caught: {e}")  # caught: x must be non-negative
```

The original exception type is preserved (rehydrated from cloudpickle on the driver side). The traceback includes both the worker frames *and* the driver-side `rayd.get` frame.

For a batch where some refs fail, `rayd.get([ref1, ref2_failed, ref3])` raises on the first failure and you lose the others' results. That's fine for "abort on first error" semantics, but often you want partial success — see [03-state-and-errors.md](03-state-and-errors.md) for `rayd.get_settled`, which returns `Ok`/`Err`/`Pending` per-ref instead.

## Cheap state inspection

You can observe a ref's lifecycle state without blocking:

```python
import time

@rayd.remote
def slow(i: int) -> int:
    time.sleep(0.5)
    return i

refs = [slow.remote(i) for i in range(5)]
states = rayd.state(refs)
for ref, s in states.items():
    print(f"{ref.hex[:8]}  {s!r}")
# 7d73d0a1  RefState.Pending
# 7d73d0a1  RefState.Pending
# ...
```

`RefState` is one of `Pending` / `ReadyLocal` / `ReadyRemote` / `Failed`. Use `s.is_ready()` and `s.is_failed()` for predicates. State inspection is a HashMap lookup — no plasma read, no deserialization. It's safe to call in a tight loop.

## Waiting for the first N to finish

`rayd.wait(refs, num_returns=N, timeout=...)` returns `(ready, not_ready)` once `N` of them are done or the timeout elapses:

```python
refs = [slow.remote(i) for i in range(5)]
ready, not_ready = rayd.wait(refs, num_returns=2, timeout=1.0)
print(f"{len(ready)} ready, {len(not_ready)} still in flight")
# Process the ready ones now; come back to the rest later
for r in rayd.get(ready):
    print(r)
```

Use this for streaming-like patterns where you want to process the first results before the slowest task finishes.

## The inline-vs-plasma threshold

Small return values are stored *inline* in the driver's local memory store. Large ones go to shared-memory plasma. The threshold is currently 100 KiB (matching Ray's `max_direct_call_object_size`).

The practical effect: returning a 50 KB object is essentially free — no shared-memory crossing, no kernel calls. Returning a 50 MB object pays the cost of a memcpy into plasma's mmap'd arena.

When working with large data, prefer `rayd.put` once and pass *the deserialized value* into tasks, rather than baking large literals into the closure or returning huge results from every task.

## Failure recovery: lineage reconstruction

If the driver still has the `ObjectRef` and the local plasma copy disappears (got evicted, fell out of cache, was spilled to disk), `rayd.get` *transparently* re-runs the producing task. This is *lineage reconstruction*. It's why each task is recorded with a retry budget, and why `Pending` is a possible ref state even for refs you submitted minutes ago.

You don't have to do anything for this — it just happens. If lineage's retry budget runs out, the get raises `ObjectUnreconstructableError`. (See `python/rayd/tests/test_lineage.py` for the exact semantics.)

## Common pitfalls

**Forgetting `rayd.shutdown()`.** Init spawns workers + plasma; shutdown joins them. If your script exits without calling shutdown, you may leak processes. Use a `try/finally` block.

**Passing refs to remote functions and expecting auto-resolution.** Doesn't happen. Pass values, not refs. (See "Refs aren't transparent across tasks" above.)

**`rayd.get([ref1, ref2_failed])` losing the second result.** Use `rayd.get_settled` for partial-success semantics. See [03-state-and-errors.md](03-state-and-errors.md).

**Closures that capture large locals.** `@rayd.remote` cloudpickles the function plus its closure on every `.remote(...)` call. If the closure captures a 100 MB object, every submit sends 100 MB over UDS. Hoist the data into a `rayd.put` once and pass the value (or ref-resolved bytes) explicitly.

## Where to look next

- [02-actors.md](02-actors.md) — when you need state that persists across calls.
- [03-state-and-errors.md](03-state-and-errors.md) — partial-success, typed `Ok/Err/Pending`, the central API improvement vs Ray.
- [`python/rayd/tests/test_tasks.py`](../../python/rayd/tests/test_tasks.py) — comprehensive test coverage, useful as executable spec.
- [`python/rayd/__init__.py`](../../python/rayd/__init__.py) — the typed Python facade. Short and worth reading.
