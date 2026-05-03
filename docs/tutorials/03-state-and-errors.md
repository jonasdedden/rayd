# Tutorial: state and errors

This is the API improvement that motivated the whole rewrite. If you only read one tutorial, make it this one.

`rayd.get(refs)` raises on the first failure, just like Ray's `ray.get(refs)`. That's fine when "any failure should abort the whole batch" is what you actually want. It's the wrong semantics when:

- You're processing a batch of independent items and want to log/handle the failures while keeping the successes.
- You want to see *which* refs failed and what their error categories are, without unpickling the underlying exception value.
- You want to peek at lifecycle state (is this ref still computing? did it fail? is its bytes already local?) without a blocking read.

Ray makes you write a try/except around each ref individually, looping with `ray.wait` to avoid blocking on slow ones. rayd gives you `rayd.get_settled` and `rayd.state` for cheap, typed, non-raising inspection.

By the end of this tutorial you'll know:

- The `Result` type hierarchy: `Ok[T]`, `Err`, `Pending`.
- `rayd.get_settled` for partial-success batches.
- `rayd.state` for a ref-state snapshot without deserializing.
- `rayd.wait` and `rayd.wait_with_states` for "first N to settle" patterns.
- The `ErrorCategory` taxonomy and how to discriminate errors without exception introspection.

## The Result hierarchy

```python
from rayd import Ok, Err, Pending, Result

# Result[T] is `Ok[T] | Err | Pending` — a tagged union you match against.
```

- `Ok[T]` carries the unpickled value (`Ok(value=v)`).
- `Err` carries an `ErrorInfo` — a lightweight struct with category, message, optional traceback, and an optional pickled exception. **No deserialization of the user's exception value happens until you ask for it.**
- `Pending` is a sentinel — the ref hasn't settled by the deadline.

`Ok` and `Err` are frozen `@dataclass(frozen=True, slots=True)` so they pattern-match cleanly.

## `rayd.get_settled`: partial-success in one call

```python
import rayd
from rayd import Ok, Err, Pending

@rayd.remote
def maybe_fail(x: int) -> int:
    if x < 0:
        msg = "x must be non-negative"
        raise ValueError(msg)
    return x * 2

rayd.init()
try:
    refs = [maybe_fail.remote(i) for i in [1, -1, 3, -5, 7]]
    results = rayd.get_settled(refs, timeout=5.0)

    successes = []
    failures = []
    for ref, r in zip(refs, results):
        match r:
            case Ok(value=v):
                successes.append(v)
            case Err(info=info):
                failures.append((ref, info.message))
            case Pending():
                # ref hadn't settled by the timeout
                pass

    print(f"got {len(successes)} successes: {successes}")
    print(f"got {len(failures)} failures:  {failures}")
finally:
    rayd.shutdown()
```

Output:

```
got 3 successes: [2, 6, 14]
got 2 failures:  [(<ObjectRef ...>, "ValueError('x must be non-negative')"), ...]
```

Compare against Ray:

```python
# Ray equivalent:
results = []
for ref in refs:
    try:
        results.append(("ok", ray.get(ref)))
    except Exception as e:
        results.append(("err", e))
```

That works but: it's serial; you pay the deserialization cost for every value (even ones you'll throw away); and you re-raise + re-catch once per ref. `rayd.get_settled` walks the local store map once with a single mutex acquisition.

## `ErrorInfo`: discriminate without unpickling

```python
case Err(info=info):
    if info.category == rayd.ErrorCategory.TaskException:
        # User-raised exception in the task body.
        ...
    elif info.category == rayd.ErrorCategory.ActorDied:
        # Actor's subprocess died (mid-call or after budget exhausted).
        ...
```

Categories you'll commonly see (`rayd.ErrorCategory.*`):

| Category | When you see it |
|---|---|
| `TaskException` | The remote function raised. |
| `ActorDied` | An actor's subprocess died mid-call or past its restart budget. |
| `OwnerDied` | The owner driver of a remote ref is no longer alive. |
| `ObjectLost` | A remote replica disappeared (mostly with cross-node fetches). |
| `ObjectUnreconstructable` | Lineage's retry budget exhausted; can't replay. |
| `FetchTimeout` | Cross-node Pull timed out. |
| `WorkerDied` | A worker subprocess died (not actor — that's `ActorDied`). |

**`info.message` is just the `str(exc)` of the original exception** — readable without unpickling. `info.traceback` is the formatted traceback string. If you want the original exception object back, that requires unpickling, which `rayd.get(ref)` does for you when called explicitly on a failed ref. The advantage of `Err` is you can decide whether to pay that cost based on the category.

## `rayd.state`: lifecycle snapshot

When you don't even want to *touch* the values:

```python
import time

@rayd.remote
def slow(i: int) -> int:
    time.sleep(0.5)
    return i

refs = [slow.remote(i) for i in range(5)]
states = rayd.state(refs)
# {ref: RefState.Pending, ref: RefState.Pending, ...}
```

`RefState` values:

- `Pending` — task is still in flight (or recorded but not yet running).
- `ReadyLocal` — value is available in the driver's local plasma/inline store.
- `ReadyRemote` — value exists in the cluster but hasn't been fetched locally yet (cross-node case).
- `Failed` — the ref settled with an error.

`s.is_ready()` and `s.is_failed()` are predicate helpers.

`rayd.state` is a HashMap lookup. No plasma read, no deserialization. Safe to call thousands of times per second.

## "Wait for the first N" patterns

For streaming-like code where you want to start processing as soon as *anything* is ready:

```python
refs = [slow.remote(i) for i in range(5)]

remaining = list(refs)
while remaining:
    ready, remaining = rayd.wait(remaining, num_returns=1, timeout=5.0)
    for ref in ready:
        print(f"got: {rayd.get(ref)}")
```

`rayd.wait(refs, num_returns=N, timeout=...)` returns when `N` refs are ready or the timeout hits. Use it as a non-blocking-ish iterator.

`rayd.wait_with_states` is a variant that returns `dict[ObjectRef, RefState]` — useful when you want to know which ones settled and which timed out, in one call.

## A realistic recipe: process with retries and per-item logging

```python
import rayd
from rayd import Ok, Err, Pending

@rayd.remote
def process_one(item: dict) -> dict:
    if item.get("malformed"):
        raise ValueError(f"malformed: {item}")
    return {**item, "processed": True}

rayd.init()
try:
    items = [{"id": 1}, {"id": 2, "malformed": True}, {"id": 3}, {"id": 4, "malformed": True}]
    refs = [process_one.remote(item) for item in items]
    results = rayd.get_settled(refs, timeout=10.0)

    processed = []
    needs_retry = []
    for ref, item, r in zip(refs, items, results):
        match r:
            case Ok(value=v):
                processed.append(v)
            case Err(info=info) if info.category == rayd.ErrorCategory.TaskException:
                # User-raised — log it, don't retry.
                print(f"ITEM {item['id']} FAILED: {info.message}")
            case Err(info=info):
                # Infra failure (worker died, fetch timeout, …) — retryable.
                needs_retry.append(item)
            case Pending():
                # Didn't settle in time — retry.
                needs_retry.append(item)

    print(f"processed: {len(processed)}")
    print(f"to retry:  {len(needs_retry)} items")
    # ... resubmit needs_retry in a fresh batch
finally:
    rayd.shutdown()
```

This is the pattern the API was designed for. In Ray you'd write a per-ref try/except loop, and you'd have to introspect the exception type to decide retryable-vs-permanent. Here the *category* is on the lightweight `ErrorInfo`, so you make the decision in O(1) without touching the exception payload.

## The `free` API for explicit cleanup

When you know you're done with a batch of refs and want their plasma copies released *now* (not whenever Python's GC gets around to dropping the `ObjectRef` objects):

```python
refs = [maybe_fail.remote(i) for i in range(1000)]
results = rayd.get_settled(refs, timeout=10.0)
# ... use results ...
rayd.free(refs)  # drop local plasma copies + signal owner
```

`rayd.free` is rare — usually letting refs go out of scope is fine — but it's there when you're working at scale and need predictable plasma footprint.

## Common pitfalls

**Pattern-matching `Err` and forgetting `info=`.** `Err` is a `@dataclass(frozen=True)` with a single `info` field. The match-case syntax requires the keyword: `case Err(info=info):`. `case Err(info):` won't bind anything.

**Calling `rayd.get(ref)` after `rayd.get_settled` returned `Err` for it.** That's allowed — you'll get the original exception raised. But it costs an unpickle. Prefer using `info.message` when a string is enough.

**Using `Pending` to mean "task is taking too long, retry."** `Pending` just means "didn't settle by the timeout you supplied." For lineage-failed objects (Ray's `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`) you get an `Err(category=ObjectUnreconstructable)` instead — that's the "actually broken, give up" signal.

**`rayd.state` on refs from another driver.** It returns the local view: refs you got via `rayd.get_actor` or unpickled from another driver may show `ReadyRemote` even though the bytes are sitting in the owner driver's plasma. Use `rayd.get` to actually pull them.

## Where to look next

- [`python/rayd/__init__.py`](../../python/rayd/__init__.py) — the typed Python facade. The `Result`, `Ok`, `Err`, `Pending` types are right there. Short and worth reading.
- [`python/rayd/tests/test_tasks.py`](../../python/rayd/tests/test_tasks.py) — tests that exercise `get_settled` extensively.
- [docs/design/05-state-and-error-api.md](../design/05-state-and-error-api.md) — the design doc. Explains the four "patterns" the API was designed to make easy.
- [docs/analysis/05-objectref-state-gap.md](../analysis/05-objectref-state-gap.md) — the analysis of Ray's pain point that motivated this whole rewrite.
