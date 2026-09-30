# API reference

The complete public surface is `include/tachyon/tachyon.h`. This document adds
the context that a header cannot carry: what each call costs, what it guarantees,
and which errors are normal instead of exceptional.

## Conventions

- Every function returns `ty_err_t`. `TY_OK` is 0; everything else is negative.
- `conf == NULL` means "use `ty_*_conf_default`".
- `timeout_ns < 0` blocks forever, `0` polls once, `> 0` waits that long.
- Handles (`ty_region_t*`, `ty_topic_t*`) are process-private. Do not share them
  across `fork()` boundaries expecting them to survive a re-map; open a new one
  in the child instead.

## Regions

### `ty_region_open(name, conf, out)`

Create or attach to a region. With `conf == NULL` it attaches with defaults,
which is what a consumer usually wants.

| Error | Meaning | What to do |
|---|---|---|
| `TY_ERR_INVAL` | name empty, too long, or contains a slash | fix the name |
| `TY_ERR_NOENT` | does not exist and `create` is 0 | create it first |
| `TY_ERR_ABI` | region built by an incompatible version | remove and recreate |
| `TY_ERR_MAP` | `mmap` failed | check `/dev/shm` space and limits |

### `ty_region_close(reg)`

Detach. **Does not delete the region** — a producer must be able to publish and
exit while its messages remain readable. Deletion is explicit.

The handle is freed, as with `fclose()` or `curl_easy_cleanup()`. Call it once;
the handle is dead afterwards and using it is undefined behaviour. The internal
magic field catches reuse only inside paths that have not yet freed, so treat
the contract, not the runtime check, as your protection.

### `ty_region_remove(name, dir)`

Unlink by name. Peers that already attached keep working; the name stops
resolving for anyone else. Returns `TY_ERR_NOENT` if it was never there, which
is not an error condition for setup code — Python's binding makes it idempotent
for exactly that reason.

### `ty_heartbeat(reg)`

Refresh this process's liveness timestamp. Only needed if your application
blocks for long stretches outside tachyon.

## Topics

### `ty_topic_create(reg, name, conf, out)`

Concurrent creation from several processes is safe: exactly one wins, the losers
get `TY_ERR_EXIST` and should call `ty_topic_open()`. This is the normal
start-up race and is expected, not exceptional.

`partitions` and `capacity` are rounded up to powers of two — the mask trick
requires it.

| Field | Default | Notes |
|---|---|---|
| `partitions` | 8 | power of two; more = more parallelism, less ordering |
| `capacity` | 1024 | slots per partition; this is your burst buffer |
| `pool_bytes` | 16 MiB | split evenly across the 8 size classes |
| `notify` | `TY_NOTIFY_HYBRID` | spin / futex / hybrid |

### `ty_topic_open(reg, name, out)`

Attach to an existing topic. `TY_ERR_NOENT` when it is not there yet.

### `ty_topic_list(reg, names, capacity)`

Enumerate topics present in a region. Pass `names == NULL` to just count.
Returns the number found, which may exceed `capacity`.

```c
char names[TACHYON_MAX_TOPICS][TACHYON_MAX_NAME + 1];
int n = ty_topic_list(reg, names, TACHYON_MAX_TOPICS);
```

This exists because inspection tooling cannot function without it: a CLI has no
other way to discover what a region contains, and can otherwise only open
topics whose names it already knows. `tyctl list` is built on it.

## Producing

### `ty_loan(topic, size, ptr, cap)`

Borrow a writable chunk. This is the zero-copy primitive: build your message
directly in shared memory and no intermediate buffer ever exists. `cap` receives
the usable capacity, which may exceed `size` (you get the whole class).

| Error | Meaning |
|---|---|
| `TY_ERR_TOOBIG` | larger than 1 MiB — pass a reference instead |
| `TY_ERR_NOMEM` | that class is exhausted (and all larger ones) — backpressure, retry |
| `TY_ERR_BUSY` | a reaper released this chunk at the same instant — retry |

### `ty_publish(topic, ptr, len, key)`

Commit a loaned chunk to `hash(key) & (partitions - 1)`. Only a 16-byte
descriptor moves. The chunk is still yours on failure — retry or `ty_abort()` it,
because abandoning a loan leaves it out of circulation for a full lease period.

### `ty_abort(topic, ptr)`

Return a loan without publishing. Cheap, and much better than leaking.

### `ty_send(topic, src, len, key)`

Convenience: loan + memcpy + publish. One copy, which is why it is slower than
the explicit pair — use `ty_loan()` when the message is large or already
somewhere you can write from.

## Consuming

### `ty_subscribe(topic, partition, msg, timeout_ns)`

Receive from one partition. `msg->data` points into shared memory; it is valid
until `ty_release()`.

| Error | Meaning |
|---|---|
| `TY_ERR_EMPTY` | polled with `timeout_ns == 0`, nothing there |
| `TY_ERR_TIMEOUT` | blocked and nothing arrived |
| `TY_ERR_FULL` | never returned from subscribe |

### `ty_subscribe_any(topic, hint, msg, timeout_ns)`

Receive from whichever partition has a message. Use when one consumer owns every
partition. It sweeps with short blocking attempts instead of parking on one
futex, because Linux has no "wait on any of these addresses" primitive — parking
on partition 0 would miss a message arriving on partition 5.

### `ty_release(topic, msg)`

Return the chunk. **Exactly once per message.** Skipping it leaks a chunk until
the reaper notices; doing it twice is undefined.

## Backpressure and introspection

### `ty_wait_room(topic, key, timeout_ns)`

Block until the destination partition has space. Prefer this to retrying
`ty_send()` in a tight loop when you want to apply backpressure on purpose.

### `ty_depth(topic, partition, out)`

Occupancy of one partition, or the total across all of them with `-1`.

### `ty_topic_stats(topic, out)`

`published`, `consumed`, `dropped`, `loan_ok`, `loan_nomem`, `reclaimed`,
`wait_wakeups`, `spin_spins`. A non-zero `reclaimed` in steady state means
something in your system is being killed, and `loan_nomem` climbing means you
undersized `pool_bytes`.

### `ty_pool_info(topic, out, n)`

Per-class chunk size, count, and current free count. Walks the free list, so it
is diagnostic-only — never call it on the hot path.

### `ty_reclaim_once(topic)`

Force one lease sweep. Returns chunks recovered. Safe to call concurrently; one
caller wins and the rest observe 0.

## Routing

### `ty_route(topic, key)`

Which partition a key maps to, without sending anything. Useful for dedicating a
worker to a fixed key range.

```c
partition = mix64(key) & (partitions - 1);
```

**Contract:** same key → same partition → strict FIFO. Different keys → no
ordering guarantee between them. Use one partition if you need total order.

## Timing helpers

### `ty_now_ns()`

`CLOCK_MONOTONIC_RAW`, nanoseconds. Safe and portable.

### `ty_now_ns_fast()`

TSC-derived. Faster, and requires an invariant, synchronised TSC — check
`ty_tsc_usable()` first, or accept that a VM migration can make it lie.
Calibrated once against `CLOCK_MONOTONIC_RAW`.

## Error codes

| Code | Value | Meaning |
|---|---:|---|
| `TY_OK` | 0 | success |
| `TY_ERR_INVAL` | -1 | invalid argument |
| `TY_ERR_EXIST` | -2 | already exists |
| `TY_ERR_NOENT` | -3 | no such region or topic |
| `TY_ERR_NOMEM` | -4 | pool or region exhausted |
| `TY_ERR_FULL` | -5 | ring full — backpressure, retry |
| `TY_ERR_EMPTY` | -6 | nothing available on a poll |
| `TY_ERR_TIMEOUT` | -7 | deadline expired |
| `TY_ERR_TOOBIG` | -8 | payload exceeds 1 MiB |
| `TY_ERR_STATE` | -9 | wrong state for this call |
| `TY_ERR_ABI` | -10 | incompatible layout version |
| `TY_ERR_IO` | -11 | OS error, see `errno` |
| `TY_ERR_MAP` | -12 | `mmap` failed |
| `TY_ERR_BUSY` | -13 | contended, retry |
| `TY_ERR_INTERNAL` | -99 | invariant violated — please file a bug |

`TY_ERR_FULL` and `TY_ERR_NOMEM` are **normal under load**, not bugs: they are
how the library tells you the consumer is behind.
