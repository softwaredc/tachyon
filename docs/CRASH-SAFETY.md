# Crash safety

The problem this file exists to solve is not exotic. It is this:

> A process is `SIGKILL`ed while holding a loaned chunk. Nothing runs. No
> `atexit`, no destructor, no cleanup handler. That chunk is now unreachable,
> and every second that passes makes it one chunk harder to explain why your
> service starts returning `NOMEM` three hours from now.

`SIGKILL` cannot be caught. That is the whole difficulty. There is no
"finally" in C, so cleanup cannot be scheduled — it has to be *inferred*.

## Ownership is leased, not held

Every outstanding chunk carries:

```c
atomic_int32_t  owner_pid;
atomic_uint64_t lease_expire_ns;
```

A loan sets both. Publishing or releasing clears them. Anything still marked
after its deadline is a candidate for recovery.

A per-class **bitmap** marks which chunks are currently owned, so a sweep
inspects only candidates instead of walking the entire pool. With a million
chunks that is ~15,000 words to read instead of a million headers.

## Three layers

### Layer 1 — heartbeats

Each attached process claims a slot in `procs[128]` and its heartbeat timestamp
advances whenever it calls into the library. `ty_heartbeat()` exists for
applications that block for long stretches outside tachyon and would otherwise
look dead.

This layer is what lets the reaper tell *slow* apart from *dead* — which is the
difference between recovering a chunk and corrupting a live message.

### Layer 2 — leases and the reaper

`ty_pool_loan()` sets `lease_expire_ns = now + lease_ns` (default 5 s). A
reaper — either the in-process thread (`spawn_reaper = 1`) or the external
`tachyond` daemon — sweeps at `lease_ns / 4` and reclaims what is abandoned.

Because a provably dead process needs no grace period (see below), recovery in
the common case is immediate instead of waiting for the deadline.

### Layer 3 — robust mutex on the control plane

Topic creation, slot allocation, and reaping take a `pthread_mutex_t`
configured with:

```c
pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
pthread_mutexattr_setrobust (&attr, PTHREAD_MUTEX_ROBUST);
```

If the holder is killed, the next acquirer receives `EOWNERDEAD`, repairs any
half-finished control-plane state, and calls `pthread_mutex_consistent()`.
Without this, one process dying during topic creation would wedge every future
topic creation in every other process, forever.

**This lock is never taken on the data path.** Publish and subscribe touch no
locks at all; that is what keeps the numbers in the README where they are.

## The decision rule

A chunk is reclaimed only when we can prove nobody still intends to use it:

```c
if (owner_pid <= 0)                  reclaim;   /* unowned */
if (!ty__pid_alive(owner_pid))       reclaim;   /* provably dead: no wait */
if (now < lease_expire_ns)           keep;      /* still within its lease   */
if (!ty_owner_progressing(...))      reclaim;   /* expired heartbeat        */
/* otherwise: alive and ticking, merely slow -- leave it alone */
```

The last case is the important one and it is easy to get wrong. A producer that
is blocked in a syscall for longer than `lease_ns` must **not** lose its chunk:
it is about to publish into it, and handing that chunk to somebody else
corrupts live data. Slow is not dead.

`tests/test_crash.c` contains `test_live_producer_not_reclaimed`, which holds a
loan for 1.5 seconds against a 200 ms lease while pumping heartbeats, and
asserts that the chunk is never reclaimed and still publishes correctly. A
crash-recovery scheme without that test is a scheme that will corrupt data.

## Failure modes we accept

Stated plainly, because "crash safe" without boundaries is marketing:

| Situation | Behaviour |
|---|---|
| Process killed while holding a loan | chunk recovered, usually immediately |
| Process killed after receiving, before releasing | chunk recovered |
| Process alive but stalled past `lease_ns` | **not** reclaimed while heartbeats advance |
| Process alive, heartbeats stopped, lease expired | reclaimed |
| More than 128 attached processes | extra processes cannot prove liveness; they fall back to lease deadlines only |
| `lease_ns` shorter than your longest legitimate loan | chunks can be reclaimed from live owners — set it longer |

The 128-process limit is the one operational constraint worth checking before
you deploy. Beyond it, liveness proofs degrade to deadlines, which is safe but
slower to recover.

## Verifying it yourself

```bash
./scripts/chaos_kill.sh 2000
```

That forks a child 2,000 times, killing it at three different points in the
message lifecycle, and asserts at the end that the pool's free count returned
to exactly its baseline. On the reference machine, 300 rounds reclaimed 2,448
chunks with zero loss.

```c
/* Or force a sweep yourself */
uint64_t n = ty_reclaim_once(topic);
```
