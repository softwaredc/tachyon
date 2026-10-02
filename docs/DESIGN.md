# Design and internals

This document explains what is actually in the shared region and why each
decision was made. It is written for people deciding whether to trust this
library with production traffic, so it includes the trade-offs and the things
that this library does not do.

## 1. Region layout

A region is one POSIX shared memory object (`/dev/shm/tachyon_<name>` by
default) mapped into every participating process. It may be mapped at a
different address in each of them, which forces the first rule:

> **Nothing in the region is a pointer. Everything is an offset.**

`TY_PTR(reg, off)` is the only legal way to turn a stored offset into something
dereferenceable. Violating this once — one `std::vector`, one vtable, one
absolute address — produces a region that works in the creating process and
corrupts memory in every other one.

```
┌─────────────────────────────────────────────────────────────────┐
│ Region header                                               16 KiB │
│   magic / abi_version / region_size / lease_ns                     │
│   ctl_lock      pthread_mutex_t, PROCESS_SHARED + ROBUST           │
│   bump_off      monotonic allocator for topic areas                │
│   procs[128]    pid + heartbeat_ns + state          (64 B each)    │
│   topics[64]    name + state + offset               (128 B each)   │
├─────────────────────────────────────────────────────────────────┤
│ Topic area 0                                                       │
│   topic header           960 B   (counters each on their own line) │
│   rings[partitions]      256 B each                                │
│   descriptors[partitions][capacity]   16 B each                    │
│   pool header            1024 B  (8 classes x 128 B)               │
│   chunks for class 0..7                                            │
│   loan bitmaps for class 0..7                                      │
├─────────────────────────────────────────────────────────────────┤
│ Topic area 1 ...                                                   │
└─────────────────────────────────────────────────────────────────┘
```

All sizes are pinned by `_Static_assert` in `src/internal.h`. If a future
change breaks one of them without bumping `TACHYON_ABI_VERSION`, the build
stops instead of silently corrupting existing regions.

## 2. The ring: Vyukov bounded MPMC

A ring of variable-length messages is an uncomfortable fit. Fixed slots waste
space on small messages and cannot hold large ones; packing payloads
contiguously makes wrap-around painful and allocation unbounded.

So the ring carries **only descriptors**:

```c
typedef struct { atomic_uint64_t seq; uint32_t chunk_idx; uint32_t len; } ty_desc_t;  // 16 B
```

The payload lives in a separate pool. Metadata is fixed size and gets the
cache behaviour it wants; payloads are variable length and get the space they
need. One 64-byte cache line carries four descriptors, which is what makes
sequential consumption fast.

The queue itself is Dmitry Vyukov's bounded MPMC algorithm:

- producers contend on `fetch_add(enq_pos)`, not on a CAS duel over `tail`
- consumers likewise on `deq_pos`
- correctness comes from per-cell sequence numbers, so a producer never touches
  another producer's slot

Initialisation sets `descs[i].seq = i`; capacity must be a power of two so
`% capacity` becomes `& mask`.

**Honest limitation:** a producer that is descheduled between claiming a slot
and filling it stalls *that slot* until it resumes. The queue still makes
progress elsewhere, but tail latency depends on your producers behaving. This
is inherent to the algorithm, not a bug we have left in.

## 3. The pool: size classes and generation-tagged free lists

Eight power-of-two classes: 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576.
Each has a Treiber stack whose head packs an index and a 32-bit generation tag
into one `uint64_t`, CAS'd atomically.

```c
head = (tag << 32) | index
```

The tag is what defeats ABA. For two poppers to both succeed on the same node,
exactly 2³² operations must occur in between — accepted as "never". Hazard
pointers would be strictly stronger and about 3x slower per operation; we
document the choice instead of hiding it.

If the exact class is exhausted, `ty_loan()` borrows upward from a larger one.
That costs some memory but keeps throughput up when one bucket is hot, and
beating a hard `NOMEM` is usually the right call.

Each chunk has a 64-byte header (magic, state, class, size, owner pid, lease
expiry, sequence), so the payload itself starts 64-byte aligned.

### Global chunk ids

A descriptor is 16 bytes and needs `seq`, a chunk index, and a length. There
is no room left for "which size class", so chunks are numbered **globally**
across all classes and `ty_gid_resolve()` does the reverse lookup on receive.
Eight comparisons per message buys a descriptor that fits four to a cache line.

## 4. Chunk lifecycle

```
        loan                 publish                subscribe            release
FREE ────────► LOANED ──────────────► PUBLISHED ──────────────► CONSUMED ──────► FREE
  ▲              │                                                   │
  └──── abort ───┘                                        crashed ───┘ (reaper)
```

Ordering matters and was the source of the nastiest bug found during
development: the chunk must become `PUBLISHED` **before** its descriptor
becomes visible in the ring. Publishing first leaves a window in which a
consumer has already dequeued the chunk and flipped it to `CONSUMED`, while the
producer's own CAS either fails outright or — far worse — lands on a chunk that
another producer has meanwhile loaned again, clearing ownership markers
underneath a live message. It only reproduced under load, every few hundred
thousand messages. `ty_publish()` now does the transition first and rolls it
back if the enqueue fails.

## 5. Notification and the lost-wakeup protocol

Three policies: spin, futex, hybrid (default).

The protocol is worth a second look:

```
waiter:    waiters++  ─fence─►  recheck queue  ─fence─►  futex_wait
producer:  enqueue    ─fence─►  read waiters   ──►  maybe futex_wake
```

Because the recheck happens *after* registration, a producer that sampled
`waiters == 0` must have enqueued before our increment, so our recheck cannot
miss its message. A producer that sampled `>= 1` guarantees a wake. Omit the
recheck and you deadlock once every few billion messages — the kind of bug that
only reproduces in production at 3am. `ty_wait_for_msg()` owns all three steps
precisely so the invariant lives in one place.

The second optimisation: skip the syscall entirely when nobody is waiting. On a
saturated stream this dominates throughput, because a futex round trip costs
more than the entire publish path.

## 6. Crash safety

SIGKILL is uncatchable — no `atexit`, no destructor, no cleanup. Ownership is
therefore **leased**, not held. See [CRASH-SAFETY.md](CRASH-SAFETY.md) for the
full treatment; the summary is three layers:

| Layer | Mechanism | Protects |
|---|---|---|
| 1 | per-process heartbeat timestamp | tells "slow" from "dead" |
| 2 | lease deadline on every outstanding chunk | bounds how long one can go missing |
| 3 | robust process-shared mutex on the control plane | recovery when a process dies holding the lock |

Recovery never trusts time alone. A chunk is reclaimed only when the owner is
provably gone, or the lease expired **and** the heartbeat stopped advancing —
because stealing a chunk from a slow-but-alive producer corrupts live data.

## 7. Ordering contract

```
partition = mix64(key) & (partitions - 1)
```

Same key → same partition → strict FIFO. Different keys may be consumed
concurrently with no ordering guarantee between them. That is the price of
parallelism and it is stated in the README instead of being quietly violated. Use
one partition if you need total order.

## 8. What this library does not do

- **No dynamic growth.** Regions and pools are sized at creation. Growing a
  mapped region under load means either a stop-the-world remap or a second
  mapping, and neither is worth the complexity for the target workloads.
- **No background thread by default.** The reaper is opt-in
  (`spawn_reaper`), and `tachyond` exists for people who want one process to do
  the sweeping. Libraries that silently start threads cause surprises at
  `fork()`.
- **No cross-machine story.** This is IPC. If you need network transparency,
  put a real RPC layer on top.
- **No security model.** Anything that can attach can read everything. Use
  filesystem permissions on `/dev/shm` as your boundary.
