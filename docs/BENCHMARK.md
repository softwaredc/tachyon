# Benchmark methodology

Every number in the README was produced by `benchmarks/bench_ipc.c` on a real
machine. This document says exactly how, so you can reproduce them — or
disprove them.

## Reproducing

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
./scripts/run_benchmarks.sh          # full matrix, writes results.txt
```

Or run one measurement directly:

```bash
./build/benchmarks/bench_ipc tachyon latency     256  50000
./build/benchmarks/bench_ipc tachyon throughput  1024 200000
TY_NOTIFY=spin ./build/benchmarks/bench_ipc tachyon latency 256 50000
```

`TY_NOTIFY` selects the wait policy: `spin`, `futex`, or `hybrid` (default).
One binary covers the whole table.

## What is measured

**Latency** — round trip between two forked processes. The parent sends, the
child echoes, the parent receives; the clock brackets all three. Reported as
round-trip; the README's "one-way" figure is half of it and is labelled as an
estimate.

**Throughput** — the parent sends `count` messages as fast as the consumer
drains them, and the clock stops only after `waitpid()` returns, so the last
message is known to have arrived.

## Fairness rules

These are the rules we impose on ourselves. They are listed because a
benchmark that quietly favours its own library is worse than no benchmark.

1. **Same memcpy count for every transport.** The send side does one memcpy, the
   receive side does one memcpy, including tachyon (`ty_send` /
   `subscribe` + `memcpy`). Building the message inside a loaned chunk removes
   the send-side copy and is faster still, but we benchmark the conservative
   path on purpose.
2. **Identical process topology.** Two processes, one per role, pinned to two
   cores with `taskset`.
3. **Warm-up before timing.** 1,000 unmeasured rounds first. Their only job is
   to reach steady state; they are not discarded because they looked bad.
4. **Exact percentiles.** All samples are sorted. p99.9 is the actual sample at
   that rank, not a histogram bucket approximation.
5. **No outlier trimming.** Nothing is dropped for being inconvenient. That is
   why the tail numbers on a virtualised kernel look worse than the medians —
   because they are.

### A note on how tachyon is measured versus the others

The file-descriptor transports (pipe, socket) are created by the parent and
inherited across `fork()`. tachyon instead has the child **re-attach** through
`ty_region_open()` and `ty_topic_open()`, so its numbers include the real
attach path a separate service takes. We picked the harder test on purpose.

## Reference environment

```
CPU          : Intel(R) Core(TM) Ultra 7 356H, 16 logical cores
Kernel       : 6.18.33.2-microsoft-standard-WSL2
OS           : Ubuntu 24.04.5 LTS
Memory       : 15 GiB
Compiler     : gcc 13.3.0, -O3
Pinning      : taskset -c 2,3
```

### Why the environment changes the interpretation

WSL2 runs on a Microsoft kernel with a virtualised syscall surface. Anything
that traps into the kernel — futex sleeps and wakes, `read`/`write` on pipes
and sockets — costs substantially more there than on bare metal.

That means:

- **tachyon in spin mode is barely affected.** It never enters the kernel, so
  the figure should reproduce closely on a real Linux host.
- **Every sleeping transport is reported pessimistically**, including tachyon's
  own futex and hybrid modes.

So the headline comparison — spin mode against pipe/socket/TCP — understates
how much better the gap looks on bare metal in one sense and overstates
tachyon's futex-mode latency in another. Run the script on your own hardware
before making a procurement decision; the numbers you get will be the numbers
that matter to you.

## Results

### Latency, 256-byte messages, 50,000 round trips (nanoseconds, round trip)

| Transport | p50 | p99 | p99.9 | mean |
|---|---:|---:|---:|---:|
| tachyon (spin) | 1,007 | 1,221 | 14,120 | 1,060 |
| tachyon (hybrid) | 22,093 | 76,159 | 164,189 | 22,515 |
| tachyon (futex) | 24,306 | 83,170 | 179,882 | 30,341 |
| Unix domain socket | 20,348 | 74,512 | 164,024 | 26,150 |
| pipe | 31,375 | 86,445 | 196,747 | 29,574 |
| TCP loopback | 37,925 | 129,706 | 209,093 | 40,770 |

### Throughput, 1 KiB messages, 200,000 messages

| Transport | msgs/sec | throughput |
|---|---:|---:|
| tachyon (spin) | 3,961,955 | 3.78 GB/s |
| tachyon (hybrid) | 3,846,457 | 3.67 GB/s |
| pipe | 2,012,017 | 1.92 GB/s |
| Unix domain socket | 1,542,371 | 1.47 GB/s |
| TCP loopback | 826,004 | 0.79 GB/s |

### In-process concurrency

4 producer threads × 20,000 messages, 4 consumer threads, 8 partitions of 128
slots (`tests/test_stress.c`):

```
80,000 messages in 0.129 s  (~620,000 msgs/s)
0 lost, 0 duplicated, 0 corrupted
```

## Correctness tests that accompany these numbers

- `tests/test_basic.c` — 1,160 assertions, passes under ASan + UBSan
- `tests/test_crash.c` — SIGKILL at three different points, pool fully restored
- `tests/test_chaos.c` — 300 kill rounds, final free count equals baseline
  exactly, 2,448 chunks reclaimed
- `tests/test_stress.c` — the MPMC run above
