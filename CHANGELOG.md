# Changelog

All notable changes. This project follows semantic versioning for the API and
treats the shared-memory layout as its own compatibility surface: any change to
a struct in `src/internal.h` bumps `TACHYON_ABI_VERSION`, which makes old
regions refuse to attach rather than corrupt silently.

## [1.0.0] - 2026-10-01

First public release.

### Fixed during development

- **Publish ordering race.** `ty_publish` used to enqueue the descriptor before
  marking the chunk published. A consumer could dequeue in that window and,
  worse, the producer's later CAS could land on a chunk someone else had just
  borrowed, clearing a live owner. State is now set before the enqueue, with
  rollback if the enqueue fails.
- **`ty_topic_close` after `ty_region_close` no longer faults.** The region
  handle used to be freed by `ty_region_close` while topic handles still pointed
  at it, so closing a topic afterwards wrote into unmapped memory. Handles now
  carry a process-local reference count and the last holder frees. This is the
  normal destructor ordering in any garbage-collected binding; it segfaulted
  the Python binding.
- **`bytes_in_use` in the pool is now decremented on release**, not only
  incremented on loan.
- **`class_id` is bounds-checked** before indexing the pool's class array.
- **Uninitialised `topics[]` array** in `tachyond`, which read indeterminate
  handles on its first sweep.

### Added

- `ty_topic_list()` so tooling can discover topics instead of needing to know
  their names up front.
- Six C examples (`hello_ipc`, `pingpong`, `multi_partition`, `key_ordering`,
  `zero_copy_payload`, `crash_recovery`) plus Python, Go and Rust examples.
  `key_ordering` and `crash_recovery` are assertions and run under `ctest`.
- Bilingual docs for design, API, benchmarking and crash safety, plus
  publishing and growth notes.
- **Core IPC.** Regions, topics, partitions, zero-copy `loan`/`publish`
  cycles, and `subscribe`/`release` consumption.
- **Vyukov MPMC descriptor ring.** Fixed 16-byte descriptors, payload in a
  separate pool, so variable-length messages work without variable-length slots.
- **Size-classed chunk pool.** Eight power-of-two classes, generation-tagged
  Treiber free lists, upward borrowing when a class is exhausted.
- **Key routing.** `partition = mix64(key) & (partitions - 1)`, with strict FIFO
  per key and no ordering across keys.
- **Three wait policies.** Spin, futex, and hybrid, selectable per topic.
- **Crash recovery.** Heartbeats, lease deadlines, bitmap-driven sweeps, and a
  robust process-shared mutex on the control plane. Optional in-process reaper
  thread or external `tachyond` daemon.
- **Python binding** via ctypes: no compiler required to install.
- **Go and Rust bindings.**
- **Benchmarks** comparing against pipe, Unix socket, and loopback TCP, with
  exact percentiles computed from the full sample set.
- **Test suite:** correctness (1,164 assertions under ASan and UBSan), crash
  recovery, SIGKILL chaos (2,000 rounds), and a 4-producer/4-consumer stress
  test.
- **Docs:** design, API reference, benchmark methodology, crash safety.

### Measured on the reference machine

| | tachyon | pipe | unix socket | TCP loopback |
|---|---|---|---|---|
| latency p50 (256 B, round trip) | **1.01 µs** | 31.4 µs | 20.3 µs | 37.9 µs |
| throughput (1 KiB) | **3.78 GB/s** | 1.92 GB/s | 1.47 GB/s | 0.79 GB/s |

Environment: Intel Core Ultra 7 356H, WSL2 kernel 6.18.33, Ubuntu 24.04.
See [docs/BENCHMARK.md](docs/BENCHMARK.md).

### Notes

- Spin mode is the fair comparison for latency; futex and hybrid numbers are
  pessimistic on WSL2 because syscalls and futex operations cost more there
  than on a bare-metal kernel.
- The sleeping transports improve more than tachyon does when you move to real
  Linux hardware.

## [Unreleased]

Nothing yet. Planned, in rough priority order:

- batch `subscribe` (amortise atomics across several messages)
- `io_uring`-friendly notification for producers
- NUMA-aware pool placement
- Rust safe wrapper with a typed message API
- region growth via a second mapping
