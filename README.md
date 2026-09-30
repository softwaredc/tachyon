# tachyon

Zero-copy IPC for Linux. Two processes exchange messages through shared memory;
the kernel never touches the payload.

[![CI](https://github.com/softwaredc/tachyon/actions/workflows/ci.yml/badge.svg)](https://github.com/softwaredc/tachyon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![Dependencies](https://img.shields.io/badge/dependencies-none-brightgreen.svg)](CMakeLists.txt)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](https://kernel.org)

[中文版](README_CN.md) | English

---

Sending 1 KB between two processes on the same machine costs two kernel copies
and four context switches if you use a pipe or a socket. The bytes never leave
the machine, so most of that work is pure overhead.

`tachyon` keeps the message in shared memory and moves only a 16-byte descriptor
through the ring.

```c
char* buf;
ty_loan(topic, 4096, (void**)&buf, NULL);   /* borrow memory from the pool */
fill(buf);                                  /* build the message in place  */
ty_publish(topic, buf, len, key);           /* 16 bytes enter the ring     */
```

---

## Numbers

Measured, not estimated. Method and commands to reproduce:
[docs/BENCHMARK.md](docs/BENCHMARK.md).

**Latency**, 256-byte messages, 50,000 round trips, nanoseconds per round trip:

| Transport | p50 | p99 | p99.9 | mean |
|---|---:|---:|---:|---:|
| **tachyon (spin)** | **1,007** | **1,221** | **14,120** | **1,060** |
| tachyon (hybrid) | 22,093 | 76,159 | 164,189 | 22,515 |
| tachyon (futex) | 24,306 | 83,170 | 179,882 | 30,341 |
| Unix domain socket | 20,348 | 74,512 | 164,024 | 26,150 |
| pipe | 31,375 | 86,445 | 196,747 | 29,574 |
| TCP loopback | 37,925 | 129,706 | 209,093 | 40,770 |

**Throughput**, 1 KiB messages, 200,000 messages:

| Transport | msgs/sec | bandwidth |
|---|---:|---:|
| **tachyon (spin)** | **3,961,955** | **3.78 GB/s** |
| tachyon (hybrid) | 3,846,457 | 3.67 GB/s |
| pipe | 2,012,017 | 1.92 GB/s |
| Unix domain socket | 1,542,371 | 1.47 GB/s |
| TCP loopback | 826,004 | 0.79 GB/s |

Against a pipe that is about 31x lower latency and 2.0x the throughput.
Against loopback TCP, 38x lower latency and 4.8x the throughput.

One caveat on fairness: every transport in both tables does one memcpy on the
send side and one on the receive side, including tachyon. That is the slow path.
Building the message inside a loaned chunk removes the send-side copy and is
faster. The tables use the slow path so the comparison is not tilted.

<details>
<summary>Test environment</summary>

```
CPU       : Intel Core Ultra 7 356H, 16 logical cores
Kernel    : 6.18.33.2-microsoft-standard-WSL2
OS        : Ubuntu 24.04.5 LTS
Memory    : 15 GiB
Compiler  : gcc 13.3.0, -O3
Pinning   : taskset -c 2,3
```

This matters when reading the tables. WSL2 runs on a virtualised kernel where
syscalls cost more than on bare metal, so every transport that sleeps and wakes
is reported pessimistically, including tachyon's own futex and hybrid modes.
Spin mode does not enter the kernel and should reproduce closely elsewhere. If
you run the script on a real Linux host, the sleeping transports will improve
more than tachyon will.
</details>

---

## Build

```bash
git clone https://github.com/softwaredc/tachyon.git && cd tachyon
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
```

No dependencies beyond `pthread` and `rt`. Nothing third-party is compiled, in
the library or in the tests.

Python binding:

```bash
cd bindings/python && pip install -e .
```

---

## Usage

Producer:

```c
#include <tachyon/tachyon.h>

ty_region_t* reg;
ty_region_conf_t rc = ty_region_conf_default;
rc.size = 64 << 20;
ty_region_open("demo", &rc, &reg);

ty_topic_t* topic;
ty_topic_conf_t tc = ty_topic_conf_default;
tc.partitions = 8;        /* power of two */
tc.capacity   = 1024;
ty_topic_create(reg, "orders", &tc, &topic);

char* buf;
ty_loan(topic, 4096, (void**)&buf, NULL);
memcpy(buf, &order, sizeof order);
ty_publish(topic, buf, sizeof order, order.user_id);
```

Consumer in another process:

```c
ty_region_t* reg;
ty_region_open("demo", NULL, &reg);        /* NULL conf: attach with defaults */

ty_topic_t* topic;
ty_topic_open(reg, "orders", &topic);

ty_msg_t m;
while (ty_subscribe(topic, partition, &m, -1) == TY_OK) {
    handle(m.data, m.len);                 /* points into shared memory */
    ty_release(topic, &m);
}
```

Python, against the same region:

```python
import tachyon

reg   = tachyon.Region("demo", create=False)
topic = reg.topic("orders")

msg = topic.receive(partition=3, timeout_ns=-1)
print(bytes(msg.data))      # memoryview over shared memory
msg.release()
```

More examples in [`examples/`](examples), including Go and Rust.

---

## How it works

**Descriptors, not payloads.** A ring of variable-length messages has to pick
between fixed slots, which waste space, and contiguous packing, which is slow.
The ring here carries only a 16-byte descriptor (`seq`, chunk id, length) and
the payload lives in a separate pool.

**Vyukov MPMC ring.** Producers contend on one `fetch_add` instead of fighting
over `tail` with CAS, which is what lets it scale past a few cores. Per-cell
sequence numbers provide correctness.

**Generation-tagged free lists.** Eight size classes, each with a Treiber stack
whose head packs a 32-bit generation tag. The tag defeats ABA: two poppers would
need exactly 2^32 intervening operations to collide. Hazard pointers are
stronger and about 3x slower per operation.

**Crash recovery.** This is the part most shared-memory queues skip, and it is
why most of them are unusable in production. A process can be `SIGKILL`ed at any
instruction and nothing runs to clean up, so a loan held at that moment would be
lost forever and the pool would slowly bleed. Details in
[docs/CRASH-SAFETY.md](docs/CRASH-SAFETY.md); in short: heartbeats, lease
deadlines, and a robust process-shared mutex on the control plane.

**Routing.**

```
partition = mix64(key) & (partitions - 1)
```

Same key means same partition and strict FIFO. Different keys go to different
partitions and can be consumed in parallel, with no ordering between them. Use
one partition if you need total order.

---

## Wait policies

| Policy | Latency | Idle CPU | Use for |
|---|---|---|---|
| `TY_NOTIFY_SPIN` | about 0.5 µs | a full core | latency above all else |
| `TY_NOTIFY_FUTEX` | about 12 µs here | none | bursty traffic, cores are scarce |
| `TY_NOTIFY_HYBRID` | spins, then sleeps | adaptive | default |

---

## Limitations

- **Linux only.** `futex`, `PTHREAD_MUTEX_ROBUST` and `mmap` semantics do not
  port to Windows or macOS.
- **Single machine.** This is IPC, not a network stack.
- **No cross-partition ordering.** Same key is ordered; different keys are not.
- **Regions are not deleted by closing.** A producer must be able to publish and
  exit while its messages remain readable. Call `ty_region_remove()` to delete.
- **1 MiB per message.** Larger payloads belong in a store, with a reference in
  the message.
- **The pool is split evenly across size classes.** Size the topic for the class
  you actually use.
- **Trust, not isolation.** Anything that can attach can read everything. Use
  filesystem permissions on `/dev/shm`.

---

## Bindings

| Language | State | Location |
|---|---|---|
| C | reference | `include/tachyon/tachyon.h` |
| Python | tested cross-process | `bindings/python` |
| Go | builds clean, `go vet` clean | `bindings/go` |
| Rust | written but not executed here | `bindings/rust` |

The Rust crate has `-sys` bindings and a safe wrapper, but the machine this was
developed on has no `cargo`, so it has not been run. Count it as
reviewed-but-unexecuted until CI confirms it.

ThreadSanitizer also does not run here: it fails on WSL2 even for a trivial
two-thread program, with `unexpected memory mapping`. CI runs it on real Linux.

---

## Layout

```
include/tachyon/tachyon.h   the API; start here
src/                        implementation (7 files)
tests/                      correctness, crash recovery, chaos, stress
benchmarks/                 produces the tables above
examples/                   C, Python, Go, Rust
bindings/                   python / go / rust
tools/                      tachyond (reaper daemon), tyctl (inspector)
docs/                       design, API, benchmarking, crash safety
scripts/                    benchmark and chaos drivers
```

## Examples

`examples/` holds runnable programs for every language binding; see
[examples/README.md](examples/README.md). The short version:

```bash
./build/examples/hello_ipc              # publish one message, read it back
./build/examples/pingpong 256 20000     # round-trip latency percentiles
./build/examples/multi_partition        # one consumer process per partition
./build/examples/key_ordering           # asserts the FIFO-per-key guarantee
./build/examples/crash_recovery         # asserts chunks survive a SIGKILL
```

## Docs

- [Design](docs/DESIGN.md) · [设计（中文）](docs/DESIGN_CN.md)
- [API](docs/API.md) · [API（中文）](docs/API_CN.md)
- [Benchmarks](docs/BENCHMARK.md) · [基准测试（中文）](docs/BENCHMARK_CN.md)
- [Crash safety](docs/CRASH-SAFETY.md) · [崩溃安全（中文）](docs/CRASH-SAFETY_CN.md)
- [Publishing to GitHub](docs/PUBLISHING.md) · [发布步骤（中文）](docs/PUBLISHING_CN.md)
- [Getting stars](docs/GETTING-STARS.md) · [涨星策略（中文）](docs/GETTING-STARS_CN.md)
- [Article: how it works](docs/articles/zero-copy-ipc-en.md) · [同名长文（中文）](docs/articles/wechat-zero-copy-ipc.md)

## Contributing

Issues and PRs welcome, see [CONTRIBUTING.md](CONTRIBUTING.md). Anything marked
`good first issue` is genuinely small.

## License

Apache-2.0, see [LICENSE](LICENSE).
