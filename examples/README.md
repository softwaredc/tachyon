# Examples

Every program here builds with the library and runs standalone. They are meant
to be read in this order.

## C

| Program | What it shows | Run |
|---|---|---|
| `hello_ipc.c` | fork a child, publish one message, read it back. The 60-line version of the whole library. | `./hello_ipc` |
| `zero_copy_payload.c` | `ty_loan` / `ty_publish` / `ty_abort`. Build the message in shared memory instead of copying into it. | `./zero_copy_payload` |
| `key_ordering.c` | the one ordering guarantee: same key, same partition, publish order. Exits non-zero if it is broken. | `./key_ordering` |
| `multi_partition.c` | one consumer process per partition, with per-partition throughput. The horizontal scaling story. | `./multi_partition` |
| `pingpong.c` | two-process round-trip latency with p50/p90/p99/p99.9. | `./pingpong 256 20000` |
| `crash_recovery.c` | a child borrows chunks and `SIGKILL`s itself; the reaper returns them. Exits non-zero if chunks leak. | `./crash_recovery` |

Build them all:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/examples/hello_ipc
./build/examples/pingpong 4096 50000
```

`key_ordering` and `crash_recovery` are assertions rather than demos and are
registered with ctest, so `ctest` covers them.

## Python

```bash
export TACHYON_LIB=/path/to/libtachyon.so
export PYTHONPATH=/path/to/tachyon/bindings/python
python3 examples/python/pubsub.py       # 10k messages, two processes
python3 examples/python/zero_copy.py    # loan + struct.pack_into + publish
```

The binding uses ctypes, so there is no build step and no compiler needed.

## Go

```bash
export CGO_CFLAGS="-I/path/to/tachyon/include"
export CGO_LDFLAGS="-L/path/to/tachyon/build -ltachyon"
export LD_LIBRARY_PATH="/path/to/tachyon/build:$LD_LIBRARY_PATH"
cd examples/go && go run main.go
```

## Rust

```bash
cd bindings/rust/tachyon
cargo build --release
cargo run --release --example main
```

The Rust binding has not been exercised on a machine with cargo installed; see
the note in the top-level README.

## Things the examples deliberately leave out

- Very short leases in production. `crash_recovery.c` uses 0.5 s so the demo
  finishes quickly; a real workload wants a lease comfortably longer than its
  slowest loan-to-publish window, otherwise the reaper reclaims chunks from
  processes that are simply slow.
- Error policy on a full ring. Most examples spin on `TY_ERR_FULL` because
  that keeps the code short. Choose your own policy (block, drop, spill to
  disk) — the library does not pick one for you.
