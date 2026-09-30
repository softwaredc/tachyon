# Contributing

Thanks for considering it. This is a concurrency library, which means the usual
rules apply and then some: a change that looks obviously correct is still not
correct until it has been argued about.

## Before you start

- **Open an issue first** for anything larger than a typo. Concurrency design
  decisions are cheap to discuss and expensive to revert.
- Look for `good first issue`. Those are genuinely small, not leftover hard
  problems.

## Building and testing

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
make check              # full suite
make asan               # ASan + UBSan on the single-process tests
make tsan               # TSan on the stress test
make chaos              # 2000 rounds of SIGKILL, assert zero leaks
```

There are no dependencies to install. If the build needs something beyond a C
compiler and libc, that is a bug in the build, not in your environment.

## What a good patch looks like

1. **One change per PR.** A refactor plus a bug fix is two PRs.
2. **Tests.** Every behaviour change needs one:
   - data-path change → `tests/test_stress.c` or `test_basic.c`
   - crash/recovery change → `tests/test_crash.c` or `test_chaos.c`
3. **Explain the memory ordering.** If you add or change an atomic, the PR
   description must say *why* that ordering is sufficient. "It passed the tests"
   is not a proof for anything involving atomics, because these bugs reproduce
   once in several hundred thousand messages.
4. **Benchmarks if it is hot.** If your change touches `ty_publish`,
   `ty_subscribe`, `ty_loan`, or the ring, include before/after numbers from
   `scripts/run_benchmarks.sh` on the same machine.

## Rules specific to this codebase

These are not style preferences; violating them breaks the library.

- **No pointers in the shared region.** Only offsets, resolved through
  `TY_PTR()`. No STL containers, no vtables, no embedded pointers.
- **Every contended counter gets its own cache line.** If you add a field next
  to `enq_pos`, `deq_pos`, or `free_head`, you have introduced false sharing and
  probably cost more than your feature gained.
- **The ring is never locked.** `ctl_lock` is for the control plane only:
  creation, allocation, reaping.
- **`_Static_assert` the layout.** If you change a shared struct, bump
  `TACHYON_ABI_VERSION` and update the assertions in `src/internal.h`.
- **Publish before you wake.** Chunk state transitions must happen before the
  descriptor becomes visible. This one caused a real bug during development;
  see the comment in `ty_publish()`.

## Style

- C11, 4-space indent, 100-column soft limit.
- `clang-format` config is in the repo root — run it, but do not let it reformat
  unrelated files.
- Comments explain *why*. The code already says what.
- Commit messages: imperative mood, one line of summary, and a body that
  explains the reasoning when it is not obvious.

## Reporting a bug

The most useful bug report includes:

- the kernel version and CPU model,
- whether it reproduces under `-O0`, `-O2`, and `-O3`,
- whether TSan or ASan says anything,
- the smallest program that shows it.

For hangs and lost messages, also include `ty_topic_stats()` output:
`published`, `consumed`, `dropped`, `reclaimed`, and `loan_nomem` narrow the
problem down faster than anything else.

## License

By contributing you agree that your work is licensed under Apache-2.0, the same
as the rest of the project.
