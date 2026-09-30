## What this changes

<!-- One paragraph. What is different after this PR? -->

## Why

<!-- The reasoning, especially if the change is subtle. For anything touching
     atomics, explain why the ordering you chose is sufficient. "Tests passed"
     is not a proof for concurrency code. -->

## Testing

- [ ] `make check` passes
- [ ] `make asan` passes (single-process tests)
- [ ] `make tsan` passes (stress test)
- [ ] `make chaos` passes (2000 SIGKILL rounds, zero leaked chunks)

## Checklist for changes touching shared memory

Only fill in what applies:

- [ ] No pointers stored in the region — offsets only, resolved with `TY_PTR()`
- [ ] New contended counters live on their own cache line
- [ ] `ctl_lock` is still never taken on the data path
- [ ] `_Static_assert`s in `src/internal.h` still hold, or `TACHYON_ABI_VERSION` was bumped

## Performance

Required if this touches `ty_publish`, `ty_subscribe`, `ty_loan`, the ring, or
the pool. Same machine, same flags, before and after:

```
command              before        after
----                 ----          ----
```

## Related

<!-- Fixes #... -->
