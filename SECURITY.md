# Security policy

## Scope

tachyon is a local inter-process communication library. Its threat model is
narrow and worth stating precisely, because "IPC" is often assumed to be a
security boundary when it is not one.

## What tachyon does and does not protect

**It does not provide isolation.** Any process that can attach to a region can
read and write every message in it. There is no authentication, no
authorisation, no per-topic access control, and no encryption. This is inherent
to shared memory: the whole point is that both sides can see the same bytes
without a privileged intermediary copying them.

**The access control that exists is the filesystem's.** Regions live in
`/dev/shm` by default, and POSIX shared memory permissions decide who can
attach. If you need a boundary between mutually untrusting processes, use a
socket with real credentials — not this library.

If you pass `conf.dir`, the same applies to that directory.

## Supported versions

| Version | Supported |
|---|---:|
| 1.0.x | yes |

## Reporting a vulnerability

Please report via GitHub's private security advisory feature on this repository
rather than a public issue, for anything in these categories:

- memory corruption reachable from a region that another local user controls
- a way to make one attached process crash or hang all others
- a way to read data from a region you should not be able to attach to
- denial of service against a process that merely attaches

**Not vulnerabilities** (please open a normal issue instead):

- another local process with the same uid reading your messages — that is the
  documented trust model
- a process intentionally killing its peers, or intentionally corrupting a
  region it can write
- `TY_ERR_NOMEM` / `TY_ERR_FULL` under load — that is backpressure

## What to include

- kernel version, CPU architecture, compiler and flags
- whether it reproduces under ASan/UBSan/TSan
- the smallest reproducer you can produce
- `ty_topic_stats()` output if the issue involves lost or hung messages

## Response expectations

Best effort for an unpaid open source project: an acknowledgement within a few
days, an assessment within two weeks. Fixes for confirmed issues ship in a patch
release.
