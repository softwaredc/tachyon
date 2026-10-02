# 崩溃安全

这篇文档要解决的问题不冷门，就是下面这件事：

> 一个进程在持有 loan 的时候被 `SIGKILL`。什么都不会执行。没有 `atexit`，没有析构函数，没有清理钩子。那块内存从此不可达，每过一秒，你的服务在三个小时后开始返回 `NOMEM` 这件事就更难解释一点。

`SIGKILL` 无法捕获，这是全部难点。C 语言没有 `finally`，清理无法"安排"，只能被**推断**出来。

[English](CRASH-SAFETY.md) | 中文

## 所有权是租借的，不是持有的

每个在外的 chunk 带着两个字段：

```c
atomic_int32_t  owner_pid;
atomic_uint64_t lease_expire_ns;
```

loan 时设置两者。publish 或 release 时清除。超过 deadline 还带着标记的，就是回收候选。

每档一个**位图**标记当前被持有的 chunk，所以一次清扫只检查候选，不用遍历整个池。一百万个 chunk 时，这是读约 15000 个字，而不是一百万个头部。

## 三层

### 第一层：心跳

每个接入的进程在 `procs[128]` 里占一个槽，调用库时心跳时间戳会推进。如果你的应用在 tachyon 之外长时间阻塞，就该调 `ty_heartbeat()`，否则会被当成死了。

这一层让回收器能区分*慢*和*死*，而这两者的区别就是"回收一块内存"和"损坏一条活消息"的区别。

### 第二层：租约与回收器

`ty_pool_loan()` 设 `lease_expire_ns = now + lease_ns`（默认 5 秒）。回收器——可以是进程内线程（`spawn_reaper = 1`），也可以是外部 `tachyond` 守护进程——按 `lease_ns / 4` 的节奏清扫，回收已放弃的块。

因为已确认死亡不需要宽限期（见下），常见情况下恢复是立即的，不用等 deadline。

### 第三层：控制面的 robust 互斥锁

创建 topic、分配槽位、回收这些操作都拿一把这样配置的锁：

```c
pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
pthread_mutexattr_setrobust (&attr, PTHREAD_MUTEX_ROBUST);
```

如果持锁者被杀，下一个获取锁的人会拿到 `EOWNERDEAD`，修好控制面状态后调 `pthread_mutex_consistent()`。没有这一层，一个进程在创建 topic 时挂掉，会让所有其他进程永远无法再创建 topic。

**这把锁在数据路径上永远不会被取。** publish 和 subscribe 完全不碰锁，这是 README 里那些数字能成立的前提。

## 判定规则

只有能证明没人还要用这块内存时才回收：

```c
if (owner_pid <= 0)              回收;    /* 无主 */
if (!ty__pid_alive(owner_pid))   回收;    /* 已确认死亡，不必等 */
if (now < lease_expire_ns)       保留;    /* 租约未到            */
if (!ty_owner_progressing(...))  回收;    /* 心跳已停            */
/* 否则：活着且在推进，只是慢 —— 不动它 */
```

最后一种情况最重要，也最容易做错。一个在 syscall 里阻塞超过 `lease_ns` 的生产者**不能**失去它的 chunk：它马上就要往里写数据，把这块内存给别人就是破坏活数据。慢不等于死。

`tests/test_crash.c` 里有 `test_live_producer_not_reclaimed`：拿住一块内存 1.5 秒，租约只有 200 毫秒，同时持续打心跳，然后断言它一次都没被回收、并且还能正常 publish。缺了这个测试的崩溃恢复方案，就是一个会损坏数据的方案。

## 我们接受的失效模式

写清楚，因为"崩溃安全"没有边界就是营销：

| 情况 | 行为 |
|---|---|
| 持有 loan 时被杀 | chunk 被回收，通常是立即 |
| 收到消息、release 前被杀 | chunk 被回收 |
| 活着但阻塞超过 `lease_ns` | 只要心跳在推进就**不**回收 |
| 活着、心跳已停、租约到期 | 回收 |
| 接入进程超过 128 个 | 多出来的无法证明存活，退化为只按租约 deadline 判断 |
| `lease_ns` 短于你最长的合法持有时长 | 活着的 owner 也可能被回收 —— 请把这个值调大 |

128 个进程那条是部署前值得核对的唯一运维约束。超过之后，存活证明退化为按 deadline 判断，安全但恢复更慢。

## 自己验证

```bash
./scripts/chaos_kill.sh 2000
```

它会 fork 子进程 2000 次，在消息生命周期的三个不同位置杀掉它，最后断言内存池的空闲数精确回到基线。参考机上 300 轮回收了 2448 个 chunk，零丢失。

```c
/* 也可以自己强制清扫一次 */
uint64_t n = ty_reclaim_once(topic);
```
