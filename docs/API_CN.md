# API 参考

完整的公开接口就是 `include/tachyon/tachyon.h`。这篇补上头文件放不下的东西：每个调用的代价、保证，以及哪些错误是正常现象而不是异常。

[English](API.md) | 中文

## 约定

- 所有函数返回 `ty_err_t`。`TY_OK` 为 0，其余为负。
- `conf == NULL` 表示用 `ty_*_conf_default`。
- `timeout_ns < 0` 永久阻塞，`0` 只轮询一次，`> 0` 等这么久。
- handle（`ty_region_t*`、`ty_topic_t*`）是进程私有的。别指望跨 `fork()` 还能用，子进程请重新打开。

## Region

### `ty_region_open(name, conf, out)`

创建或附加 region。`conf` 传 NULL 时按默认附加，消费者通常要的就是这个。

| 错误 | 含义 | 怎么办 |
|---|---|---|
| `TY_ERR_INVAL` | 名字为空、过长或含斜杠 | 改名字 |
| `TY_ERR_NOENT` | 不存在且 `create` 为 0 | 先创建 |
| `TY_ERR_ABI` | region 由不兼容版本创建 | 删掉重建 |
| `TY_ERR_MAP` | `mmap` 失败 | 查 `/dev/shm` 空间和限制 |

### `ty_region_close(reg)`

解除映射。**不会删除 region**——生产者必须能发完就退出、而消息仍然可读。删除是显式的。

handle 会被释放，和 `fclose()`、`curl_easy_cleanup()` 一样。只调一次，调完 handle 就废了，再用是未定义行为。内部的 magic 字段只能拦截尚未释放的路径，真正保护你的是这个契约，不是运行时检查。

### `ty_region_remove(name, dir)`

按名字删除。已经附加的其他进程继续可用，只是这个名字不再对新人解析。region 从来没存在过会返回 `TY_ERR_NOENT`；Python binding 把它做成了幂等，因为初始化代码几乎总是想"确保它没了"。

### `ty_heartbeat(reg)`

刷新本进程存活时间戳。只有应用在 tachyon 之外长时间阻塞时才需要。

## Topic

### `ty_topic_create(reg, name, conf, out)`

多进程并发创建是安全的：只有一个成功，其他拿到 `TY_ERR_EXIST`，此时应改调 `ty_topic_open()`。这是正常的启动竞争，不是异常。

`partitions` 和 `capacity` 会被向上取整到 2 的幂，掩码运算要求如此。

| 字段 | 默认 | 说明 |
|---|---|---|
| `partitions` | 8 | 2 的幂；越多并行度越高、顺序性越弱 |
| `capacity` | 1024 | 每分区槽位数，也就是你的突发缓冲 |
| `pool_bytes` | 16 MiB | 在 8 个档位间均分 |
| `notify` | `TY_NOTIFY_HYBRID` | spin / futex / hybrid |

### `ty_topic_open(reg, name, out)`

附加已存在的 topic。还没有时返回 `TY_ERR_NOENT`。

### `ty_topic_list(reg, names, capacity)`

枚举 region 里现有的 topic。`names` 传 NULL 就只计数。返回值可能大于 `capacity`。

```c
char names[TACHYON_MAX_TOPICS][TACHYON_MAX_NAME + 1];
int n = ty_topic_list(reg, names, TACHYON_MAX_TOPICS);
```

这个接口存在是因为巡检工具离不开它：CLI 没有别的办法发现 region 里有什么，否则只能打开它已经知道名字的 topic。`tyctl list` 就建在它上面。

## 生产侧

### `ty_loan(topic, size, ptr, cap)`

借一块可写内存。这是零拷贝的原语：直接在共享内存里构造消息，中间不存在任何缓冲区。`cap` 拿到可用容量，可能大于 `size`（你得到整个档位）。

| 错误 | 含义 |
|---|---|
| `TY_ERR_TOOBIG` | 超过 1 MiB，请传引用 |
| `TY_ERR_NOMEM` | 该档位（及所有更大的档位）已空，属于背压，重试即可 |
| `TY_ERR_BUSY` | 同一瞬间回收器释放了这个 chunk，重试 |

### `ty_publish(topic, ptr, len, key)`

提交借出的 chunk 到 `hash(key) & (partitions - 1)`。只有 16 字节移动。失败时 chunk 仍归你——重试或 `ty_abort()`，因为丢着不管会让它离开流通整整一个租约周期。

### `ty_abort(topic, ptr)`

不发布地归还。开销很小，比泄漏好得多。

### `ty_send(topic, src, len, key)`

便捷封装：loan + memcpy + publish。一次拷贝，所以它比显式两步慢。消息很大或已在你可直写的地方时，用 `ty_loan()`。

## 消费侧

### `ty_subscribe(topic, partition, msg, timeout_ns)`

从一个分区收一条。`msg->data` 指向共享内存，在 `ty_release()` 之前有效。

| 错误 | 含义 |
|---|---|
| `TY_ERR_EMPTY` | 用 `timeout_ns == 0` 轮询，没有消息 |
| `TY_ERR_TIMEOUT` | 阻塞等待后没有消息到达 |

### `ty_subscribe_any(topic, hint, msg, timeout_ns)`

从任何一个有消息的分区收。适合单个消费者管所有分区。它用短阻塞轮转扫描，而不是在一个 futex 上长睡，因为 Linux 没有"等这些地址里的任意一个"这种原语——睡在分区 0 上会漏掉分区 5 来的消息。

### `ty_release(topic, msg)`

归还 chunk。**每条消息正好一次。** 漏掉会泄漏一个 chunk 直到回收器发现；调两次是未定义行为。

## 背压与自检

### `ty_wait_room(topic, key, timeout_ns)`

等到目标分区有空间。想显式施加背压时，用它比自己循环重试 `ty_send()` 好。

注意这是自旋等待：ring 满了没有"消费者侧唤醒"这条路。加一个生产者 futex 会在发布路径上多一次系统调用，而需要等空间的调用方本来就已经在慢路径上了。除非你有富余的核，否则请传 timeout。

### `ty_depth(topic, partition, out)`

某个分区的待处理数，传 `-1` 拿全部分区的总数。

### `ty_topic_stats(topic, out)`

`published`、`consumed`、`dropped`、`loan_ok`、`loan_nomem`、`reclaimed`、`wait_wakeups`、`spin_spins`。稳态下 `reclaimed` 非 0 说明你的系统里有进程在被杀；`loan_nomem` 持续上涨说明 `pool_bytes` 给小了。

### `ty_pool_info(topic, out, n)`

每档的 chunk 大小、数量、当前空闲数。它要遍历空闲链表，所以只用于诊断，别放热路径。

### `ty_reclaim_once(topic)`

强制清扫一次租约，返回回收数量。并发调用安全，只有一个调用方真正干活，其余看到 0。

## 路由

### `ty_route(topic, key)`

某个 key 落到哪个分区，不发数据也能算。想给固定 key 区间分配 worker 时有用。

```c
partition = mix64(key) & (partitions - 1);
```

**契约：** 同 key 落到同分区，严格 FIFO。不同 key 之间没有顺序保证。要全局有序就用 1 个分区。

## 计时辅助

### `ty_now_ns()`

`CLOCK_MONOTONIC_RAW`，纳秒。稳妥可移植。

### `ty_now_ns_fast()`

基于 TSC。更快，但要求 TSC 是恒定且同步的——先用 `ty_tsc_usable()` 判断，或者接受虚拟机迁移可能让它说谎。它会对 `CLOCK_MONOTONIC_RAW` 校准一次。

## 错误码

| 码 | 值 | 含义 |
|---|---:|---|
| `TY_OK` | 0 | 成功 |
| `TY_ERR_INVAL` | -1 | 参数非法 |
| `TY_ERR_EXIST` | -2 | 已存在 |
| `TY_ERR_NOENT` | -3 | region 或 topic 不存在 |
| `TY_ERR_NOMEM` | -4 | 池或 region 耗尽 |
| `TY_ERR_FULL` | -5 | 环满，背压，重试 |
| `TY_ERR_EMPTY` | -6 | 轮询没消息 |
| `TY_ERR_TIMEOUT` | -7 | 截止时间到了 |
| `TY_ERR_TOOBIG` | -8 | 载荷超过 1 MiB |
| `TY_ERR_STATE` | -9 | 状态不对 |
| `TY_ERR_ABI` | -10 | 布局版本不兼容 |
| `TY_ERR_IO` | -11 | 系统错误，看 `errno` |
| `TY_ERR_MAP` | -12 | `mmap` 失败 |
| `TY_ERR_BUSY` | -13 | 有竞争，重试 |
| `TY_ERR_INTERNAL` | -99 | 不变式被破坏，请提 bug |

`TY_ERR_FULL` 和 `TY_ERR_NOMEM` 在负载下是**正常现象**，不是 bug：它们是库在告诉你消费者落后了。
