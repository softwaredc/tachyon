# tachyon

Linux 上的零拷贝进程间通信库。两个进程通过共享内存交换消息，内核不碰 payload。

[![CI](https://github.com/softwaredc/tachyon/actions/workflows/ci.yml/badge.svg)](https://github.com/softwaredc/tachyon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![Dependencies](https://img.shields.io/badge/dependencies-none-brightgreen.svg)](CMakeLists.txt)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](https://kernel.org)

[English](README.md) | 中文

---

在同一台机器上，两个进程之间传 1KB 数据，如果用管道或 socket，要付两次内核拷贝加四次上下文切换。这些字节压根没出机器，大部分开销是白付的。

`tachyon` 把消息留在共享内存里，环里只跑一个 16 字节的描述符。

```c
char* buf;
ty_loan(topic, 4096, (void**)&buf, NULL);   /* 从内存池借一块 */
fill(buf);                                  /* 原地构造消息   */
ty_publish(topic, buf, len, key);           /* 只往环里放 16 字节 */
```

---

## 性能数据

实测，不是估算。方法和复现命令见 [docs/BENCHMARK_CN.md](docs/BENCHMARK_CN.md)。

**延迟**，256 字节消息，5 万次往返，单位纳秒（往返）：

| 传输方式 | p50 | p99 | p99.9 | 均值 |
|---|---:|---:|---:|---:|
| **tachyon（spin）** | **1,007** | **1,221** | **14,120** | **1,060** |
| tachyon（hybrid） | 22,093 | 76,159 | 164,189 | 22,515 |
| tachyon（futex） | 24,306 | 83,170 | 179,882 | 30,341 |
| Unix domain socket | 20,348 | 74,512 | 164,024 | 26,150 |
| 管道 pipe | 31,375 | 86,445 | 196,747 | 29,574 |
| TCP loopback | 37,925 | 129,706 | 209,093 | 40,770 |

**吞吐**，1 KiB 消息，20 万条：

| 传输方式 | 消息/秒 | 带宽 |
|---|---:|---:|
| **tachyon（spin）** | **3,961,955** | **3.78 GB/s** |
| tachyon（hybrid） | 3,846,457 | 3.67 GB/s |
| 管道 pipe | 2,012,017 | 1.92 GB/s |
| Unix domain socket | 1,542,371 | 1.47 GB/s |
| TCP loopback | 826,004 | 0.79 GB/s |

对比管道：延迟低约 31 倍，吞吐 2.0 倍。对比 loopback TCP：延迟低约 38 倍，吞吐 4.8 倍。

关于公平性有一点要说明：两张表里的所有传输方式（包括 tachyon）都在发送侧和接收侧各做一次 memcpy。
这是慢路径。如果在 loan 出来的内存块里直接构造消息，发送侧那次拷贝可以完全省掉，会更快。
我们测慢路径，是为了让对比不偏。

<details>
<summary>测试环境</summary>

```
CPU    : Intel Core Ultra 7 356H，16 逻辑核
内核   : 6.18.33.2-microsoft-standard-WSL2
系统   : Ubuntu 24.04.5 LTS
内存   : 15 GiB
编译器 : gcc 13.3.0, -O3
绑核   : taskset -c 2,3
```

这个环境会影响你怎么读上面两张表。WSL2 跑在虚拟化内核上，系统调用比裸机贵，
所以凡是会睡眠唤醒的传输方式都被报得偏悲观，包括 tachyon 自己的 futex 和 hybrid 模式。
spin 模式不进内核，换个环境应该接近。你在真实 Linux 上跑脚本，睡眠类传输的提升会比 tachyon 更明显。
</details>

---

## 编译

```bash
git clone https://github.com/softwaredc/tachyon.git && cd tachyon
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
```

除了 `pthread` 和 `rt` 没有别的依赖。库和测试里都不编译任何第三方代码。

Python binding：

```bash
cd bindings/python && pip install -e .
```

---

## 用法

生产者：

```c
#include <tachyon/tachyon.h>

ty_region_t* reg;
ty_region_conf_t rc = ty_region_conf_default;
rc.size = 64 << 20;
ty_region_open("demo", &rc, &reg);

ty_topic_t* topic;
ty_topic_conf_t tc = ty_topic_conf_default;
tc.partitions = 8;        /* 2 的幂 */
tc.capacity   = 1024;
ty_topic_create(reg, "orders", &tc, &topic);

char* buf;
ty_loan(topic, 4096, (void**)&buf, NULL);
memcpy(buf, &order, sizeof order);
ty_publish(topic, buf, sizeof order, order.user_id);
```

另一个进程里的消费者：

```c
ty_region_t* reg;
ty_region_open("demo", NULL, &reg);        /* conf 传 NULL：按默认附加 */

ty_topic_t* topic;
ty_topic_open(reg, "orders", &topic);

ty_msg_t m;
while (ty_subscribe(topic, partition, &m, -1) == TY_OK) {
    handle(m.data, m.len);                 /* 直接指向共享内存 */
    ty_release(topic, &m);
}
```

Python 连同一个 region：

```python
import tachyon

reg   = tachyon.Region("demo", create=False)
topic = reg.topic("orders")

msg = topic.receive(partition=3, timeout_ns=-1)
print(bytes(msg.data))      # 指向共享内存的 memoryview
msg.release()
```

更多例子在 [`examples/`](examples)，含 Go 和 Rust。

---

## 实现要点

**传描述符，不传载荷。** 变长消息的环形队列只能在两种烂选项里挑：定长槽浪费空间，紧凑排列又慢。
这里的环只放 16 字节描述符（`seq`、chunk 编号、长度），payload 放在独立的池里。

**Vyukov MPMC 环。** 生产者在一次 `fetch_add` 上竞争，而不是用 CAS 抢 `tail`，这是它能扩展到多核的原因。
正确性由每个槽自己的序列号提供。

**带代际标记的空闲链表。** 八个档位，每个档位一个 Treiber 栈，栈顶打包 32 位代际 tag。
tag 用来消除 ABA：两个 popper 想撞车，中间得恰好发生 2^32 次操作。
Hazard pointer 更严谨，但每次操作慢约 3 倍。

**崩溃恢复。** 这是大多数共享内存队列跳过的一块，也是它们大多没法上生产的原因。
进程可能在任何一条指令上被 `SIGKILL`，什么清理代码都不会跑，那一刻持有的 loan 就永久丢了，
内存池会慢慢失血。细节见 [docs/CRASH-SAFETY_CN.md](docs/CRASH-SAFETY_CN.md)，
简而言之：心跳、租约 deadline、控制面上的 robust 进程间互斥锁。

**路由。**

```
partition = mix64(key) & (partitions - 1)
```

同一个 key 进同一个分区，严格 FIFO。不同 key 进不同分区，可以并行消费，彼此之间没有顺序。
要全局有序就用 1 个分区。

---

## 等待策略

| 策略 | 延迟 | 空闲 CPU | 适用场景 |
|---|---|---|---|
| `TY_NOTIFY_SPIN` | 约 0.5 µs | 占满一个核 | 延迟压倒一切 |
| `TY_NOTIFY_FUTEX` | 本机约 12 µs | 不占 | 流量突发，核很贵 |
| `TY_NOTIFY_HYBRID` | 先自旋再睡 | 自适应 | 默认 |

---

## 局限性

- **仅 Linux。** `futex`、`PTHREAD_MUTEX_ROBUST`、`mmap` 语义移植不到 Windows/macOS。
- **仅单机。** 这是 IPC，不是网络栈。
- **跨分区无序。** 同 key 有序，不同 key 无序。
- **关闭 region 不等于删除。** 生产者必须能发完就退出、消息仍然可读。要删请调 `ty_region_remove()`。
- **单条消息上限 1 MiB。** 更大的数据该放存储里，消息里传引用。
- **内存池按档位均分。** 请按你实际用到的档位估算容量。
- **是信任模型，不是隔离模型。** 能附加就能读全部。安全边界用 `/dev/shm` 的文件权限。

---

## 语言绑定

| 语言 | 状态 | 位置 |
|---|---|---|
| C | 参考实现 | `include/tachyon/tachyon.h` |
| Python | 已实测跨进程收发 | `bindings/python` |
| Go | 编译通过，go vet 干净 | `bindings/go` |
| Rust | 已编写，本机**未运行** | `bindings/rust` |

Rust crate 有 `-sys` 原始绑定和安全封装两层，但开发机上没有 cargo，所以没跑过。
在 CI 确认之前请按"已审阅、未执行"看待。

另外 ThreadSanitizer 在本机也跑不了：WSL2 上即便一个两线程的最小程序也会报
`unexpected memory mapping`。CI 里会在真实 Linux 上跑。

---

## 目录

```
include/tachyon/tachyon.h   公开 API，从这里看起
src/                        实现（7 个文件）
tests/                      正确性、崩溃恢复、混沌、压力
benchmarks/                 产出上面的表格
examples/                   C、Python、Go、Rust
bindings/                   python / go / rust
tools/                      tachyond（回收守护进程）、tyctl（巡检）
docs/                       设计、API、基准、崩溃安全
scripts/                    基准与混沌驱动
```

## 示例

`examples/` 下是各语言可直接运行的程序，索引见
[examples/README.md](examples/README.md)。常用的几个：

```bash
./build/examples/hello_ipc              # 发一条消息，读回来
./build/examples/pingpong 256 20000     # 往返延迟分位数
./build/examples/multi_partition        # 每个分区一个消费进程
./build/examples/key_ordering           # 断言同 key 的 FIFO 保证
./build/examples/crash_recovery         # 断言 chunk 能从 SIGKILL 中恢复
```

## 文档

- [API](docs/API.md) · [API（中文）](docs/API_CN.md)

## 贡献

欢迎 issue 和 PR，见 [CONTRIBUTING.md](CONTRIBUTING.md)。标了 `good first issue` 的都是真的小问题。

## 许可

Apache-2.0，见 [LICENSE](LICENSE)。
