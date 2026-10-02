# 基准测试方法

README 里的每个数字都由 `benchmarks/bench_ipc.c` 在真实机器上跑出来。这里写清怎么跑，以便你复现，或者推翻它。

[English](BENCHMARK.md) | 中文

## 复现

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
./scripts/run_benchmarks.sh          # 跑完整矩阵，结果写入 results.txt
```

也可以只跑一项：

```bash
./build/bench_ipc tachyon latency     256  50000
./build/bench_ipc tachyon throughput  1024 200000
TY_NOTIFY=spin ./build/bench_ipc tachyon latency 256 50000
```

`TY_NOTIFY` 选等待策略：`spin`、`futex`、`hybrid`（默认）。一个二进制覆盖整张表。

## 测的是什么

**延迟** —— 两个 fork 出来的进程之间的往返。父发、子回、父收，计时把三步都包住。报的是往返值；README 里的"单向"是它的一半，并已标注为估算。

**吞吐** —— 父进程以消费者能跟上的速度连发 `count` 条，计时只在 `waitpid()` 返回后停止，所以最后一条消息确定已送达。

## 自己给自己定的公平规则

列出来是因为，一个悄悄偏向自家库的基准比没有基准更糟。

1. **每种传输方式的 memcpy 次数相同。** 发送侧一次、接收侧一次，tachyon 也一样（`ty_send` / `subscribe` + `memcpy`）。在 loan 出来的块里直接构造消息可以省掉发送侧那次，会更快，但我们故意测慢路径。
2. **进程拓扑一致。** 两个进程，收发各一，用 `taskset` 绑到两个核。
3. **计时前先预热。** 先跑 1000 轮不计入。它们唯一的作用是进入稳态，不是因为数字难看才丢掉。
4. **分位数精确。** 全部样本排序。p99.9 是那个排位上的真实样本，不是直方图近似。
5. **不做离群值修剪。** 不会因为有数字不好看就丢。这也是为什么虚拟化内核上的尾值比中位数难看——因为它本来就难看。

### tachyon 的测法和别人有什么不同

文件描述符类传输（管道、socket）由父进程创建、跨 `fork()` 继承。tachyon 则让子进程通过 `ty_region_open()` 和 `ty_topic_open()` **重新附加**，所以它的数字里包含真实服务要走的那条附加路径。这是更难的测法，是故意选的。

## 参考环境

```
CPU    : Intel Core Ultra 7 356H，16 逻辑核
内核   : 6.18.33.2-microsoft-standard-WSL2
系统   : Ubuntu 24.04.5 LTS
内存   : 15 GiB
编译器 : gcc 13.3.0, -O3
绑核   : taskset -c 2,3
```

### 为什么环境会影响解读

WSL2 跑在微软的虚拟化内核上。凡是陷入内核的操作——futex 睡眠唤醒、管道和 socket 的 `read`/`write`——都比裸机贵不少。

这意味着：

- **tachyon 的 spin 模式几乎不受影响。** 它不进内核，换个环境应该很接近。
- **所有会睡眠的传输都被报得偏悲观**，包括 tachyon 自己的 futex 和 hybrid 模式。

所以那个头条对比（spin 对管道/socket/TCP）在某种意义上低估了裸机上的差距，同时又高估了 tachyon futex 模式的延迟。做选型前请在自己的硬件上跑一遍脚本——对你有意义的数字是你跑出来的那组。

## 结果

### 延迟，256 字节消息，5 万次往返（纳秒，往返）

| 传输方式 | p50 | p99 | p99.9 | 均值 |
|---|---:|---:|---:|---:|
| tachyon（spin） | 1,007 | 1,221 | 14,120 | 1,060 |
| tachyon（hybrid） | 22,093 | 76,159 | 164,189 | 22,515 |
| tachyon（futex） | 24,306 | 83,170 | 179,882 | 30,341 |
| Unix domain socket | 20,348 | 74,512 | 164,024 | 26,150 |
| pipe | 31,375 | 86,445 | 196,747 | 29,574 |
| TCP loopback | 37,925 | 129,706 | 209,093 | 40,770 |

### 吞吐，1 KiB 消息，20 万条

| 传输方式 | 消息/秒 | 带宽 |
|---|---:|---:|
| tachyon（spin） | 3,961,955 | 3.78 GB/s |
| tachyon（hybrid） | 3,846,457 | 3.67 GB/s |
| pipe | 2,012,017 | 1.92 GB/s |
| Unix domain socket | 1,542,371 | 1.47 GB/s |
| TCP loopback | 826,004 | 0.79 GB/s |

### 进程内并发

4 个生产者线程 × 2 万条，4 个消费者线程，8 个分区 × 128 槽（`tests/test_stress.c`）：

```
8 万条消息，0.129 秒（约 62 万条/秒）
0 丢失，0 重复，0 损坏
```

## 配套的正确性测试

- `tests/test_basic.c` —— 1160 条断言，ASan + UBSan 下通过
- `tests/test_crash.c` —— 三个不同位置的 SIGKILL，内存池完整恢复
- `tests/test_chaos.c` —— 300 轮 kill，最终空闲数精确等于基线，回收 2448 个 chunk
- `tests/test_stress.c` —— 上面的 MPMC 用例

## 本机跑不了的那一项

ThreadSanitizer 在 WSL2 上不可用：即便一个两线程的最小程序也会报
`unexpected memory mapping`。这是 WSL2 的内存布局限制，不是本项目的问题。
TSan 在 CI（GitHub Actions 的真实 Linux）上跑。
