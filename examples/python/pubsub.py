#!/usr/bin/env python3
"""
Producer / consumer over tachyon, two processes, no sockets.

Run:
    TACHYON_LIB=/path/to/libtachyon.so python3 pubsub.py

The child process attaches to the same region by name, the same way a separate
service would. Nothing is inherited across fork() except the region name.
"""
import os
import sys
import time

import tachyon

REGION = "py_pubsub_demo"
TOPIC = "events"
MESSAGES = 10_000


def producer():
    tachyon.Region.remove(REGION)
    reg = tachyon.Region(REGION, create=True, size=64 << 20, spawn_reaper=False)
    topic = reg.topic(TOPIC, partitions=4, capacity=1024, pool_bytes=16 << 20)

    print(f"producer: topic has {topic.partitions} partitions, "
          f"{topic.capacity} slots each")

    payload = b"x" * 256
    t0 = time.perf_counter()
    for i in range(MESSAGES):
        while True:
            try:
                topic.send(payload, key=i)
                break
            except tachyon.TachyonError as e:
                # Ring full or pool exhausted. tachyon does not pick a policy
                # for you: here we wait for room, which is the right answer
                # for a demo and often the wrong one for a trading system.
                if e.code in (tachyon.ERR_FULL, tachyon.ERR_NOMEM):
                    time.sleep(0.001)
                    continue
                raise
    elapsed = time.perf_counter() - t0

    print(f"producer: {MESSAGES} messages in {elapsed * 1e3:.1f} ms "
          f"({MESSAGES / elapsed:,.0f} msg/s)")
    print("producer: stats", {k: v for k, v in topic.stats().items() if v})

    # Blocks until the child has drained everything.
    deadline = time.time() + 15
    while topic.depth() > 0 and time.time() < deadline:
        time.sleep(0.01)

    reg.close()
    tachyon.Region.remove(REGION)


def consumer():
    # Give the producer a moment to create the region.
    for _ in range(100):
        try:
            reg = tachyon.Region(REGION, create=False, spawn_reaper=False)
            break
        except tachyon.TachyonError:
            time.sleep(0.05)
    else:
        print("consumer: region never appeared", file=sys.stderr)
        return 1

    topic = reg.topic(TOPIC)
    received = 0
    hint = 0
    while received < MESSAGES:
        # receive_any scans partitions starting from `hint`, so a single
        # consumer can drain the whole topic without missing a partition.
        msg = topic.receive_any(timeout_ns=5_000_000_000, hint=hint)
        if msg is None:
            break
        hint = (msg.partition + 1) % topic.partitions
        received += 1
        with msg:          # releases the chunk back to the pool
            if received == 1:
                print(f"consumer: first message len={msg.len} "
                      f"key={msg.key} partition={msg.partition}")

    print(f"consumer: received {received}/{MESSAGES}")
    print("consumer: stats", {k: v for k, v in topic.stats().items() if v})
    reg.close()
    return 0 if received == MESSAGES else 1


def main():
    pid = os.fork()
    if pid == 0:
        # Child: run to completion, then exit with its own status so a short
        # read shows up as a non-zero exit code. os._exit skips stdio
        # flushing, hence the explicit flush.
        code = consumer()
        sys.stdout.flush()
        os._exit(code)

    producer()
    _, status = os.waitpid(pid, 0)
    return os.WEXITSTATUS(status)


if __name__ == "__main__":
    sys.exit(main())
