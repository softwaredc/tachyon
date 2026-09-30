#!/usr/bin/env python3
"""
The zero-copy path from Python: loan a buffer, fill it in shared memory,
publish it. No intermediate bytes object is ever created.

Worth it when you serialise into the buffer (struct.pack_into, json.dumps into
a bytearray you then copy, protobuf SerializeToString) instead of building a
string first. For tiny messages just use topic.send(); the memcpy is cheaper
than the Python-level bookkeeping.

Run:
    TACHYON_LIB=/path/to/libtachyon.so python3 zero_copy.py
"""
import os
import struct
import sys
import time

import tachyon

REGION = "py_zerocopy_demo"
TOPIC = "orders"

# order_id (Q), user_id (Q), price (d), side (4s)
RECORD = struct.Struct("<QQd4s")


def producer():
    tachyon.Region.remove(REGION)
    reg = tachyon.Region(REGION, create=True, size=32 << 20, spawn_reaper=False)
    topic = reg.topic(TOPIC, partitions=4, capacity=256, pool_bytes=8 << 20)

    for i in range(5):
        with topic.loan(RECORD.size) as buf:
            # Write directly into shared memory. Nothing is copied on publish.
            RECORD.pack_into(buf._buf, 0, 1000 + i, 9000 + i, 123.45 + i, b"BUY")
            topic.publish(buf, RECORD.size, key=9000 + i)
        print(f"producer: published order {1000 + i} "
              f"-> partition {topic.route(9000 + i)}")

    # A loan you change your mind about. abort() returns the chunk at once;
    # forgetting to abort means waiting a full lease before the reaper finds it.
    buf = topic.loan(4096)
    print("producer: borrowed", buf, "then aborted it")
    buf.abort()

    print("producer: pool", topic.pool_info()[:4])
    reg.close()


def consumer():
    # The forked child may run before the parent has created the region.
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

    seen = []
    for _ in range(5):
        msg = topic.receive_any(timeout_ns=5_000_000_000)
        if msg is None:
            print("consumer: nothing arrived", file=sys.stderr)
            reg.close()
            return 1
        with msg:
            # bytes(msg.data) copies; msg.data itself is a view over the region.
            order_id, user_id, price, side = RECORD.unpack_from(bytes(msg.data))
            seen.append(order_id)
            print(f"consumer: order={order_id} user={user_id} "
                  f"price={price:.2f} side={side.decode()} "
                  f"(partition={msg.partition})")

    # Note what is NOT checked here: arrival order. These five orders use five
    # different keys, so they land in different partitions and there is no
    # ordering guarantee between them. Order is only guaranteed per key -- see
    # examples/key_ordering.c for that invariant.
    ok = sorted(seen) == [1000, 1001, 1002, 1003, 1004]
    print("consumer:", "OK" if ok else "MISMATCH", sorted(seen))
    reg.close()
    tachyon.Region.remove(REGION)
    return 0 if ok else 1


def main():
    pid = os.fork()
    if pid == 0:
        # os._exit skips stdio flushing, so flush before exiting: without this
        # the child's output disappears whenever stdout is a pipe.
        code = consumer()
        sys.stdout.flush()
        os._exit(code)
    producer()
    _, status = os.waitpid(pid, 0)
    return os.WEXITSTATUS(status)


if __name__ == "__main__":
    sys.exit(main())
