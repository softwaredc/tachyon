"""
tachyon -- zero-copy inter-process communication for Python.

This binding uses ctypes rather than a compiled extension module, so
`pip install tachyon` works on any machine with libtachyon.so present and needs
no compiler, no headers, and no build step. The cost is one ffi call per
operation, a few microseconds, which is noise next to the message handling you
would do anyway. The payload itself is never copied.

Quick start::

    import tachyon

    prod = tachyon.Region("demo", create=True, size=64 << 20)
    topic = prod.topic("orders", partitions=8, capacity=1024)
    topic.send(b"hello", key=42)

    # in another process
    cons = tachyon.Region("demo", create=False)
    topic = cons.topic("orders")
    msg = topic.receive(partition=tachyon.route_for(8, 42))
    print(bytes(msg.data))
    topic.release(msg)

Key idea: `msg.data` is a memoryview straight into the shared region. There is
no copy between processes; the bytes you read are the bytes the producer wrote.
"""

from __future__ import annotations

import atexit
import ctypes
import os
import sys
from ctypes import (POINTER, byref, c_char_p, c_int, c_int64, c_size_t,
                    c_uint, c_uint32, c_uint64, c_void_p, cast)
from typing import Optional

__version__ = "1.0.0"

# Destructors that call into a shared library are a classic way to segfault at
# interpreter shutdown, when the library may already be torn down. Once atexit
# has fired, our __del__ implementations become no-ops; callers who care about
# releasing promptly should do it explicitly, as they should anyway.
_SHUTTING_DOWN = False


def _on_exit() -> None:
    global _SHUTTING_DOWN
    _SHUTTING_DOWN = True


atexit.register(_on_exit)

# --------------------------------------------------------------------- library

def _find_library() -> str:
    override = os.environ.get("TACHYON_LIB")
    if override:
        return override
    here = os.path.dirname(os.path.abspath(__file__))
    for cand in (
        os.path.join(here, "_native", "libtachyon.so"),
        os.path.join(here, "..", "..", "..", "build", "libtachyon.so"),
        os.path.join(here, "..", "..", "..", "build", "src", "libtachyon.so"),
        "/usr/local/lib/libtachyon.so",
        "/usr/lib/libtachyon.so",
        "libtachyon.so",
    ):
        if os.path.exists(cand):
            return cand
    return "libtachyon.so"


_lib = ctypes.CDLL(_find_library())

# ---------------------------------------------------------------------- errors

OK = 0
ERR_INVAL = -1
ERR_EXIST = -2
ERR_NOENT = -3
ERR_NOMEM = -4
ERR_FULL = -5
ERR_EMPTY = -6
ERR_TIMEOUT = -7
ERR_TOOBIG = -8
ERR_STATE = -9
ERR_ABI = -10
ERR_IO = -11
ERR_MAP = -12
ERR_BUSY = -13
ERR_INTERNAL = -99


class TachyonError(Exception):
    """Raised for any non-OK status returned by the native library."""

    def __init__(self, code: int, message: str = ""):
        self.code = code
        text = _lib.ty_strerror(code)
        try:
            text = text.decode("utf-8") if isinstance(text, bytes) else str(text)
        except Exception:
            text = "error"
        super().__init__(f"[{code}] {text}{(': ' + message) if message else ''}")


_lib.ty_strerror.restype = c_char_p
_lib.ty_strerror.argtypes = [c_int]


def _check(code: int, message: str = "") -> int:
    if code != OK:
        raise TachyonError(code, message)
    return code


# -------------------------------------------------------------------- structs

class _RegionConf(ctypes.Structure):
    _fields_ = [
        ("size", c_size_t),
        ("create", c_int),
        ("exclusive", c_int),
        ("hugepage", c_int),
        ("spawn_reaper", c_int),
        ("lease_ns", c_uint64),
        ("dir", c_char_p),
    ]


class _TopicConf(ctypes.Structure):
    _fields_ = [
        ("partitions", c_uint32),
        ("capacity", c_uint32),
        ("pool_bytes", c_size_t),
        ("notify", c_int),
        ("flags", c_uint32),
    ]


class _Msg(ctypes.Structure):
    _fields_ = [
        ("data", c_void_p),
        ("len", c_uint32),
        ("capacity", c_uint32),
        ("key", c_uint64),
        ("seq", c_uint64),
        ("partition", c_uint32),
        ("_pad", c_uint32),
    ]


class _Stats(ctypes.Structure):
    _fields_ = [
        ("published", c_uint64),
        ("consumed", c_uint64),
        ("dropped", c_uint64),
        ("loan_ok", c_uint64),
        ("loan_nomem", c_uint64),
        ("reclaimed", c_uint64),
        ("wait_wakeups", c_uint64),
        ("spin_spins", c_uint64),
        ("_rsv", c_uint64 * 3),
    ]


class _PoolClassInfo(ctypes.Structure):
    _fields_ = [
        ("chunk_size", c_uint64),
        ("chunk_count", c_uint64),
        ("free_count", c_uint64),
    ]


# ---------------------------------------------------------------------- protos

_lib.ty_region_open.restype = c_int
_lib.ty_region_open.argtypes = [c_char_p, POINTER(_RegionConf),
                                POINTER(c_void_p)]
_lib.ty_region_close.restype = c_int
_lib.ty_region_close.argtypes = [c_void_p]
_lib.ty_region_remove.restype = c_int
_lib.ty_region_remove.argtypes = [c_char_p, c_char_p]
_lib.ty_region_size.restype = c_size_t
_lib.ty_region_size.argtypes = [c_void_p]

_lib.ty_topic_create.restype = c_int
_lib.ty_topic_create.argtypes = [c_void_p, c_char_p, POINTER(_TopicConf),
                                 POINTER(c_void_p)]
_lib.ty_topic_open.restype = c_int
_lib.ty_topic_open.argtypes = [c_void_p, c_char_p, POINTER(c_void_p)]
_lib.ty_topic_close.restype = c_int
_lib.ty_topic_close.argtypes = [c_void_p]
_lib.ty_topic_partitions.restype = c_uint32
_lib.ty_topic_partitions.argtypes = [c_void_p]
_lib.ty_topic_capacity.restype = c_uint32
_lib.ty_topic_capacity.argtypes = [c_void_p]

_lib.ty_loan.restype = c_int
_lib.ty_loan.argtypes = [c_void_p, c_size_t, POINTER(c_void_p),
                         POINTER(c_size_t)]
_lib.ty_abort.restype = c_int
_lib.ty_abort.argtypes = [c_void_p, c_void_p]
_lib.ty_publish.restype = c_int
_lib.ty_publish.argtypes = [c_void_p, c_void_p, c_size_t, c_uint64]
_lib.ty_send.restype = c_int
_lib.ty_send.argtypes = [c_void_p, c_void_p, c_size_t, c_uint64]

_lib.ty_subscribe.restype = c_int
_lib.ty_subscribe.argtypes = [c_void_p, c_uint32, POINTER(_Msg), c_int64]
_lib.ty_subscribe_any.restype = c_int
_lib.ty_subscribe_any.argtypes = [c_void_p, POINTER(c_uint32), POINTER(_Msg),
                                  c_int64]
_lib.ty_release.restype = c_int
_lib.ty_release.argtypes = [c_void_p, POINTER(_Msg)]

_lib.ty_route.restype = c_uint32
_lib.ty_route.argtypes = [c_void_p, c_uint64]
_lib.ty_depth.restype = c_int
_lib.ty_depth.argtypes = [c_void_p, c_int, POINTER(c_uint64)]
_lib.ty_topic_stats.restype = c_int
_lib.ty_topic_stats.argtypes = [c_void_p, POINTER(_Stats)]
_lib.ty_pool_info.restype = c_int
_lib.ty_pool_info.argtypes = [c_void_p, POINTER(_PoolClassInfo), c_size_t]
_lib.ty_reclaim_once.restype = c_uint64
_lib.ty_reclaim_once.argtypes = [c_void_p]
_lib.ty_heartbeat.argtypes = [c_void_p]

_lib.ty_now_ns.restype = c_uint64
_lib.ty_cpu_count.restype = c_int

MAX_MSG_SIZE = 1024 * 1024

SPIN, FUTEX, HYBRID = 0, 1, 2


# --------------------------------------------------------------------- helpers

def route_for(partitions: int, key: int) -> int:
    """Which partition a key maps to, without opening a topic.

    Useful when you want to dedicate a worker to a fixed set of keys and need
    to know its partition id up front.
    """
    # mirrors ty_mix64 + mask in the C implementation
    x = key & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    x = (x * 0xC4CEB9FE1A85EC53) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    return x & (partitions - 1)


def now_ns() -> int:
    return int(_lib.ty_now_ns())


def cpu_count() -> int:
    return int(_lib.ty_cpu_count())


# ---------------------------------------------------------------------- message

class Message:
    """
    A received message.

    `data` is a memoryview over shared memory, not a copy. Treat it as
    borrowed: once you call release() (or the garbage collector does it for
    you), reading it is undefined behaviour. If you need the bytes to outlive
    the message, call `bytes(msg.data)`.
    """

    __slots__ = ("_topic", "_raw", "_released", "data", "len", "capacity",
                 "key", "seq", "partition")

    def __init__(self, topic: "Topic", raw: _Msg):
        self._topic = topic
        self._raw = raw
        self._released = False
        self.data = _view_of(raw.data, raw.len) if raw.data else memoryview(b"")
        self.len = int(raw.len)
        self.capacity = int(raw.capacity)
        self.key = int(raw.key)
        self.seq = int(raw.seq)
        self.partition = int(raw.partition)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        _check(_lib.ty_release(self._topic._handle, byref(self._raw)),
               "ty_release")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()
        return False

    def __del__(self):
        # Best effort: never raise from a destructor, but do not silently leak
        # a chunk either -- a leaked chunk is invisible pool erosion.
        if _SHUTTING_DOWN:
            return
        try:
            if not self._released:
                self.release()
        except Exception:
            pass

    def __len__(self):
        return self.len

    def __bytes__(self):
        return bytes(self.data)

    def __repr__(self):
        return (f"<Message partition={self.partition} len={self.len} "
                f"key={self.key} seq={self.seq}>")


def _view_of(addr: int, length: int) -> memoryview:
    """Build a memoryview over a raw address. This is the zero-copy bit.

    c_ubyte (format 'B') rather than c_char (format 'c'): only 'B' supports
    slice assignment, and a loaned buffer that cannot be written to is useless.
    """
    if not addr or length == 0:
        return memoryview(b"")
    buf = (ctypes.c_ubyte * length).from_address(int(addr))
    return memoryview(buf)


class Loan:
    """
    A buffer borrowed from the shared pool, not yet published.

    Write into `.view` and then call publish(). Because the buffer lives in the
    shared region, whatever you write here is what the consumer reads -- no
    intermediate copy exists anywhere. If you decide not to publish, call
    abort() so the chunk goes straight back instead of waiting for the lease.
    """

    __slots__ = ("_topic", "_addr", "_buf", "capacity", "view", "_done")

    def __init__(self, topic: "Topic", addr: int, capacity: int):
        self._topic = topic
        self._addr = addr
        # Keep the ctypes array alive alongside the view: ctypes supports slice
        # assignment, memoryview does not for these formats.
        self._buf = (ctypes.c_ubyte * capacity).from_address(addr) if addr else None
        self.capacity = capacity
        self.view = memoryview(self._buf) if self._buf is not None else memoryview(b"")
        self._done = False

    def write(self, data: bytes, offset: int = 0) -> int:
        """Copy bytes into the loaned buffer. Returns bytes written."""
        n = min(len(data), self.capacity - offset)
        if n > 0:
            self._buf[offset:offset + n] = bytes(data[:n])
        return n

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        _check(_lib.ty_abort(self._topic._handle, c_void_p(self._addr)),
               "ty_abort")

    def __len__(self):
        return self.capacity

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # Abandon on exception; on clean exit publish() has already run.
        if not self._done:
            self.abort()
        return False

    def __repr__(self):
        return f"<Loan capacity={self.capacity} addr=0x{self._addr:x}>"


# ------------------------------------------------------------------------ topic

class Topic:
    """A named channel inside a region."""

    def __init__(self, region: "Region", handle: int):
        self._region = region
        self._handle = handle
        self._closed = False

    @property
    def partitions(self) -> int:
        return int(_lib.ty_topic_partitions(self._handle))

    @property
    def capacity(self) -> int:
        return int(_lib.ty_topic_capacity(self._handle))

    def route(self, key: int) -> int:
        return int(_lib.ty_route(self._handle, c_uint64(key)))

    # -- producing ----------------------------------------------------------

    def send(self, payload: bytes, key: int = 0) -> None:
        """Publish a bytes-like object under a routing key.

        Same key always lands in the same partition, so messages sharing a key
        are delivered in the order they were sent.
        """
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        if len(payload) > MAX_MSG_SIZE:
            raise TachyonError(ERR_TOOBIG, "payload exceeds 1 MiB")
        buf = ctypes.create_string_buffer(bytes(payload), len(payload))
        _check(_lib.ty_send(self._handle, buf, len(payload), c_uint64(key)),
               "ty_send")

    def loan(self, size: int) -> Loan:
        """Borrow a writable buffer in shared memory.

        Build your message directly here and hand it to publish(); this is the
        true zero-copy path, with no memcpy on the producer side at all.
        """
        ptr = c_void_p()
        cap = c_size_t()
        _check(_lib.ty_loan(self._handle, c_size_t(size), byref(ptr),
                            byref(cap)), "ty_loan")
        return Loan(self, int(ptr.value or 0), int(cap.value))

    def publish(self, loan: Loan, length: int, key: int = 0) -> None:
        """Publish a previously loaned buffer to key's partition."""
        if not isinstance(loan, Loan):
            raise TypeError("publish() expects the Loan returned by loan()")
        _check(_lib.ty_publish(self._handle, c_void_p(loan._addr),
                               c_size_t(length), c_uint64(key)), "ty_publish")
        loan._done = True

    # -- consuming -----------------------------------------------------------

    def receive(self, partition: int = 0, timeout_ns: int = -1) -> Optional[Message]:
        """Receive one message.

        timeout_ns < 0 blocks forever, 0 polls once, > 0 waits that long.
        Returns None when the deadline expires with nothing arriving.
        """
        raw = _Msg()
        rc = _lib.ty_subscribe(self._handle, c_uint32(partition), byref(raw),
                               c_int64(timeout_ns))
        if rc == ERR_EMPTY or rc == ERR_TIMEOUT:
            return None
        _check(rc, "ty_subscribe")
        return Message(self, raw)

    def receive_any(self, timeout_ns: int = -1, hint: Optional[int] = None
                    ) -> Optional[Message]:
        """Receive from whichever partition has a message waiting."""
        h = c_uint32(hint or 0)
        raw = _Msg()
        rc = _lib.ty_subscribe_any(self._handle, byref(h), byref(raw),
                                   c_int64(timeout_ns))
        if rc == ERR_EMPTY or rc == ERR_TIMEOUT:
            return None
        _check(rc, "ty_subscribe_any")
        return Message(self, raw)

    # -- introspection -------------------------------------------------------

    def depth(self, partition: int = -1) -> int:
        out = c_uint64()
        _check(_lib.ty_depth(self._handle, c_int(partition), byref(out)),
               "ty_depth")
        return int(out.value)

    def stats(self) -> dict:
        s = _Stats()
        _check(_lib.ty_topic_stats(self._handle, byref(s)), "ty_topic_stats")
        return {k: int(getattr(s, k)) for k, _ in _Stats._fields_
                if not k.startswith("_")}

    def pool_info(self) -> list:
        info = (_PoolClassInfo * 8)()
        _check(_lib.ty_pool_info(self._handle, info, 8), "ty_pool_info")
        return [{"chunk_size": int(i.chunk_size),
                 "chunk_count": int(i.chunk_count),
                 "free_count": int(i.free_count)} for i in info]

    def reclaim(self) -> int:
        """Force one lease sweep; returns how many chunks were recovered."""
        return int(_lib.ty_reclaim_once(self._handle))

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _lib.ty_topic_close(self._handle)

    def __del__(self):
        if _SHUTTING_DOWN:
            return
        try:
            self.close()
        except Exception:
            pass


# ----------------------------------------------------------------------- region

class Region:
    """A shared memory region holding one or more topics."""

    def __init__(self, name: str, create: bool = True,
                 size: int = 64 << 20, exclusive: bool = False,
                 hugepage: bool = False, spawn_reaper: bool = False,
                 lease_ns: int = 0, dir: Optional[str] = None):
        """
        spawn_reaper defaults to False here, unlike the C defaults.

        The reaper is a background thread, and Python code forks freely; mixing
        threads with fork() is a well known source of deadlocks in the child
        (CPython warns about it explicitly). Python users should either run the
        external `tachyond` daemon or call Topic.reclaim() from a dedicated
        process, both of which keep the child process single-threaded.
        """
        if not name or "/" in name or "\\" in name:
            raise ValueError("region name must be non-empty and contain no slashes")
        conf = _RegionConf()
        conf.size = size
        conf.create = 1 if create else 0
        conf.exclusive = 1 if exclusive else 0
        conf.hugepage = 1 if hugepage else 0
        conf.spawn_reaper = 1 if spawn_reaper else 0
        conf.lease_ns = lease_ns
        conf.dir = dir.encode() if dir else None

        handle = c_void_p()
        rc = _lib.ty_region_open(name.encode(), byref(conf), byref(handle))
        if rc == ERR_NOENT and not create:
            raise TachyonError(rc, f"no such region: {name}")
        _check(rc, "ty_region_open")
        self._handle = handle
        self._name = name
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def size(self) -> int:
        return int(_lib.ty_region_size(self._handle))

    def topic(self, name: str, partitions: int = 8, capacity: int = 1024,
              pool_bytes: int = 16 << 20, notify: int = HYBRID) -> Topic:
        """Open a topic, creating it if it does not exist yet.

        Concurrent creation from several processes is safe: the losers simply
        attach to whatever the winner created.
        """
        conf = _TopicConf()
        conf.partitions = partitions
        conf.capacity = capacity
        conf.pool_bytes = pool_bytes
        conf.notify = notify
        conf.flags = 0

        handle = c_void_p()
        rc = _lib.ty_topic_create(self._handle, name.encode(), byref(conf),
                                  byref(handle))
        if rc == ERR_EXIST or rc == ERR_NOENT:
            _check(_lib.ty_topic_open(self._handle, name.encode(),
                                      byref(handle)), "ty_topic_open")
        else:
            _check(rc, "ty_topic_create")
        return Topic(self, handle)

    def heartbeat(self) -> None:
        """Refresh this process's liveness timestamp.

        Only needed if your application blocks for long stretches outside
        tachyon calls, where the reaper might otherwise assume you died.
        """
        _lib.ty_heartbeat(self._handle)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _lib.ty_region_close(self._handle)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __del__(self):
        if _SHUTTING_DOWN:
            return
        try:
            self.close()
        except Exception:
            pass

    @staticmethod
    def remove(name: str, dir: Optional[str] = None) -> bool:
        """Delete a region by name.

        Idempotent on purpose: setup code almost always wants "make sure this
        is gone", and raising because it already was gone forces every caller
        to wrap it in try/except for no benefit.
        """
        rc = _lib.ty_region_remove(name.encode(),
                                   dir.encode() if dir else None)
        if rc == ERR_NOENT:
            return False
        _check(rc, "ty_region_remove")
        return True


__all__ = ["Region", "Topic", "Message", "Loan", "TachyonError", "route_for",
           "now_ns", "cpu_count", "MAX_MSG_SIZE", "SPIN", "FUTEX", "HYBRID",
           "__version__"]
