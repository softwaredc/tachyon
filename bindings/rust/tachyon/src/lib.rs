//! Safe Rust interface to tachyon, a zero-copy IPC library.
//!
//! ```
//! use tachyon::{Region, RegionConf, TopicConf};
//!
//! let reg = Region::open("demo", RegionConf { size: 64 << 20, create: true, ..Default::default() })?;
//! let topic = reg.topic("orders", TopicConf { partitions: 8, ..Default::default() })?;
//! topic.send(b"hello", 42)?;
//!
//! let msg = topic.receive(topic.route(42), -1)?;
//! assert_eq!(&msg[..], b"hello");
//! drop(msg);            // returns the chunk to the pool
//! # Ok::<(), tachyon::Error>(())
//! ```
//!
//! # Safety model
//!
//! The payload of a received message lives in memory shared with other
//! processes. Rust cannot enforce aliasing rules across a process boundary, so
//! [`Message`] holds a raw slice and enforces release-on-drop instead. Treat
//! the bytes as borrowed: copy them if they must outlive the message.
//!
//! # Ordering
//!
//! Messages sharing a routing key land in the same partition and are delivered
//! in send order. Messages with different keys have **no** relative ordering
//! guarantee -- that is the price of consuming in parallel.

use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr;

pub use tachyon_sys as sys;

/// Errors returned by every fallible operation in this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Invalid,
    AlreadyExists,
    NotFound,
    OutOfMemory,
    Full,
    Empty,
    Timeout,
    TooBig,
    BadState,
    AbiMismatch,
    Io,
    MmapFailed,
    Busy,
    Internal,
    BadName,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Error::Invalid => "invalid argument",
            Error::AlreadyExists => "already exists",
            Error::NotFound => "no such region or topic",
            Error::OutOfMemory => "pool or region exhausted",
            Error::Full => "ring is full (backpressure)",
            Error::Empty => "no message available",
            Error::Timeout => "deadline expired",
            Error::TooBig => "payload exceeds 1 MiB",
            Error::BadState => "wrong state for this call",
            Error::AbiMismatch => "region built by an incompatible version",
            Error::Io => "operating system error",
            Error::MmapFailed => "mmap failed",
            Error::Busy => "contended, retry",
            Error::Internal => "internal invariant violated",
            Error::BadName => "region name empty or contains a slash",
        };
        f.write_str(s)
    }
}

impl std::error::Error for Error {}

impl From<sys::ty_err_t> for Error {
    fn from(e: sys::ty_err_t) -> Self {
        use sys::ty_err_t::*;
        match e {
            OK => unreachable!("OK is not an error"),
            ERR_INVAL => Error::Invalid,
            ERR_EXIST => Error::AlreadyExists,
            ERR_NOENT => Error::NotFound,
            ERR_NOMEM => Error::OutOfMemory,
            ERR_FULL => Error::Full,
            ERR_EMPTY => Error::Empty,
            ERR_TIMEOUT => Error::Timeout,
            ERR_TOOBIG => Error::TooBig,
            ERR_STATE => Error::BadState,
            ERR_ABI => Error::AbiMismatch,
            ERR_IO => Error::Io,
            ERR_MAP => Error::MmapFailed,
            ERR_BUSY => Error::Busy,
            ERR_INTERNAL => Error::Internal,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn check(e: sys::ty_err_t) -> Result<()> {
    match e {
        sys::ty_err_t::OK => Ok(()),
        other => Err(Error::from(other)),
    }
}

/// Wait policy for consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Notify {
    /// Busy-spin: lowest latency, burns a core.
    Spin,
    /// Always sleep: zero idle CPU, higher wake latency.
    Futex,
    /// Spin briefly, then sleep. Default and usually right.
    #[default]
    Hybrid,
}

impl From<Notify> for sys::ty_notify_mode_t {
    fn from(n: Notify) -> Self {
        match n {
            Notify::Spin => sys::ty_notify_mode_t::SPIN,
            Notify::Futex => sys::ty_notify_mode_t::FUTEX,
            Notify::Hybrid => sys::ty_notify_mode_t::HYBRID,
        }
    }
}

/// Configuration for opening a region.
#[derive(Debug, Clone)]
pub struct RegionConf {
    pub size: usize,
    pub create: bool,
    pub exclusive: bool,
    pub hugepage: bool,
    /// Spawn a background reaper thread. Leave off if your process forks -- a
    /// thread plus fork is a well known source of deadlocks in the child.
    pub spawn_reaper: bool,
    pub lease_ns: u64,
}

impl Default for RegionConf {
    fn default() -> Self {
        Self {
            size: 64 << 20,
            create: true,
            exclusive: false,
            hugepage: false,
            spawn_reaper: false,
            lease_ns: 0,
        }
    }
}

/// Configuration for creating a topic.
#[derive(Debug, Clone)]
pub struct TopicConf {
    pub partitions: u32,
    pub capacity: u32,
    pub pool_bytes: usize,
    pub notify: Notify,
}

impl Default for TopicConf {
    fn default() -> Self {
        Self {
            partitions: 8,
            capacity: 1024,
            pool_bytes: 16 << 20,
            notify: Notify::Hybrid,
        }
    }
}

/// A mapped shared region.
pub struct Region {
    handle: *mut sys::ty_region,
}

// Regions are process-local handles into memory shared on purpose; the library
// itself is thread safe.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// Open or create a region by name.
    pub fn open(name: &str, conf: RegionConf) -> Result<Self> {
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(Error::BadName);
        }
        let cname = CString::new(name).map_err(|_| Error::BadName)?;
        let mut handle: *mut sys::ty_region = ptr::null_mut();

        let mut c = sys::ty_region_conf_t {
            size: conf.size,
            create: conf.create as _,
            exclusive: conf.exclusive as _,
            hugepage: conf.hugepage as _,
            spawn_reaper: conf.spawn_reaper as _,
            lease_ns: conf.lease_ns,
            dir: ptr::null(),
        };

        unsafe { check(sys::ty_region_open(cname.as_ptr(), &mut c, &mut handle))? };
        Ok(Self { handle })
    }

    /// Open a topic, creating it if needed. Concurrent creation from several
    /// processes is safe: the losers attach to the winner's topic.
    pub fn topic(&self, name: &str, conf: TopicConf) -> Result<Topic> {
        let cname = CString::new(name).map_err(|_| Error::Invalid)?;
        let mut handle: *mut sys::ty_topic = ptr::null_mut();

        let c = sys::ty_topic_conf_t {
            partitions: conf.partitions,
            capacity: conf.capacity,
            pool_bytes: conf.pool_bytes,
            notify: conf.notify.into(),
            flags: 0,
        };

        unsafe {
            match sys::ty_topic_create(self.handle, cname.as_ptr(), &c, &mut handle) {
                sys::ty_err_t::OK => {}
                sys::ty_err_t::ERR_EXIST | sys::ty_err_t::ERR_NOENT => {
                    check(sys::ty_topic_open(self.handle, cname.as_ptr(), &mut handle))?;
                }
                other => return Err(Error::from(other)),
            }
        }
        Ok(Topic { handle })
    }

    /// Mapped size in bytes.
    pub fn size(&self) -> usize {
        unsafe { sys::ty_region_size(self.handle) }
    }

    /// Refresh this process's heartbeat. Only needed if you block for long
    /// stretches outside tachyon.
    pub fn heartbeat(&self) {
        unsafe { sys::ty_heartbeat(self.handle) }
    }

    /// Detach. Does **not** delete the region -- a producer must be able to
    /// publish and exit while its messages remain readable.
    pub fn close(self) -> Result<()> {
        unsafe { check(sys::ty_region_close(self.handle)) }
    }

    /// Delete a region by name. Idempotent: deleting what is already gone is
    /// not an error.
    pub fn remove(name: &str) -> Result<()> {
        let cname = CString::new(name).map_err(|_| Error::BadName)?;
        match unsafe { sys::ty_region_remove(cname.as_ptr(), ptr::null()) } {
            sys::ty_err_t::OK | sys::ty_err_t::ERR_NOENT => Ok(()),
            other => Err(Error::from(other)),
        }
    }
}

/// A named channel inside a region.
pub struct Topic {
    handle: *mut sys::ty_topic,
}

unsafe impl Send for Topic {}
unsafe impl Sync for Topic {}

impl Topic {
    /// Which partition a key maps to.
    pub fn route(&self, key: u64) -> u32 {
        unsafe { sys::ty_route(self.handle, key) }
    }

    pub fn partitions(&self) -> u32 {
        unsafe { sys::ty_topic_partitions(self.handle) }
    }

    pub fn capacity(&self) -> u32 {
        unsafe { sys::ty_topic_capacity(self.handle) }
    }

    /// Publish a payload under a routing key. One memcpy.
    pub fn send(&self, payload: &[u8], key: u64) -> Result<()> {
        if payload.len() > sys::TACHYON_MAX_MSG_SIZE {
            return Err(Error::TooBig);
        }
        unsafe {
            check(sys::ty_send(
                self.handle,
                payload.as_ptr() as *const c_void,
                payload.len(),
                key,
            ))
        }
    }

    /// Borrow a writable buffer in shared memory -- the true zero-copy path.
    pub fn loan(&self, size: usize) -> Result<Loan<'_>> {
        let mut ptr: *mut c_void = ptr::null_mut();
        let mut cap: usize = 0;
        unsafe { check(sys::ty_loan(self.handle, size, &mut ptr, &mut cap))? };
        Ok(Loan {
            topic: self,
            ptr,
            cap,
            done: false,
        })
    }

    /// Receive one message.
    ///
    /// `timeout_ns < 0` blocks forever, `0` polls once, `> 0` waits that long.
    pub fn receive(&self, partition: u32, timeout_ns: i64) -> Result<Message<'_>> {
        let mut raw = sys::ty_msg_t {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
            key: 0,
            seq: 0,
            partition: 0,
            _pad: 0,
        };
        unsafe { check(sys::ty_subscribe(self.handle, partition, &mut raw, timeout_ns))? };
        Ok(Message::new(self, raw))
    }

    /// Receive from whichever partition has a message waiting.
    pub fn receive_any(&self, timeout_ns: i64) -> Result<Message<'_>> {
        let mut raw = sys::ty_msg_t {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
            key: 0,
            seq: 0,
            partition: 0,
            _pad: 0,
        };
        let mut hint: u32 = 0;
        unsafe { check(sys::ty_subscribe_any(self.handle, &mut hint, &mut raw, timeout_ns))? };
        Ok(Message::new(self, raw))
    }

    /// Pending messages on a partition, or across all of them with -1.
    pub fn depth(&self, partition: i32) -> Result<u64> {
        let mut out: u64 = 0;
        unsafe { check(sys::ty_depth(self.handle, partition, &mut out))? };
        Ok(out)
    }

    /// Force one lease sweep; returns chunks recovered.
    pub fn reclaim(&self) -> u64 {
        unsafe { sys::ty_reclaim_once(self.handle) }
    }

    pub fn stats(&self) -> Result<Stats> {
        let mut s = unsafe { std::mem::zeroed::<sys::ty_stats_t>() };
        unsafe { check(sys::ty_topic_stats(self.handle, &mut s))? };
        Ok(Stats {
            published: s.published,
            consumed: s.consumed,
            dropped: s.dropped,
            loan_ok: s.loan_ok,
            loan_nomem: s.loan_nomem,
            reclaimed: s.reclaimed,
            wait_wakeups: s.wait_wakeups,
            spin_spins: s.spin_spins,
        })
    }
}

impl Drop for Topic {
    fn drop(&mut self) {
        unsafe { sys::ty_topic_close(self.handle) };
    }
}

/// Counter snapshot.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub published: u64,
    pub consumed: u64,
    pub dropped: u64,
    pub loan_ok: u64,
    pub loan_nomem: u64,
    pub reclaimed: u64,
    pub wait_wakeups: u64,
    pub spin_spins: u64,
}

impl Stats {
    /// Non-zero in steady state means processes are being killed in your system.
    pub fn reclaimed(&self) -> u64 {
        self.reclaimed
    }
    /// Climbing means `pool_bytes` is undersized.
    pub fn loan_nomem(&self) -> u64 {
        self.loan_nomem
    }
}

/// A borrowed, not-yet-published buffer in shared memory.
pub struct Loan<'a> {
    topic: &'a Topic,
    ptr: *mut c_void,
    cap: usize,
    done: bool,
}

impl<'a> Loan<'a> {
    /// Writable view. Writing here writes the message: there is no copy.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.cap) }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Commit this buffer to `key`'s partition.
    pub fn publish(mut self, len: usize, key: u64) -> Result<()> {
        let rc = unsafe { sys::ty_publish(self.topic.handle, self.ptr, len, key) };
        self.done = true;
        check(rc)
    }
}

impl<'a> Drop for Loan<'a> {
    fn drop(&mut self) {
        // publish() consumed the loan; anything else abandons it, which must
        // return the chunk rather than let it sit out for a whole lease.
        if !self.done {
            unsafe { sys::ty_abort(self.topic.handle, self.ptr) };
        }
    }
}

/// A received message. The bytes live in shared memory and are valid until
/// this value is dropped.
pub struct Message<'a> {
    topic: &'a Topic,
    raw: sys::ty_msg_t,
}

impl<'a> Message<'a> {
    fn new(topic: &'a Topic, raw: sys::ty_msg_t) -> Self {
        Self { topic, raw }
    }

    pub fn key(&self) -> u64 {
        self.raw.key
    }
    pub fn seq(&self) -> u64 {
        self.raw.seq
    }
    pub fn partition(&self) -> u32 {
        self.raw.partition
    }
    pub fn capacity(&self) -> usize {
        self.raw.capacity as usize
    }

    /// Explicitly return the chunk. Called automatically on drop as well.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.raw.data.is_null() {
            unsafe { sys::ty_release(self.topic.handle, &mut self.raw) };
            self.raw.data = ptr::null_mut();
        }
    }
}

impl<'a> std::ops::Deref for Message<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        if self.raw.data.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.raw.data as *const u8, self.raw.len as usize) }
    }
}

impl<'a> Drop for Message<'a> {
    fn drop(&mut self) {
        self.release_inner();
    }
}

/// Monotonic timestamp in nanoseconds.
pub fn now_ns() -> u64 {
    unsafe { sys::ty_now_ns() }
}

/// Name of the C library version this binding was built against.
pub fn version() -> &'static str {
    unsafe {
        CStr::from_ptr(sys::ty_strerror(sys::ty_err_t::OK))
            .to_str()
            .unwrap_or("ok")
    }
}
