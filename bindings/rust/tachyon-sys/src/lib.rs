//! Raw FFI bindings to libtachyon.
//!
//! This crate is deliberately unsafe and unopinionated: it exposes the C API
//! as-is. Everything here is `unsafe` because every call reaches into memory
//! shared with other processes, where the usual Rust guarantees do not hold --
//! another process can write to a chunk you are holding.
//!
//! For a safe interface use the `tachyon` crate, which wraps this one.

#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]

use std::os::raw::{c_char, c_int, c_void};

pub const TACHYON_VERSION_MAJOR: c_int = 1;
pub const TACHYON_VERSION_MINOR: c_int = 0;
pub const TACHYON_VERSION_PATCH: c_int = 0;
pub const TACHYON_ABI_VERSION: c_int = 1;

pub const TACHYON_MAX_NAME: usize = 63;
pub const TACHYON_MAX_TOPICS: usize = 64;
pub const TACHYON_MAX_PROCS: usize = 128;
pub const TACHYON_SIZE_CLASSES: usize = 8;
pub const TACHYON_MAX_MSG_SIZE: usize = 1024 * 1024;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ty_err_t {
    OK = 0,
    ERR_INVAL = -1,
    ERR_EXIST = -2,
    ERR_NOENT = -3,
    ERR_NOMEM = -4,
    ERR_FULL = -5,
    ERR_EMPTY = -6,
    ERR_TIMEOUT = -7,
    ERR_TOOBIG = -8,
    ERR_STATE = -9,
    ERR_ABI = -10,
    ERR_IO = -11,
    ERR_MAP = -12,
    ERR_BUSY = -13,
    ERR_INTERNAL = -99,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ty_notify_mode_t {
    SPIN = 0,
    FUTEX = 1,
    HYBRID = 2,
}

pub enum ty_region {}
pub enum ty_topic {}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ty_region_conf_t {
    pub size: usize,
    pub create: c_int,
    pub exclusive: c_int,
    pub hugepage: c_int,
    pub spawn_reaper: c_int,
    pub lease_ns: u64,
    pub dir: *const c_char,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ty_topic_conf_t {
    pub partitions: u32,
    pub capacity: u32,
    pub pool_bytes: usize,
    pub notify: ty_notify_mode_t,
    pub flags: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ty_msg_t {
    pub data: *mut c_void,
    pub len: u32,
    pub capacity: u32,
    pub key: u64,
    pub seq: u64,
    pub partition: u32,
    pub _pad: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ty_stats_t {
    pub published: u64,
    pub consumed: u64,
    pub dropped: u64,
    pub loan_ok: u64,
    pub loan_nomem: u64,
    pub reclaimed: u64,
    pub wait_wakeups: u64,
    pub spin_spins: u64,
    pub _rsv: [u64; 3],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ty_pool_class_info_t {
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub free_count: u64,
}

extern "C" {
    pub fn ty_strerror(err: ty_err_t) -> *const c_char;

    pub fn ty_region_open(
        name: *const c_char,
        conf: *const ty_region_conf_t,
        out: *mut *mut ty_region,
    ) -> ty_err_t;
    pub fn ty_region_close(reg: *mut ty_region) -> ty_err_t;
    pub fn ty_region_remove(name: *const c_char, dir: *const c_char) -> ty_err_t;
    pub fn ty_region_base(reg: *mut ty_region) -> *mut c_void;
    pub fn ty_region_size(reg: *mut ty_region) -> usize;
    pub fn ty_region_name(reg: *mut ty_region) -> *const c_char;

    pub fn ty_topic_create(
        reg: *mut ty_region,
        name: *const c_char,
        conf: *const ty_topic_conf_t,
        out: *mut *mut ty_topic,
    ) -> ty_err_t;
    pub fn ty_topic_open(
        reg: *mut ty_region,
        name: *const c_char,
        out: *mut *mut ty_topic,
    ) -> ty_err_t;
    pub fn ty_topic_close(topic: *mut ty_topic) -> ty_err_t;
    pub fn ty_topic_name(topic: *const ty_topic) -> *const c_char;
    pub fn ty_topic_partitions(topic: *const ty_topic) -> u32;
    pub fn ty_topic_capacity(topic: *const ty_topic) -> u32;

    pub fn ty_loan(
        topic: *mut ty_topic,
        size: usize,
        ptr: *mut *mut c_void,
        cap: *mut usize,
    ) -> ty_err_t;
    pub fn ty_abort(topic: *mut ty_topic, ptr: *mut c_void) -> ty_err_t;
    pub fn ty_publish(
        topic: *mut ty_topic,
        ptr: *mut c_void,
        len: usize,
        key: u64,
    ) -> ty_err_t;
    pub fn ty_send(
        topic: *mut ty_topic,
        src: *const c_void,
        len: usize,
        key: u64,
    ) -> ty_err_t;

    pub fn ty_subscribe(
        topic: *mut ty_topic,
        partition: u32,
        msg: *mut ty_msg_t,
        timeout_ns: i64,
    ) -> ty_err_t;
    pub fn ty_subscribe_any(
        topic: *mut ty_topic,
        hint: *mut u32,
        msg: *mut ty_msg_t,
        timeout_ns: i64,
    ) -> ty_err_t;
    pub fn ty_release(topic: *mut ty_topic, msg: *mut ty_msg_t) -> ty_err_t;

    pub fn ty_wait_room(topic: *mut ty_topic, key: u64, timeout_ns: i64) -> ty_err_t;
    pub fn ty_depth(topic: *mut ty_topic, partition: i32, out: *mut u64) -> ty_err_t;

    pub fn ty_topic_stats(topic: *mut ty_topic, out: *mut ty_stats_t) -> ty_err_t;
    pub fn ty_pool_info(
        topic: *mut ty_topic,
        out: *mut ty_pool_class_info_t,
        n: usize,
    ) -> ty_err_t;

    pub fn ty_reclaim_once(topic: *mut ty_topic) -> u64;
    pub fn ty_heartbeat(reg: *mut ty_region);

    pub fn ty_route(topic: *const ty_topic, key: u64) -> u32;
    pub fn ty_size_class(index: usize) -> usize;
    pub fn ty_size_class_count() -> usize;

    pub fn ty_now_ns() -> u64;
    pub fn ty_now_ns_fast() -> u64;
    pub fn ty_tsc_usable() -> c_int;
    pub fn ty_cpu_count() -> c_int;
}
