/*
 * tachyon.h -- zero-copy inter-process communication over shared memory.
 *
 * Copyright 2026 the tachyon authors. Licensed under the Apache License,
 * Version 2.0. See LICENSE in the repository root.
 *
 * Design summary
 * --------------
 * A `region` is a POSIX shared memory object holding one or more `topic`s.
 * A topic exposes N `partition`s (must be a power of two); every message is
 * routed as `partition = hash(key) & (N-1)`, which guarantees strict FIFO
 * ordering for messages sharing the same key while letting different keys be
 * consumed in parallel.
 *
 * Payload never travels through the kernel. Producers `loan` a chunk out of a
 * size-classed pool living inside the region, fill it in place, then `publish`
 * it, which only pushes a 16-byte descriptor into the target ring. Consumers
 * `subscribe` to a partition, read the payload in place, and `release` the
 * chunk back to the pool.
 *
 * Everything living in the shared region is addressed by offset, never by
 * pointer, so the region can be mapped at different base addresses in every
 * process.
 */
#ifndef TACHYON_H
#define TACHYON_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ version */

#define TACHYON_VERSION_MAJOR 1
#define TACHYON_VERSION_MINOR 0
#define TACHYON_VERSION_PATCH 0
#define TACHYON_VERSION_STRING "1.0.0"

/* ABI layout revision. Bumped whenever the shared-memory layout changes. */
#define TACHYON_ABI_VERSION 1

/* ------------------------------------------------------------------- limits */

#define TACHYON_MAX_NAME      63   /* max region/topic name length (bytes)   */
#define TACHYON_MAX_TOPICS    64   /* topics per region                      */
#define TACHYON_MAX_PROCS     128  /* concurrent attached processes          */
#define TACHYON_SIZE_CLASSES  8    /* number of payload size classes         */
#define TACHYON_CACHELINE     64

/** Largest payload that can be loaned (the biggest size class). */
#define TACHYON_MAX_MSG_SIZE (1024u * 1024u)

/* ------------------------------------------------------------------ errors */

typedef enum {
    TY_OK          =   0,  /**< success                                      */
    TY_ERR_INVAL   =  -1,  /**< invalid argument                             */
    TY_ERR_EXIST   =  -2,  /**< region/topic already exists                  */
    TY_ERR_NOENT   =  -3,  /**< no such region/topic                         */
    TY_ERR_NOMEM   =  -4,  /**< not enough room in the region                */
    TY_ERR_FULL    =  -5,  /**< target ring is full                          */
    TY_ERR_EMPTY   =  -6,  /**< no message available before the deadline     */
    TY_ERR_TIMEOUT =  -7,  /**< blocked until the deadline, nothing arrived   */
    TY_ERR_TOOBIG  =  -8,  /**< payload larger than TACHYON_MAX_MSG_SIZE     */
    TY_ERR_STATE   =  -9,  /**< object is in the wrong state for this call   */
    TY_ERR_ABI     = -10,  /**< region was created by an incompatible lib    */
    TY_ERR_IO      = -11,  /**< OS-level failure (see errno)                 */
    TY_ERR_MAP     = -12,  /**< mmap() failure                               */
    TY_ERR_BUSY    = -13,  /**< another process holds an exclusive lock      */
    TY_ERR_INTERNAL= -99   /**< invariant violation, please file a bug       */
} ty_err_t;

/** Turn an error code into a short human-readable string. */
const char* ty_strerror(ty_err_t err);

/* ------------------------------------------------------------ notify policy */

/**
 * How consumers wait when a partition is empty. This is the classic
 * latency-vs-cpu trade-off; switched per topic at creation time.
 */
typedef enum {
    /** Busy-spin. Lowest latency (~100ns), burns a full core. */
    TY_NOTIFY_SPIN   = 0,
    /** Linux futex. Sleeps with zero CPU cost, wake-up ~2-5us. Default. */
    TY_NOTIFY_FUTEX  = 1,
    /** Spin for a bounded while, then fall back to futex. Best of both. */
    TY_NOTIFY_HYBRID = 2
} ty_notify_mode_t;

/* ------------------------------------------------------------------ handles */

/** Opaque handle to a mapped region; process-private, not shareable. */
typedef struct ty_region ty_region_t;

/**
 * Handle to a topic. Points into the mapped region, therefore each thread or
 * process must obtain its own handle via ty_topic_open() -- never pass one
 * across a fork() into a child that remaps the region.
 */
typedef struct ty_topic ty_topic_t;

/* ------------------------------------------------------------ configuration */

typedef struct {
    /** Total bytes to reserve if the region has to be created (rounded up
     *  to the page size). Ignored when attaching to an existing region. */
    size_t size;

    /** Create the region if it does not exist yet. */
    int create;

    /** Fail if create is set and the region already exists. */
    int exclusive;

    /** Reserve pages backed by MAP_HUGETLB when possible (may fail silently). */
    int hugepage;

    /**
     * Spawn an in-process reaper thread that reclaims chunks abandoned by
     * crashed peers. One attached process doing this is enough; the first
     * opener wins. Set to 0 to rely on the external `tachyond` daemon.
     */
    int spawn_reaper;

    /**
     * Grace period before an outstanding loan is considered abandoned
     * (nanoseconds). 0 selects the 5s default. Must outlive the longest
     * legitimate loan-to-publish window in your workload.
     */
    uint64_t lease_ns;

    /** Directory holding the shm files (NULL -> /dev/shm). Ignored by memfd. */
    const char* dir;
} ty_region_conf_t;

/** Sensible defaults for ty_region_conf_t. Use with designated initialisers. */
extern const ty_region_conf_t ty_region_conf_default;

typedef struct {
    /** Number of partitions. Rounded up to a power of two (>= 1). */
    uint32_t partitions;

    /** Slots per partition ring. Rounded up to a power of two (>= 2). */
    uint32_t capacity;

    /** Bytes reserved for payload chunks inside this topic. */
    size_t pool_bytes;

    /** Consumer wake-up policy. */
    ty_notify_mode_t notify;

    /** Disable next-field padding if you know what you are doing. Reserved. */
    uint32_t flags;
} ty_topic_conf_t;

extern const ty_topic_conf_t ty_topic_conf_default;

/* ----------------------------------------------------------------- messages */

/**
 * A received message. `data` points into the shared region and stays valid
 * until ty_release() is called. It is NOT a copy -- never free() it, and never
 * hand it to another thread without transferring ownership.
 */
typedef struct {
    void*    data;      /**< payload address inside the mapped region        */
    uint32_t len;       /**< payload length written by the producer          */
    uint32_t capacity;  /**< usable bytes available in this chunk            */
    uint64_t key;       /**< routing key supplied by the producer            */
    uint64_t seq;       /**< per-partition monotonic sequence number         */
    uint32_t partition; /**< partition this message came from                */
    uint32_t _pad;
} ty_msg_t;

/* ------------------------------------------------------------------- region */

/**
 * Open (and optionally create) a shared region.
 *
 * @param name  Region name. Sanitised and prefixed internally; only
 *              [A-Za-z0-9_.-] should be used.
 * @param conf  Configuration. NULL means "attach to existing with defaults".
 * @param out   Receives the handle on success.
 * @return      TY_OK or a negative ty_err_t.
 */
ty_err_t ty_region_open(const char* name, const ty_region_conf_t* conf,
                        ty_region_t** out);

/**
 * Detach from a region. Unlinks the backing file only when this process was
 * the creator and no other process is attached.
 */
ty_err_t ty_region_close(ty_region_t* reg);

/**
 * Unlink a region by name. Existing mappings keep working in processes that
 * already attached; the name stops resolving for everyone else.
 */
ty_err_t ty_region_remove(const char* name, const char* dir);

/** Base address, useful for diagnostics. Never dereference across processes. */
void* ty_region_base(ty_region_t* reg);
size_t ty_region_size(ty_region_t* reg);
const char* ty_region_name(ty_region_t* reg);

/* -------------------------------------------------------------------- topic */

/**
 * Create a topic inside an open region.
 *
 * Concurrent creation from several processes is safe: exactly one wins and the
 * losers observe TY_ERR_EXIST, in which case call ty_topic_open().
 */
ty_err_t ty_topic_create(ty_region_t* reg, const char* name,
                         const ty_topic_conf_t* conf, ty_topic_t** out);

/** Attach to an existing topic by name. */
ty_err_t ty_topic_open(ty_region_t* reg, const char* name, ty_topic_t** out);

/** Detach a topic handle obtained from create or open. */
ty_err_t ty_topic_close(ty_topic_t* topic);

/**
 * Enumerate the topics currently present in a region.
 *
 * @param names     Caller-provided array of TACHYON_MAX_NAME+1 byte buffers,
 *                  or NULL to just count.
 * @param capacity  Number of slots in `names`.
 * @return the number of topics present (may exceed `capacity`), or a negative
 *         ty_err_t.
 *
 * Needed by inspection tooling: without it, a CLI has no way to discover what
 * a region contains and can only open topics whose names it already knows.
 */
int ty_topic_list(ty_region_t* reg, char (*names)[TACHYON_MAX_NAME + 1],
                  size_t capacity);

const char* ty_topic_name(const ty_topic_t* topic);
uint32_t ty_topic_partitions(const ty_topic_t* topic);
uint32_t ty_topic_capacity(const ty_topic_t* topic);

/* ------------------------------------------------------------- producer side */

/**
 * Borrow a writable chunk from the pool. Zero copy: you build the message
 * directly in shared memory, no intermediate buffer exists.
 *
 * @param size  Requested payload size. Rounded up to the next size class.
 * @param ptr   Receives the payload address.
 * @param cap   Receives the usable capacity (>= size). May be NULL.
 * @return      TY_OK, TY_ERR_NOMEM when the class is exhausted,
 *              TY_ERR_TOOBIG when size > TACHYON_MAX_MSG_SIZE.
 */
ty_err_t ty_loan(ty_topic_t* topic, size_t size, void** ptr, size_t* cap);

/**
 * Abandon a loaned chunk without publishing it. Always prefer this over
 * leaking a loan -- a leaked loan is reclaimed only after lease_ns elapses.
 */
ty_err_t ty_abort(ty_topic_t* topic, void* ptr);

/**
 * Publish a loaned chunk. The payload is not copied; only a 16-byte descriptor
 * lands in `hash(key) & (partitions-1)`.
 *
 * @param len  Actual payload length; must be <= the chunk capacity.
 * @param key  Routing key. Same key => same partition => strict FIFO order.
 */
ty_err_t ty_publish(ty_topic_t* topic, void* ptr, size_t len, uint64_t key);

/**
 * Convenience wrapper: loan + memcpy + publish. Slower than the explicit pair
 * but handy for small messages and for the bindings.
 */
ty_err_t ty_send(ty_topic_t* topic, const void* src, size_t len, uint64_t key);

/* ------------------------------------------------------------- consumer side */

/**
 * Receive the next message from one partition.
 *
 * @param partition  Partition index, must be < partitions.
 * @param timeout_ns < 0 blocks forever, 0 polls once, > 0 waits that long.
 * @return TY_OK, TY_ERR_EMPTY (poll found nothing), TY_ERR_TIMEOUT (blocked
 *         and nothing arrived).
 *
 * Ordering contract: within a partition messages are delivered in publish
 * order. Across partitions there is NO ordering guarantee -- that is the price
 * of parallelising by key, and it is stated here rather than buried in a doc.
 */
ty_err_t ty_subscribe(ty_topic_t* topic, uint32_t partition, ty_msg_t* msg,
                      int64_t timeout_ns);

/**
 * Receive from whichever partition has a pending message, starting the scan at
 * *hint and leaving the next hint behind. Use this when a single consumer owns
 * every partition; prefer ty_subscribe() when you want affinity.
 */
ty_err_t ty_subscribe_any(ty_topic_t* topic, uint32_t* hint, ty_msg_t* msg,
                          int64_t timeout_ns);

/**
 * Return a message's chunk to the pool. Must be called exactly once per
 * delivered message, otherwise the chunk leaks until the reaper notices.
 */
ty_err_t ty_release(ty_topic_t* topic, ty_msg_t* msg);

/* ---------------------------------------------------------------- backpressure */

/**
 * Block until the destination partition has room, or the deadline expires.
 * Zero replies TY_OK immediately when room exists.
 */
ty_err_t ty_wait_room(ty_topic_t* topic, uint64_t key, int64_t timeout_ns);

/** Occupancy of one partition, or of every partition when -1 is passed. */
ty_err_t ty_depth(ty_topic_t* topic, int32_t partition, uint64_t* out);

/* -------------------------------------------------------------------- stats */

typedef struct {
    uint64_t published;   /**< messages successfully enqueued                */
    uint64_t consumed;    /**< messages dequeued                             */
    uint64_t dropped;     /**< publishes refused because the ring was full    */
    uint64_t loan_ok;     /**< successful loans                              */
    uint64_t loan_nomem;  /**< loans refused due to pool exhaustion          */
    uint64_t reclaimed;   /**< chunks recovered from dead processes          */
    uint64_t wait_wakeups;/**< futex wakes handed to consumers               */
    uint64_t spin_spins;  /**< spin iterations before finding work           */
    uint64_t _rsv[3];
} ty_stats_t;

ty_err_t ty_topic_stats(ty_topic_t* topic, ty_stats_t* out);

/** Bytes charged to each size class and how many are currently free. */
typedef struct {
    uint64_t chunk_size;
    uint64_t chunk_count;
    uint64_t free_count;
} ty_pool_class_info_t;

ty_err_t ty_pool_info(ty_topic_t* topic, ty_pool_class_info_t* out, size_t n);

/* -------------------------------------------------------------- maintenance */

/**
 * Force a lease sweep right now instead of waiting for the reaper tick.
 * Returns the number of chunks reclaimed. Safe to call concurrently; one
 * process wins the race, the others see 0.
 */
uint64_t ty_reclaim_once(ty_topic_t* topic);

/**
 * Refresh this process's heartbeat timestamp. Applications that block for long
 * stretches outside tachyon calls should tick this periodically so the reaper
 * does not mistake a busy process for a dead one.
 */
void ty_heartbeat(ty_region_t* reg);

/* ------------------------------------------------------------------ helpers */

/** Routing predecessor: which partition a key maps to, without touching data. */
uint32_t ty_route(const ty_topic_t* topic, uint64_t key);

/** Size-class boundaries, exposed for tests and for capacity planning. */
size_t ty_size_class(size_t index);
size_t ty_size_class_count(void);

/** Wall clock in nanoseconds (CLOCK_MONOTONIC_RAW), selling point of the lib. */
uint64_t ty_now_ns(void);

/** rdtsc-derived nanosecond timestamp; faster but requires invariant TSC. */
uint64_t ty_now_ns_fast(void);

/** True when the CPU reports an invariant, synchronised TSC. */
int ty_tsc_usable(void);

/** Number of online CPUs, used to pick a default partition count. */
int ty_cpu_count(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* TACHYON_H */
