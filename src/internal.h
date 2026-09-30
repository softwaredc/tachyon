/*
 * internal.h -- shared-memory layout of a tachyon region.
 *
 * Every structure below lives inside the shared region and is therefore
 * subject to three rules that are easy to get wrong:
 *
 *   1. No virtual pointers. Nothing that the compiler would relocate may be
 *      stored in the region, so all links are absolute offsets from the region
 *      base, resolved through TY_PTR() at access time.
 *   2. Fixed POD layout only. No vtables, no STL containers, no embedded
 *      pointers, no allocator state.
 *   3. Explicit cache-line separation. Every counter that two cores might
 *      write concurrently gets its own line; a single accidental sharing costs
 *      more throughput than all of the atomics combined.
 *
 * All atomics must be lock-free for the cross-process guarantees to hold, and
 * that is asserted at compile time at the bottom of this file.
 */
#ifndef TACHYON_INTERNAL_H
#define TACHYON_INTERNAL_H

/* _GNU_SOURCE is required before any libc header for two things we genuinely
 * depend on: EOWNERDEAD for robust mutex recovery, and the futex constants. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>

#include "tachyon/tachyon.h"

/* The public header spells the count TACHYON_SIZE_CLASSES; internally the
 * abbreviation is used pervasively. One alias keeps them from drifting. */
#define TY_SIZE_CLASSES TACHYON_SIZE_CLASSES

/* Robust-mutex recovery codes. They are guaranteed stable on Linux but are not
 * always visible depending on which feature macros were set before glibc's
 * headers were first pulled in, so they are pinned here rather than left to
 * the mercy of include order. */
#ifndef EOWNERDEAD
#define EOWNERDEAD 130
#endif
#ifndef ENOTRECOVERABLE
#define ENOTRECOVERABLE 131
#endif

/* --------------------------------------------------------------- constants */

#define TY_REGION_MAGIC  0x74616368ULL /* "tach" */
#define TY_TOPIC_MAGIC   0x54795031ULL /* "TyP1" */
#define TY_CHUNK_MAGIC   0x63686E6BULL /* "chnk" */
#define TY_MAGIC         TY_REGION_MAGIC

#define TY_NIL_IDX       0xFFFFFFFFu

#define TY_CACHELINE     64
#define TY_CHUNK_HDR_SZ  64
#define TY_DESC_SZ       16

/** Chunk lifecycle. Transitions are Owner-based and checked with CAS. */
enum {
    TY_CHUNK_FREE      = 0, /**< owned by the pool free-list                */
    TY_CHUNK_LOANED    = 1, /**< owned by the producer that loaned it       */
    TY_CHUNK_PUBLISHED = 2, /**< descriptor sits in a ring, not yet read     */
    TY_CHUNK_CONSUMED  = 3  /**< handed to a consumer, awaiting ty_release   */
};

enum { TY_TOPIC_INIT = 0, TY_TOPIC_READY = 1, TY_TOPIC_DEAD = 2 };
enum { TY_PROC_FREE = 0, TY_PROC_LIVE = 1 };

#define TY_ALIGN(_n, _a)   (((_n) + (_a) - 1u) & ~((_a) - 1u))
#define TY_ALIGN64(_n)     TY_ALIGN((_n), 64u)

/* ------------------------------------------------------------ size classes */

/** Payload capacities, in bytes. Must be ascending. Index 0 == smallest. */
extern const uint64_t ty_size_classes[TY_SIZE_CLASSES];

/* --------------------------------------------------------------- atomics */

typedef _Atomic uint32_t ty_atomic_u32;
typedef _Atomic uint64_t ty_atomic_u64;
typedef _Atomic int32_t  ty_atomic_i32;

#define TY_LOAD_RELAXED(_p)   atomic_load_explicit((_p), memory_order_relaxed)
#define TY_LOAD_ACQUIRE(_p)   atomic_load_explicit((_p), memory_order_acquire)
#define TY_STORE_RELAXED(_p, _v) atomic_store_explicit((_p), (_v), memory_order_relaxed)
#define TY_STORE_RELEASE(_p, _v) atomic_store_explicit((_p), (_v), memory_order_release)
#define TY_FETCH_ADD(_p, _v)  atomic_fetch_add_explicit((_p), (_v), memory_order_relaxed)
#define TY_FETCH_SUB(_p, _v)  atomic_fetch_sub_explicit((_p), (_v), memory_order_relaxed)
#define TY_CAS_WEAK(_p, _e, _d) atomic_compare_exchange_weak_explicit( \
        (_p), (_e), (_d), memory_order_acq_rel, memory_order_acquire)
#define TY_CAS_STRONG(_p, _e, _d) atomic_compare_exchange_strong_explicit( \
        (_p), (_e), (_d), memory_order_acq_rel, memory_order_acquire)

/* Pack/unpack the generation-tagged index used by every free-list. The tag
 * defeats ABA: two producers racing on pop() can only both succeed if the
 * other side completes exactly 2^32 operations in between. */
static inline uint64_t ty_pack_tag(uint32_t idx, uint32_t tag) {
    return ((uint64_t)tag << 32) | (uint64_t)idx;
}
static inline uint32_t ty_tag_idx(uint64_t packed) { return (uint32_t)(packed & 0xFFFFFFFFu); }
static inline uint32_t ty_tag_gen(uint64_t packed) { return (uint32_t)(packed >> 32); }

/* ------------------------------------------------------------ fingerprint */

/* SplitMix64 finalizer: cheap, excellent avalanche across the integer keys
 * that routing keys usually are. Not a PRNG; do not use it as one. */
static inline uint64_t ty_mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static inline void ty_cpu_relax(void) {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    __atomic_thread_fence(memory_order_seq_cst);
#endif
}

/* ---------------------------------------------------------------- chunk hdr */

/**
 * 64-byte prefix in front of every payload. Sized so the payload itself starts
 * 64-byte aligned, which matters for the benchmarks more than you would think.
 */
typedef struct ty_chunk_s {
    uint32_t       magic;           /* 0   TY_CHUNK_MAGIC                    */
    ty_atomic_u32  state;           /* 4   one of TY_CHUNK_*                 */
    uint32_t       class_id;        /* 8                                     */
    uint32_t       size;            /* 12  payload length at publish         */
    ty_atomic_u32  next;            /* 16  free-list link                    */
    ty_atomic_i32  owner_pid;       /* 20  who loaned it                     */
    uint32_t       partition;       /* 24                                    */
    uint32_t       reserved0;       /* 28                                    */
    ty_atomic_u64  lease_expire_ns; /* 32  reaper deadline                   */
    ty_atomic_u64  seq;             /* 40  per-partition publish sequence    */
    uint64_t       reserved1[2];    /* 48..63                                */
} ty_chunk_t;

/* ------------------------------------------------------------ ring design */

/**
 * One descriptor slot. Vyukov's bounded MPMC algorithm requires that `seq` and
 * the payload live in the same cell; keeping the cell at exactly 16 bytes lets
 * a cache line carry four descriptors, which is what makes batching pay off.
 */
typedef struct ty_desc_s {
    ty_atomic_u64  seq;
    uint32_t       chunk_idx;
    uint32_t       len;
} ty_desc_t;

/**
 * Per-partition control block. `enq_pos` and `deq_pos` are written by opposing
 * camps (producers vs consumers) and would ping-pong a cache line between
 * cores if they were adjacent -- hence the padded union layout.
 */
typedef struct ty_ring_s {
    union { ty_atomic_u64 enq_pos; char pad0[64]; };
    union { ty_atomic_u64 deq_pos; char pad1[64]; };
    union {
        struct {
            ty_atomic_u32 futex_word;  /**< bumped on every publish         */
            ty_atomic_u32 waiters;     /**< consumers currently blocked     */
        };
        char pad2[64];
    };
    uint64_t descs_off;   /**< absolute region offset of the desc array      */
    uint64_t capacity;    /**< power of two                                  */
    uint64_t mask;        /**< capacity - 1                                  */
    char     pad3[64 - 24];
} ty_ring_t;

/* ----------------------------------------------------------- pool layout */

/**
 * Two cache lines per class: line 0 holds the contended free-list head, line 1
 * the read-mostly geometry. Splitting them keeps bulk attribute reads from
 * stealing the line away from concurrent pops.
 */
typedef struct ty_pool_class_s {
    union {
        struct {
            /* Generation-tagged Treiber stack head. */
            ty_atomic_u64 free_head;
            char          unused[56];
            uint64_t      chunk_size;   /**< usable payload bytes            */
            uint64_t      chunk_stride; /**< hdr + payload, cache padded     */
            uint64_t      chunk_count;
            uint64_t      gid_base;   /**< first global chunk id of this class */
            uint64_t      chunks_off;   /**< absolute region offset          */
            uint64_t      bitmap_off;   /**< loan bitmap, absolute offset    */
            uint64_t      bitmap_words;
            uint64_t      reserved0;
        };
        char pad[128];
    };
} ty_pool_class_t;

typedef struct ty_pool_s {
    ty_pool_class_t classes[TY_SIZE_CLASSES];
} ty_pool_t;

/* ----------------------------------------------------------- shared topic */

typedef struct ty_topic_shm_s {
    /* ---- immutable after creation ---- */
    uint64_t       magic;                                    /* 0   */
    uint32_t       abi_version;                               /* 8   */
    uint32_t       state;                                     /* 12  */
    char           name[TACHYON_MAX_NAME + 1];                /* 16  */
    uint32_t       partitions;                                /*     */
    uint32_t       part_mask;
    uint32_t       capacity;
    uint32_t       ring_mask;
    uint64_t       max_msg_size;
    uint32_t       notify_mode;
    uint32_t       flags;
    uint64_t       lease_ns;
    uint64_t       rings_off;
    uint64_t       pool_off;
    uint64_t       total_bytes;
    uint64_t       _reserved0;

    /* ---- hot, each on its own line ---- */
    _Alignas(64) ty_atomic_u32 refcount;
    _Alignas(64) ty_atomic_u64 published;
    _Alignas(64) ty_atomic_u64 consumed;
    _Alignas(64) ty_atomic_u64 dropped;
    _Alignas(64) ty_atomic_u64 loan_ok;
    _Alignas(64) ty_atomic_u64 loan_nomem;
    _Alignas(64) ty_atomic_u64 reclaimed;
    _Alignas(64) ty_atomic_u64 wait_wakeups;
    _Alignas(64) ty_atomic_u64 spin_spins;
    _Alignas(64) ty_atomic_u32 reclaim_lock;  /**< trylock, reaper exclusivity */
    _Alignas(64) ty_atomic_u64 last_reclaim_ns;
    _Alignas(64) ty_atomic_u64 bytes_in_use;
} ty_topic_shm_t;

/* ---------------------------------------------------------- region header */

typedef struct ty_proc_slot_s {
    ty_atomic_i32  pid;
    ty_atomic_u64  heartbeat_ns;
    ty_atomic_u32  state;
    uint32_t       reserved;
    char           pad[TY_CACHELINE - 24];
} ty_proc_slot_t;

#define TY_TOPIC_DIR_SZ 128

typedef struct ty_topic_dir_s {
    ty_atomic_u32  state;                       /**< 0 free, 1 ready          */
    uint32_t       sequence;                    /**< bumps on reuse           */
    char           name[TACHYON_MAX_NAME + 1];
    uint64_t       off;       /**< absolute region offset of the topic area   */
    char           pad[TY_TOPIC_DIR_SZ - 8 - (TACHYON_MAX_NAME + 1) - 8];
} ty_topic_dir_t;

typedef struct ty_region_hdr_s {
    uint64_t       magic;
    uint32_t       abi_version;
    uint32_t       version_packed;      /**< (major<<16)|(minor<<8)|patch    */
    uint64_t       region_size;
    uint64_t       page_size;
    uint64_t       create_time_ns;
    uint64_t       reserved0;
    uint64_t       lease_ns;            /**< region-wide default lease       */
    uint64_t       reserved1;

    /* Control-plane mutex. Process-shared AND robust: if the holder is
     * SIGKILLed the next acquirer gets EOWNERDEAD and repairs state. It is
     * never taken on the data path -- only for slot allocation and reaping. */
    _Alignas(64) pthread_mutex_t     ctl_lock;
    pthread_mutexattr_t              ctl_lock_attr;

    /* Bump allocator that topic areas are carved out of. */
    _Alignas(64) ty_atomic_u64 bump_off;

    _Alignas(64) ty_atomic_u32 creator_pid;
    _Alignas(64) ty_atomic_u32 attached;      /**< live openers               */
    _Alignas(64) ty_atomic_u32 reaper_pid;
    _Alignas(64) ty_atomic_u64 reaper_heartbeat_ns;

    _Alignas(64) ty_proc_slot_t  procs[TACHYON_MAX_PROCS];
    _Alignas(64) ty_topic_dir_t  topics[TACHYON_MAX_TOPICS];
} ty_region_hdr_t;

#define TY_PROCS_OFFSET   offsetof(ty_region_hdr_t, procs)
#define TY_TOPICS_OFFSET  offsetof(ty_region_hdr_t, topics)

/* ------------------------------------------------------------- handles */

/* Set on a live handle and cleared by ty_region_close, so a double close (or
 * any use of a handle after close) returns an error instead of dereferencing
 * freed memory. Cheap insurance against the most common misuse of a C API. */
#define TY_HANDLE_MAGIC 0xA11CE55UL

struct ty_region {
    uint32_t          magic;
    void*             base;
    size_t            size;
    ty_region_hdr_t*  hdr;
    int               fd;
    int               use_shm;      /**< 1: shm_unlink on close, 0: unlink   */
    int               created;      /**< this process created the region     */
    int               proc_slot;    /**< -1 when the table was full           */
    char              name[TACHYON_MAX_NAME + 1];
    char              shm_path[256];
    uint64_t          lease_ns;
    int               spawn_reaper;
    pthread_t         reaper_tid;
    ty_atomic_u32     reaper_running;
    ty_atomic_u32     closing;

    /**
     * Process-local references to this handle: one for the opener, plus one
     * per live topic. ty_region_close() tears down the mapping but must not
     * free() the handle while a topic still points at it, otherwise
     * ty_topic_close() writes its refcount into unmapped memory. The last
     * reference holder frees it.
     */
    ty_atomic_u32     handle_refs;
    ty_region_conf_t  conf;
};

struct ty_topic {
    ty_region_t*      reg;
    ty_topic_shm_t*   shm;
    uint64_t          off;          /**< absolute region offset of shm       */
    char              name[TACHYON_MAX_NAME + 1];
    ty_ring_t*        rings;        /**< rings_off resolved at open time      */
    ty_pool_t*        pool;
};

/** Resolve an absolute region offset. The only legal way to turn an offset
 *  stored in the shared region into something dereferenceable. */
#define TY_PTR(_reg, _off)  ((void*)((char*)((_reg)->base) + (uint64_t)(_off)))

static inline ty_ring_t* ty_topic_ring(ty_topic_t* t, uint32_t p) {
    return (ty_ring_t*)TY_PTR(t->reg, t->shm->rings_off + (uint64_t)p * sizeof(ty_ring_t));
}
static inline ty_desc_t* ty_ring_descs(ty_region_t* reg, ty_ring_t* r) {
    return (ty_desc_t*)TY_PTR(reg, r->descs_off);
}
static inline ty_chunk_t* ty_chunk_addr(ty_pool_class_t* c, ty_region_t* reg, uint32_t idx) {
    return (ty_chunk_t*)TY_PTR(reg, c->chunks_off + (uint64_t)idx * c->chunk_stride);
}
static inline void* ty_chunk_payload(ty_chunk_t* c) {
    return (void*)((char*)c + TY_CHUNK_HDR_SZ);
}
static inline ty_chunk_t* ty_payload_chunk(void* p) {
    return (ty_chunk_t*)((char*)p - TY_CHUNK_HDR_SZ);
}

/** Smallest class able to hold `size`, or TY_SIZE_CLASSES when too big. */
static inline uint32_t ty_class_for(size_t size) {
    for (uint32_t i = 0; i < TY_SIZE_CLASSES; ++i) {
        if (size <= ty_size_classes[i]) return i;
    }
    return TY_SIZE_CLASSES;
}

/** Reverse-map a chunk header to its index inside the owning class. */
static inline uint32_t ty_class_idx_of(ty_pool_class_t* cls, ty_region_t* reg,
                                       ty_chunk_t* ch) {
    uint64_t addr = (uint64_t)((char*)ch - (char*)reg->base);
    return (uint32_t)((addr - cls->chunks_off) / cls->chunk_stride);
}

/**
 * Descriptors carry a 16-byte budget, which leaves room for one index and no
 * second field for "which size class". So chunks are numbered globally across
 * classes and this does the reverse lookup. Eight comparisons on the receive
 * path buys a descriptor that fits in a single cache line quartet.
 */
static inline ty_chunk_t* ty_gid_resolve(ty_pool_t* pool, ty_region_t* reg,
                                         uint32_t gid, ty_pool_class_t** cls_out) {
    for (uint32_t c = 0; c < TY_SIZE_CLASSES; ++c) {
        ty_pool_class_t* cls = &pool->classes[c];
        if (gid >= cls->gid_base && gid < cls->gid_base + cls->chunk_count) {
            if (cls_out) *cls_out = cls;
            return ty_chunk_addr(cls, reg, (uint32_t)(gid - cls->gid_base));
        }
    }
    return NULL;
}

/* util.c */
uint64_t ty__now_ns(void);
int      ty__pid_alive(int pid);
void     ty__touch_heartbeat(ty_region_t* reg);
int      ty__proc_slot_acquire(ty_region_t* reg);
void     ty__proc_slot_release(ty_region_t* reg);
int      ty__futex_wait(ty_atomic_u32* word, uint32_t expected, int64_t timeout_ns);
int      ty__futex_wake(ty_atomic_u32* word, uint32_t count);

/* ------------------------------------------------------- cross-TU internals */

/* ring.c */
void     ty_ring_init(ty_region_t* reg, ty_ring_t* ring, uint64_t descs_off,
                      uint64_t capacity, uint64_t notify_mode);
ty_err_t ty_ring_enqueue(ty_ring_t* ring, ty_desc_t* descs, uint32_t chunk_idx,
                         uint32_t len);
ty_err_t ty_ring_dequeue(ty_ring_t* ring, ty_desc_t* descs, uint32_t* chunk_idx,
                         uint32_t* len, uint64_t* seq);
uint64_t ty_ring_depth(const ty_ring_t* ring);

/* notify.c */
void     ty_ring_wake(ty_topic_t* topic, ty_ring_t* ring);
int      ty_spin_once(ty_ring_t* ring, ty_desc_t* descs, uint32_t* idx,
                      uint32_t* len, uint64_t* seq, int rounds, ty_topic_t* topic);
ty_err_t ty_wait_for_msg(ty_topic_t* topic, ty_ring_t* ring, ty_desc_t* descs,
                         uint32_t* idx, uint32_t* len, uint64_t* seq,
                         int64_t timeout_ns, uint64_t deadline_ns);

/* pool.c */
int      ty_pool_init(ty_region_t* reg, ty_pool_t* pool, size_t payload_bytes,
                      uint64_t* cur_off, uint64_t region_limit);
ty_err_t ty_pool_loan(ty_topic_t* topic, size_t size, void** out, size_t* cap);
ty_err_t ty_pool_abort(ty_topic_t* topic, void* ptr);
ty_err_t ty_pool_release(ty_topic_t* topic, ty_chunk_t* ch);
ty_err_t ty_pool_class_info(ty_topic_t* topic, ty_pool_class_info_t* out, size_t n);

/* reaper.c */
uint64_t ty__reclaim_topic(ty_topic_t* topic, uint64_t now_ns, int dry_run);

/* Compile-time layout guards. If any of these fire, the ABI changed without
 * bumping TACHYON_ABI_VERSION -- which would silently corrupt existing
 * regions, so the build must stop here. */
_Static_assert(sizeof(ty_chunk_t) == 64, "chunk header must be one cache line");
_Static_assert(sizeof(ty_desc_t) == 16, "descriptor must be 16 bytes");
_Static_assert(sizeof(ty_ring_t) == 256, "ring control block must be 256 bytes");
_Static_assert(sizeof(ty_pool_class_t) == 128, "pool class must be two cache lines");
_Static_assert(sizeof(ty_proc_slot_t) == 64, "proc slot must be one cache line");
_Static_assert(sizeof(ty_topic_dir_t) == 128, "topic dir entry must be 128 bytes");
_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2, "need lock-free 64-bit atomics");
_Static_assert(ATOMIC_INT_LOCK_FREE == 2, "need lock-free 32-bit atomics");
_Static_assert(TACHYON_SIZE_CLASSES == 8, "size class table changed");

#endif /* TACHYON_INTERNAL_H */
