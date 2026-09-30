/*
 * topic.c -- the API surface: routing, publish, consume, bookkeeping.
 *
 * Everything here is deliberately thin. Each public call is a handful of steps
 * whose cost is paid once per message, so the really hot parts live in
 * ring.c / pool.c / notify.c and this file only wires them together.
 *
 * Routing contract, stated once more because it is the thing users get wrong:
 *   partition = mix64(key) & (partitions - 1)
 *
 * Same key => same partition => strictly ordered, exactly like a Kafka
 * partition. Different keys may be consumed concurrently and with no mutual
 * ordering whatsoever. That is not a bug to be fixed later; it is the price of
 * being able to consume in parallel, and it is documented rather than quietly
 * violated.
 */
#include "internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------ layout */

typedef struct {
    uint64_t topic_bytes;
    uint64_t rings_bytes;
    uint64_t descs_bytes;
    uint64_t pool_bytes;
    uint64_t total;
} ty_layout_t;

static void ty_topic_measure(const ty_topic_conf_t* conf, uint32_t partitions,
                             uint32_t capacity, ty_layout_t* out) {
    out->topic_bytes = TY_ALIGN64(sizeof(ty_topic_shm_t));
    out->rings_bytes = TY_ALIGN64((uint64_t)partitions * sizeof(ty_ring_t));
    out->descs_bytes = TY_ALIGN64((uint64_t)partitions * capacity * TY_DESC_SZ);
    out->pool_bytes  = TY_ALIGN64(sizeof(ty_pool_t));
    out->total       = out->topic_bytes + out->rings_bytes + out->descs_bytes +
                       out->pool_bytes + conf->pool_bytes;
}

static uint32_t ty_round_pow2(uint32_t v) {
    if (v <= 1) return 1;
    uint32_t p = 1;
    while (p < v) p <<= 1;
    return p;
}

/* Handles are invalidated by ty_region_close(); every entry point that takes
 * one checks, rather than walking into a freed mapping. */
static inline int ty_region_live(ty_region_t* reg) {
    return reg && reg->magic == TY_HANDLE_MAGIC && reg->hdr;
}

/* -------------------------------------------------------------- create/open */

ty_err_t ty_topic_create(ty_region_t* reg, const char* name,
                         const ty_topic_conf_t* conf, ty_topic_t** out) {
    if (!ty_region_live(reg) || !name || !out) return TY_ERR_INVAL;
    if (strlen(name) == 0 || strlen(name) > TACHYON_MAX_NAME) return TY_ERR_INVAL;

    const ty_topic_conf_t* c = conf ? conf : &ty_topic_conf_default;
    uint32_t partitions = ty_round_pow2(c->partitions ? c->partitions : 1);
    uint32_t capacity   = ty_round_pow2(c->capacity   ? c->capacity   : 2);
    if (capacity < 2) capacity = 2;

    ty_region_hdr_t* hdr = reg->hdr;
    ty_topic_dir_t*  dir = (ty_topic_dir_t*)TY_PTR(reg, TY_TOPICS_OFFSET);

    /* The control lock is process-shared and robust, so a SIGKILL inside this
     * critical section shows up as EOWNERDEAD on the next acquirer instead of
     * wedging every future topic creation forever. */
    int rc = pthread_mutex_lock(&hdr->ctl_lock);
    if (rc == EOWNERDEAD) {
        pthread_mutex_consistent(&hdr->ctl_lock);
    } else if (rc != 0) {
        return TY_ERR_IO;
    }

    int slot = -1;
    for (int i = 0; i < TACHYON_MAX_TOPICS; ++i) {
        if (TY_LOAD_RELAXED(&dir[i].state) == TY_TOPIC_READY &&
            strncmp(dir[i].name, name, TACHYON_MAX_NAME) == 0) {
            pthread_mutex_unlock(&hdr->ctl_lock);
            return TY_ERR_EXIST;
        }
    }
    for (int i = 0; i < TACHYON_MAX_TOPICS; ++i) {
        uint32_t st = 0;
        if (TY_LOAD_RELAXED(&dir[i].state) == 0 &&
            atomic_compare_exchange_strong_explicit(&dir[i].state, &st, 1u,
                                                    memory_order_acq_rel,
                                                    memory_order_relaxed)) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        pthread_mutex_unlock(&hdr->ctl_lock);
        return TY_ERR_NOMEM; /* topic table full */
    }

    ty_layout_t L;
    ty_topic_measure(c, partitions, capacity, &L);

    uint64_t off  = TY_ALIGN64(TY_LOAD_RELAXED(&hdr->bump_off));
    if (off + L.total > (uint64_t)reg->size) {
        TY_STORE_RELAXED(&dir[slot].state, 0);
        pthread_mutex_unlock(&hdr->ctl_lock);
        return TY_ERR_NOMEM;
    }

    ty_topic_shm_t* ts = (ty_topic_shm_t*)TY_PTR(reg, off);
    memset(ts, 0, sizeof(*ts));

    uint64_t rings_off = off + L.topic_bytes;
    uint64_t descs_off = rings_off + L.rings_bytes;
    uint64_t pool_off  = descs_off + L.descs_bytes;

    ts->magic         = TY_TOPIC_MAGIC;
    ts->abi_version   = TACHYON_ABI_VERSION;
    ts->state         = TY_TOPIC_INIT;
    snprintf(ts->name, sizeof ts->name, "%s", name);
    ts->partitions    = partitions;
    ts->part_mask     = partitions - 1;
    ts->capacity      = capacity;
    ts->ring_mask     = capacity - 1;
    ts->max_msg_size  = c->pool_bytes ? TACHYON_MAX_MSG_SIZE : TACHYON_MAX_MSG_SIZE;
    ts->notify_mode   = (uint32_t)c->notify;
    ts->lease_ns      = reg->lease_ns;
    ts->rings_off     = rings_off;
    ts->pool_off      = pool_off;
    ts->total_bytes   = L.total;

    /* Rings and their descriptor arrays. */
    ty_ring_t* rings = (ty_ring_t*)TY_PTR(reg, rings_off);
    for (uint32_t p = 0; p < partitions; ++p) {
        uint64_t d_off = descs_off + (uint64_t)p * capacity * TY_DESC_SZ;
        ty_ring_init(reg, &rings[p], d_off, capacity, ts->notify_mode);
    }

    /* Chunk pool: fills the tail of this topic's area. */
    uint64_t cur = pool_off + L.pool_bytes;
    int prc = ty_pool_init(reg, (ty_pool_t*)TY_PTR(reg, pool_off),
                           c->pool_bytes, &cur, (uint64_t)reg->size);
    if (prc != TY_OK) {
        TY_STORE_RELAXED(&dir[slot].state, 0);
        pthread_mutex_unlock(&hdr->ctl_lock);
        return (ty_err_t)prc;
    }

    atomic_store(&ts->refcount, 0);
    atomic_store(&ts->reclaim_lock, 0);

    uint64_t end = TY_ALIGN64(cur);
    TY_STORE_RELAXED(&hdr->bump_off, end);
    ts->total_bytes = end - off;

    /* Publish the directory entry last, with a release store, so any process
     * that sees READY is guaranteed to see a fully constructed topic. */
    strncpy(dir[slot].name, name, TACHYON_MAX_NAME);
    dir[slot].name[TACHYON_MAX_NAME] = '\0';
    dir[slot].off   = off;
    dir[slot].sequence++;
    atomic_thread_fence(memory_order_release);
    TY_STORE_RELEASE(&dir[slot].state, TY_TOPIC_READY);

    ts->state = TY_TOPIC_READY;
    pthread_mutex_unlock(&hdr->ctl_lock);

    return ty_topic_open(reg, name, out);
}

ty_err_t ty_topic_open(ty_region_t* reg, const char* name, ty_topic_t** out) {
    if (!ty_region_live(reg) || !name || !out) return TY_ERR_INVAL;

    ty_topic_dir_t* dir = (ty_topic_dir_t*)TY_PTR(reg, TY_TOPICS_OFFSET);
    for (int i = 0; i < TACHYON_MAX_TOPICS; ++i) {
        if (TY_LOAD_ACQUIRE(&dir[i].state) != TY_TOPIC_READY) continue;
        if (strncmp(dir[i].name, name, TACHYON_MAX_NAME) != 0) continue;

        ty_topic_shm_t* ts = (ty_topic_shm_t*)TY_PTR(reg, dir[i].off);
        if (ts->magic != TY_TOPIC_MAGIC) return TY_ERR_INTERNAL;
        if (ts->abi_version != TACHYON_ABI_VERSION) return TY_ERR_ABI;
        if (ts->state != TY_TOPIC_READY) return TY_ERR_STATE;

        ty_topic_t* t = (ty_topic_t*)calloc(1, sizeof(ty_topic_t));
        if (!t) return TY_ERR_NOMEM;
        t->reg  = reg;
        t->shm  = ts;
        t->off  = dir[i].off;
        t->rings = (ty_ring_t*)TY_PTR(reg, ts->rings_off);
        t->pool  = (ty_pool_t*)TY_PTR(reg, ts->pool_off);
        snprintf(t->name, sizeof t->name, "%s", name);

        TY_FETCH_ADD(&ts->refcount, 1);
        TY_FETCH_ADD(&reg->handle_refs, 1u);  /* keeps reg alive for close() */
        *out = t;
        return TY_OK;
    }
    return TY_ERR_NOENT;
}

int ty_topic_list(ty_region_t* reg, char (*names)[TACHYON_MAX_NAME + 1],
                  size_t capacity) {
    if (!ty_region_live(reg)) return TY_ERR_INVAL;

    ty_topic_dir_t* dir = (ty_topic_dir_t*)TY_PTR(reg, TY_TOPICS_OFFSET);
    int found = 0;

    for (int i = 0; i < TACHYON_MAX_TOPICS; ++i) {
        if (TY_LOAD_ACQUIRE(&dir[i].state) != TY_TOPIC_READY) continue;
        if (names && (size_t)found < capacity) {
            memcpy(names[found], dir[i].name, TACHYON_MAX_NAME + 1);
            names[found][TACHYON_MAX_NAME] = '\0';
        }
        ++found;
    }
    return found;
}

ty_err_t ty_topic_close(ty_topic_t* topic) {
    if (!topic) return TY_ERR_INVAL;

    ty_region_t* reg = topic->reg;

    /* The region may already have been detached, in which case its mapping is
     * gone and touching shm would fault. Ordering matters more than people
     * expect: GC-driven destructors (Python, Go finalizers) close objects in
     * an arbitrary order, so closing a topic after its region has to work. */
    if (reg && reg->magic == TY_HANDLE_MAGIC && reg->base) {
        TY_FETCH_ADD(&topic->shm->refcount, (uint32_t)-1);
    }
    free(topic);

    if (reg && TY_FETCH_SUB(&reg->handle_refs, 1u) == 1u) free(reg);
    return TY_OK;
}

const char* ty_topic_name(const ty_topic_t* t) { return t ? t->name : NULL; }
uint32_t ty_topic_partitions(const ty_topic_t* t) { return t ? t->shm->partitions : 0; }
uint32_t ty_topic_capacity(const ty_topic_t* t) { return t ? t->shm->capacity : 0; }

uint32_t ty_route(const ty_topic_t* t, uint64_t key) {
    if (!t) return 0;
    return (uint32_t)(ty_mix64(key) & (uint64_t)t->shm->part_mask);
}

/* ------------------------------------------------------------------ produce */

ty_err_t ty_loan(ty_topic_t* t, size_t size, void** ptr, size_t* cap) {
    if (!t || !ptr) return TY_ERR_INVAL;
    return ty_pool_loan(t, size, ptr, cap);
}

ty_err_t ty_abort(ty_topic_t* t, void* ptr) {
    if (!t) return TY_ERR_INVAL;
    return ty_pool_abort(t, ptr);
}

ty_err_t ty_publish(ty_topic_t* t, void* ptr, size_t len, uint64_t key) {
    if (!t || !ptr) return TY_ERR_INVAL;
    if (len > TACHYON_MAX_MSG_SIZE) return TY_ERR_TOOBIG;

    ty_chunk_t* ch = ty_payload_chunk(ptr);
    if (ch->magic != TY_CHUNK_MAGIC) return TY_ERR_INVAL;

    ty_region_t*     reg = t->reg;
    uint32_t         cls_i = ch->class_id;
    if (cls_i >= TY_SIZE_CLASSES) return TY_ERR_INTERNAL;
    ty_pool_class_t* cls = &t->pool->classes[cls_i];

    uint32_t idx = ty_class_idx_of(cls, reg, ch);
    if (len > cls->chunk_size) return TY_ERR_TOOBIG;

    uint32_t part = ty_route(t, key);
    ty_ring_t* ring  = ty_topic_ring(t, part);
    ty_desc_t* descs = ty_ring_descs(reg, ring);

    /* Global id, so the 16-byte descriptor needs only one index field. */
    uint32_t gid = (uint32_t)(cls->gid_base + idx);

    /*
     * Ordering here is the difference between a correct library and one that
     * silently corrupts memory under load.
     *
     * The chunk must become PUBLISHED *before* its descriptor becomes visible
     * in the ring. Doing it the other way round leaves a window in which a
     * consumer has already dequeued gid and flipped the chunk to CONSUMED,
     * while the producer's own CAS then either fails outright or -- far worse
     * -- lands on a chunk that some other producer has meanwhile loaned again,
     * clearing ownership markers underneath a live message.
     *
     * If enqueue fails we must undo the transition, otherwise the loan leaks
     * for a full lease period.
     */
    uint32_t expect = TY_CHUNK_LOANED;
    if (!atomic_compare_exchange_strong_explicit(&ch->state, &expect,
                                                 TY_CHUNK_PUBLISHED,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return TY_ERR_STATE;
    }

    ty_err_t rc = ty_ring_enqueue(ring, descs, gid, (uint32_t)len);
    if (rc != TY_OK) {
        /* Give ownership back to the caller, who may retry or abort. */
        uint32_t back = TY_CHUNK_PUBLISHED;
        atomic_compare_exchange_strong_explicit(&ch->state, &back,
                                                TY_CHUNK_LOANED,
                                                memory_order_acq_rel,
                                                memory_order_relaxed);
        TY_FETCH_ADD(&t->shm->dropped, 1);
        return rc;
    }

    ch->size      = (uint32_t)len;
    ch->partition = part;
    /* The key rides along inside the chunk header: it ships for free in memory
     * that is already allocated, costs nothing to write, and lets consumers
     * filter without producers having to duplicate it into the payload. */
    TY_STORE_RELAXED((ty_atomic_u64*)&ch->reserved1[0], key);
    TY_STORE_RELAXED(&ch->lease_expire_ns, 0);
    TY_STORE_RELAXED(&ch->owner_pid, 0);

    /* Producer's ownership ends here: clear the lease bit so the reaper stops
     * considering this chunk. Its fate now belongs to the ring. */
    uint64_t words_off = cls->bitmap_off;
    ty_atomic_u64* words = (ty_atomic_u64*)TY_PTR(reg, words_off);
    atomic_fetch_and_explicit(&words[idx >> 6],
                              ~((uint64_t)1 << (idx & 63u)), memory_order_acq_rel);

    TY_FETCH_ADD(&t->shm->published, 1);
    ty_ring_wake(t, ring);
    return TY_OK;
}

ty_err_t ty_send(ty_topic_t* t, const void* src, size_t len, uint64_t key) {
    void* buf;
    size_t cap;
    ty_err_t rc = ty_loan(t, len, &buf, &cap);
    if (rc != TY_OK) return rc;
    if (src && len) memcpy(buf, src, len);
    rc = ty_publish(t, buf, len, key);
    if (rc != TY_OK) ty_abort(t, buf);
    return rc;
}

/* ------------------------------------------------------------------ consume */

/**
 * Turn a dequeued descriptor into a usable message.
 *
 * Takes ownership on behalf of the calling process: the chunk flips to
 * CONSUMED and gets a fresh lease, so that crashing mid-processing is
 * recoverable instead of leaking the chunk until restart. Returns TY_ERR_STATE
 * when somebody else got there first, which callers treat as "skip this one".
 */
static ty_err_t ty_topic_fill(ty_topic_t* t, uint32_t gid, uint32_t len,
                              uint64_t seq, ty_msg_t* msg) {
    ty_pool_class_t* cls = NULL;
    ty_chunk_t* ch = ty_gid_resolve(t->pool, t->reg, gid, &cls);
    if (!ch) return TY_ERR_INTERNAL;
    if (ch->magic != TY_CHUNK_MAGIC) return TY_ERR_INTERNAL;

    uint32_t expect = TY_CHUNK_PUBLISHED;
    if (!atomic_compare_exchange_strong_explicit(&ch->state, &expect,
                                                 TY_CHUNK_CONSUMED,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return TY_ERR_STATE;
    }

    uint32_t idx = (uint32_t)(gid - cls->gid_base);
    TY_STORE_RELAXED(&ch->owner_pid, (int32_t)getpid());
    TY_STORE_RELAXED(&ch->lease_expire_ns, ty_now_ns() + t->shm->lease_ns);
    ty_atomic_u64* words = (ty_atomic_u64*)TY_PTR(t->reg, cls->bitmap_off);
    atomic_fetch_or_explicit(&words[idx >> 6], (uint64_t)1 << (idx & 63u),
                             memory_order_acq_rel);

    msg->data      = ty_chunk_payload(ch);
    msg->len       = len;
    msg->capacity  = (uint32_t)cls->chunk_size;
    msg->key       = TY_LOAD_RELAXED((ty_atomic_u64*)&ch->reserved1[0]);
    msg->seq       = seq;
    msg->partition = ch->partition;
    return TY_OK;
}

static ty_err_t ty_topic_fetch(ty_topic_t* t, ty_ring_t* ring, ty_msg_t* msg,
                               int64_t timeout_ns) {
    ty_region_t* reg   = t->reg;
    ty_desc_t*   descs = ty_ring_descs(reg, ring);
    uint32_t gid, len;
    uint64_t seq;

    if (ty_ring_dequeue(ring, descs, &gid, &len, &seq) == TY_OK) {
        ty_err_t rc = ty_topic_fill(t, gid, len, seq, msg);
        if (rc == TY_OK) return TY_OK;
    }

    /* Contract from the header: timeout_ns == 0 polls exactly once. */
    if (timeout_ns == 0) return TY_ERR_EMPTY;

    /* Hybrids spin before sleeping: during a burst this is the difference
     * between sub-microsecond and multi-microsecond handoff. */
    if (t->shm->notify_mode != TY_NOTIFY_FUTEX) {
        int rounds = (t->shm->notify_mode == TY_NOTIFY_SPIN) ? 512 : 64;
        if (ty_spin_once(ring, descs, &gid, &len, &seq, rounds, t)) {
            ty_err_t rc = ty_topic_fill(t, gid, len, seq, msg);
            if (rc == TY_OK) return TY_OK;
        }
    }

    uint64_t deadline = (timeout_ns > 0) ? (ty_now_ns() + (uint64_t)timeout_ns) : 0;

    /*
     * ty_ring_dequeue has already advanced deq_pos by the time we try to
     * materialise the chunk, so a fill failure means the descriptor is gone and
     * the message with it. That should be impossible -- the producer flips the
     * chunk to PUBLISHED before its descriptor becomes visible -- so if it
     * happens something is genuinely wrong with the region.
     *
     * The temptation is to `continue` and hope the next one works. Resist it:
     * on a corrupt region that spins forever at 100% CPU while silently eating
     * the backlog. Count the loss, and bail out once it stops looking like a
     * race and starts looking like damage.
     */
    int failed_fills = 0;

    for (;;) {
        ty_err_t rc = ty_wait_for_msg(t, ring, descs, &gid, &len, &seq,
                                      timeout_ns, deadline);
        if (rc == TY_OK) {
            ty_err_t frc = ty_topic_fill(t, gid, len, seq, msg);
            if (frc == TY_OK) return TY_OK;
            TY_FETCH_ADD(&t->shm->dropped, 1);
            if (++failed_fills > 64) return TY_ERR_INTERNAL;
            continue;
        }
        if (rc == TY_ERR_TIMEOUT) return TY_ERR_TIMEOUT;

        if (ty_ring_dequeue(ring, descs, &gid, &len, &seq) == TY_OK) {
            ty_err_t frc = ty_topic_fill(t, gid, len, seq, msg);
            if (frc == TY_OK) return TY_OK;
            TY_FETCH_ADD(&t->shm->dropped, 1);
            if (++failed_fills > 64) return TY_ERR_INTERNAL;
        }
        if (timeout_ns > 0 && (int64_t)(deadline - ty_now_ns()) <= 0) {
            return TY_ERR_TIMEOUT;
        }
    }
}

ty_err_t ty_subscribe(ty_topic_t* t, uint32_t partition, ty_msg_t* msg,
                      int64_t timeout_ns) {
    if (!t || !msg) return TY_ERR_INVAL;
    if (partition >= t->shm->partitions) return TY_ERR_INVAL;
    memset(msg, 0, sizeof(*msg));
    return ty_topic_fetch(t, ty_topic_ring(t, partition), msg, timeout_ns);
}

/**
 * Round-robin across partitions.
 *
 * A single consumer owning every partition cannot park on a single futex,
 * because Linux has no "wait on any of these addresses" primitive. So we sweep
 * with short blocking attempts instead of sleeping indefinitely on one
 * partition -- otherwise a message arriving on partition 5 while we sleep on
 * partition 0 would sit unnoticed until the deadline.
 */
ty_err_t ty_subscribe_any(ty_topic_t* t, uint32_t* hint, ty_msg_t* msg,
                          int64_t timeout_ns) {
    if (!t || !msg) return TY_ERR_INVAL;
    uint32_t  n     = t->shm->partitions;
    uint32_t  start = hint ? (*hint & t->shm->part_mask) : 0;
    uint64_t  deadline = (timeout_ns > 0) ? ty_now_ns() + (uint64_t)timeout_ns : 0;

    memset(msg, 0, sizeof(*msg));

    /* First pass: non-blocking sweep, the common case when data is waiting. */
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t p = (start + i) & t->shm->part_mask;
        if (ty_topic_fetch(t, ty_topic_ring(t, p), msg, 0) == TY_OK) {
            if (hint) *hint = (p + 1) & t->shm->part_mask;
            return TY_OK;
        }
    }
    if (timeout_ns == 0) return TY_ERR_EMPTY;

    for (;;) {
        int64_t slice = 1000000; /* 1ms per partition keeps the round trip sane */

        for (uint32_t i = 0; i < n; ++i) {
            uint32_t p = (start + i) & t->shm->part_mask;
            if (timeout_ns > 0) {
                int64_t left = (int64_t)(deadline - ty_now_ns());
                if (left <= 0) return TY_ERR_TIMEOUT;
                if (slice > left) slice = left;
            }
            ty_err_t rc = ty_topic_fetch(t, ty_topic_ring(t, p), msg, slice);
            if (rc == TY_OK) {
                if (hint) *hint = (p + 1) & t->shm->part_mask;
                return TY_OK;
            }
        }
        start = (start + 1) & t->shm->part_mask;
    }
}

ty_err_t ty_release(ty_topic_t* t, ty_msg_t* msg) {
    if (!t || !msg || !msg->data) return TY_ERR_INVAL;
    ty_chunk_t* ch = ty_payload_chunk(msg->data);
    if (ch->magic != TY_CHUNK_MAGIC) return TY_ERR_INVAL;
    ty_err_t rc = ty_pool_release(t, ch);
    if (rc == TY_OK) {
        msg->data = NULL;
        TY_FETCH_ADD(&t->shm->consumed, 1);
    }
    return rc;
}

/* ----------------------------------------------------------------- plumbing */

ty_err_t ty_depth(ty_topic_t* t, int32_t partition, uint64_t* out) {
    if (!t || !out) return TY_ERR_INVAL;
    if (partition < 0) {
        uint64_t total = 0;
        for (uint32_t i = 0; i < t->shm->partitions; ++i) {
            total += ty_ring_depth(ty_topic_ring(t, i));
        }
        *out = total;
        return TY_OK;
    }
    if ((uint32_t)partition >= t->shm->partitions) return TY_ERR_INVAL;
    *out = ty_ring_depth(ty_topic_ring(t, (uint32_t)partition));
    return TY_OK;
}

ty_err_t ty_wait_room(ty_topic_t* t, uint64_t key, int64_t timeout_ns) {
    /* There is no consumer-side wake-up for a full ring, so waiting for room is
     * a spin. That is deliberate: adding a producer futex would cost a syscall
     * on the publish path, and callers who need room are already in the slow
     * case. Pass a timeout unless you have a core to spare. */
    uint64_t deadline = (timeout_ns > 0) ? ty_now_ns() + (uint64_t)timeout_ns : 0;

    for (;;) {
        ty_ring_t* ring = ty_topic_ring(t, ty_route(t, key));
        uint64_t depth = ty_ring_depth(ring);
        if (depth < (uint64_t)t->shm->capacity) return TY_OK;
        if (timeout_ns == 0) return TY_ERR_FULL;
        if (timeout_ns > 0 && ty_now_ns() >= deadline) break;
        ty_cpu_relax();
    }
    return TY_ERR_TIMEOUT;
}

ty_err_t ty_topic_stats(ty_topic_t* t, ty_stats_t* out) {
    if (!t || !out) return TY_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    out->published    = TY_LOAD_RELAXED(&t->shm->published);
    out->consumed     = TY_LOAD_RELAXED(&t->shm->consumed);
    out->dropped      = TY_LOAD_RELAXED(&t->shm->dropped);
    out->loan_ok      = TY_LOAD_RELAXED(&t->shm->loan_ok);
    out->loan_nomem   = TY_LOAD_RELAXED(&t->shm->loan_nomem);
    out->reclaimed    = TY_LOAD_RELAXED(&t->shm->reclaimed);
    out->wait_wakeups = TY_LOAD_RELAXED(&t->shm->wait_wakeups);
    out->spin_spins   = TY_LOAD_RELAXED(&t->shm->spin_spins);
    return TY_OK;
}

ty_err_t ty_pool_info(ty_topic_t* t, ty_pool_class_info_t* out, size_t n) {
    return ty_pool_class_info(t, out, n);
}

uint64_t ty_reclaim_once(ty_topic_t* t) {
    if (!t) return 0;
    return ty__reclaim_topic(t, ty_now_ns(), 0);
}
