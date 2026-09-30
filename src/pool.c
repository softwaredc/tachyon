/*
 * pool.c -- size-classed chunk allocator living inside the shared region.
 *
 * Why size classes instead of one flat allocator: a flat free list either
 * fragments or needs a concurrent malloc, and neither is acceptable on the
 * hot path of an IPC library. Eight power-of-two-ish classes cover the whole
 * realistic message distribution, waste is bounded by the class stride, and
 * allocation is a single CAS.
 *
 * The free list is a Treiber stack with a 32-bit generation tag packed into the
 * high half of the head word. That is what stops ABA: for two poppers to both
 * succeed on the same node somebody has to complete exactly 2^32 intervening
 * operations, which we accept as "never". A hazard-pointer scheme would be
 * strictly stronger but costs to 3x on every operation -- an honest trade that
 * is recorded here rather than papered over.
 *
 * Every outstanding lease is also marked in a per-class bitmap. The bitmap is
 * what makes crash recovery cheap: instead of walking every chunk, the reaper
 * reads bitmap words and only inspects chunks that somebody actually owns.
 */
#include "internal.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* --------------------------------------------------------------- bitmap ops */

static inline _Atomic uint64_t* ty_bitmap_words(ty_region_t* reg, uint64_t off) {
    return (_Atomic uint64_t*)TY_PTR(reg, off);
}

static inline void ty_bitmap_set(ty_region_t* reg, uint64_t off, uint32_t idx) {
    ty_atomic_u64* w = ty_bitmap_words(reg, off);
    atomic_fetch_or_explicit(&w[idx >> 6], (uint64_t)1 << (idx & 63u),
                             memory_order_acq_rel);
}

static inline void ty_bitmap_clear(ty_region_t* reg, uint64_t off, uint32_t idx) {
    ty_atomic_u64* w = ty_bitmap_words(reg, off);
    atomic_fetch_and_explicit(&w[idx >> 6], ~((uint64_t)1 << (idx & 63u)),
                              memory_order_acq_rel);
}

/* ----------------------------------------------------------- free list ops */

static uint32_t ty_pool_pop(ty_pool_class_t* cls, ty_region_t* reg) {
    uint64_t head = TY_LOAD_ACQUIRE(&cls->free_head);
    for (;;) {
        uint32_t idx = ty_tag_idx(head);
        if (idx == TY_NIL_IDX) return TY_NIL_IDX;

        ty_chunk_t* ch = ty_chunk_addr(cls, reg, idx);
        uint32_t next  = TY_LOAD_RELAXED(&ch->next);
        uint64_t newh  = ty_pack_tag(next, ty_tag_gen(head) + 1u);

        if (TY_CAS_WEAK(&cls->free_head, &head, newh)) {
            TY_STORE_RELAXED(&ch->next, TY_NIL_IDX);
            return idx;
        }
        /* Head moved under us; `head` now holds the fresh value. */
    }
}

static void ty_pool_push(ty_pool_class_t* cls, ty_region_t* reg, uint32_t idx) {
    ty_chunk_t* ch = ty_chunk_addr(cls, reg, idx);
    uint64_t head  = TY_LOAD_RELAXED(&cls->free_head);
    uint64_t newh;
    do {
        TY_STORE_RELAXED(&ch->next, ty_tag_idx(head));
        newh = ty_pack_tag(idx, ty_tag_gen(head) + 1u);
    } while (!TY_CAS_WEAK(&cls->free_head, &head, newh));
}

/* -------------------------------------------------------------------- init */

int ty_pool_init(ty_region_t* reg, ty_pool_t* pool, size_t payload_bytes,
                 uint64_t* cur_off, uint64_t region_limit) {
    /* Split the payload budget evenly across classes. Simple, predictable, and
     * documented: workloads with a known size mix should size the topic for
     * the classes they actually use. */
    size_t   per_class = payload_bytes / TY_SIZE_CLASSES;
    uint64_t gid_base  = 0;

    for (uint32_t c = 0; c < TY_SIZE_CLASSES; ++c) {
        ty_pool_class_t* cls = &pool->classes[c];
        uint64_t sz     = ty_size_classes[c];
        uint64_t stride = TY_CHUNK_HDR_SZ + sz;
        uint64_t count  = (uint64_t)(per_class / stride);
        if (count == 0) count = (c < 3) ? 4 : 1; /* small classes always usable */
        if (count > (uint64_t)TY_NIL_IDX - 1) count = (uint64_t)TY_NIL_IDX - 1;

        uint64_t chunks_bytes = count * stride;
        uint64_t chunks_off   = TY_ALIGN64(*cur_off);
        if (chunks_off + chunks_bytes > region_limit) {
            count = (region_limit - chunks_off) / stride;
            chunks_bytes = count * stride;
            if (count == 0) return TY_ERR_NOMEM;
        }

        uint64_t words   = (count + 63) / 64 + 1;
        uint64_t bmp_off = TY_ALIGN64(chunks_off + chunks_bytes);
        if (bmp_off + words * 8 > region_limit) return TY_ERR_NOMEM;

        memset(TY_PTR(reg, chunks_off), 0, (size_t)chunks_bytes);
        memset(TY_PTR(reg, bmp_off), 0, (size_t)(words * 8));

        atomic_store(&cls->free_head, 0);
        cls->chunk_size  = sz;
        cls->chunk_stride= stride;
        cls->chunk_count = count;
        cls->chunks_off  = chunks_off;
        cls->bitmap_off  = bmp_off;
        cls->bitmap_words= words;
        cls->gid_base    = gid_base;
        gid_base        += count;

        /* Link every chunk into the free list, back to front so index 0 ends
         * up popped last (cosmetic, but makes debugging dumps readable). */
        for (uint64_t i = 0; i < count; ++i) {
            ty_chunk_t* ch = ty_chunk_addr(cls, reg, (uint32_t)i);
            ch->magic     = TY_CHUNK_MAGIC;
            atomic_store(&ch->state, TY_CHUNK_FREE);
            ch->class_id  = c;
            atomic_store(&ch->next, (i + 1 < count) ? (uint32_t)(i + 1) : TY_NIL_IDX);
            atomic_store(&ch->owner_pid, 0);
            atomic_store(&ch->lease_expire_ns, 0);
            atomic_store(&ch->seq, 0);
        }
        uint64_t head = ty_pack_tag(0u, 1u);
        atomic_store_explicit(&cls->free_head, head, memory_order_release);

        *cur_off = bmp_off + words * 8;
    }

    return TY_OK;
}

/* -------------------------------------------------------------------- loan */

ty_err_t ty_pool_loan(ty_topic_t* topic, size_t size, void** out, size_t* cap) {
    if (!topic || !out) return TY_ERR_INVAL;
    if (size > TACHYON_MAX_MSG_SIZE) return TY_ERR_TOOBIG;
    if (size == 0) size = 1;

    ty_region_t* reg = topic->reg;
    ty_pool_t*   pool = topic->pool;

    uint32_t first = ty_class_for(size);
    uint32_t idx   = TY_NIL_IDX;
    uint32_t cls_i = first;

    /* Try the exact class first, then walk upward. Borrowing a bigger class
     * costs a little memory but keeps throughput up when one size bucket is
     * hot -- better than failing a loan outright. */
    for (uint32_t c = first; c < TY_SIZE_CLASSES; ++c) {
        idx = ty_pool_pop(&pool->classes[c], reg);
        if (idx != TY_NIL_IDX) { cls_i = c; break; }
    }
    if (idx == TY_NIL_IDX) {
        TY_FETCH_ADD(&topic->shm->loan_nomem, 1);
        return TY_ERR_NOMEM;
    }

    ty_pool_class_t* cls = &pool->classes[cls_i];
    ty_chunk_t*      ch  = ty_chunk_addr(cls, reg, idx);

    if (ch->magic != TY_CHUNK_MAGIC) return TY_ERR_INTERNAL;

    uint32_t expect = TY_CHUNK_FREE;
    if (!atomic_compare_exchange_strong_explicit(&ch->state, &expect,
                                                 TY_CHUNK_LOANED,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        /* Another reaper handed this chunk out at the same instant. Give it
         * back and let the caller decide whether to retry. */
        ty_pool_push(cls, reg, idx);
        return TY_ERR_BUSY;
    }

    uint64_t now = ty_now_ns();
    ch->size          = (uint32_t)size;
    ch->class_id      = cls_i;
    ch->partition     = 0xFFFFFFFFu;
    TY_STORE_RELAXED(&ch->owner_pid, (int32_t)getpid());
    TY_STORE_RELAXED(&ch->lease_expire_ns, now + topic->shm->lease_ns);
    ty_bitmap_set(reg, cls->bitmap_off, idx);

    TY_FETCH_ADD(&topic->shm->loan_ok, 1);
    TY_FETCH_ADD(&topic->shm->bytes_in_use, cls->chunk_size);

    *out = ty_chunk_payload(ch);
    if (cap) *cap = (size_t)cls->chunk_size;
    return TY_OK;
}

/* Return a loaned chunk that was never published. */
ty_err_t ty_pool_abort(ty_topic_t* topic, void* ptr) {
    if (!topic || !ptr) return TY_ERR_INVAL;
    ty_chunk_t* ch = ty_payload_chunk(ptr);
    if (ch->magic != TY_CHUNK_MAGIC) return TY_ERR_INVAL;

    /* class_id is read out of shared memory, so it is untrusted: a corrupt or
     * foreign pointer must be rejected rather than used as an array index. */
    if (ch->class_id >= TY_SIZE_CLASSES) return TY_ERR_INVAL;

    ty_region_t*     reg = topic->reg;
    ty_pool_class_t* cls = &topic->pool->classes[ch->class_id];
    uint32_t idx = ty_class_idx_of(cls, reg, ch);

    uint32_t expect = TY_CHUNK_LOANED;
    if (!atomic_compare_exchange_strong_explicit(&ch->state, &expect,
                                                 TY_CHUNK_FREE,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return TY_ERR_STATE;
    }
    ty_bitmap_clear(reg, cls->bitmap_off, idx);
    TY_STORE_RELAXED(&ch->owner_pid, 0);
    TY_STORE_RELAXED(&ch->lease_expire_ns, 0);
    TY_FETCH_SUB(&topic->shm->bytes_in_use, cls->chunk_size);
    ty_pool_push(cls, reg, idx);
    return TY_OK;
}

/* Hand a consumed chunk back to a free list. The common path is one CAS. */
ty_err_t ty_pool_release(ty_topic_t* topic, ty_chunk_t* ch) {
    if (ch->class_id >= TY_SIZE_CLASSES) return TY_ERR_INVAL;

    ty_region_t*     reg = topic->reg;
    ty_pool_class_t* cls = &topic->pool->classes[ch->class_id];
    uint32_t         idx = ty_class_idx_of(cls, reg, ch);

    uint32_t expect = TY_CHUNK_CONSUMED;
    if (!atomic_compare_exchange_strong_explicit(&ch->state, &expect,
                                                 TY_CHUNK_FREE,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return TY_ERR_STATE;
    }
    ty_bitmap_clear(reg, cls->bitmap_off, idx);
    TY_STORE_RELAXED(&ch->owner_pid, 0);
    TY_STORE_RELAXED(&ch->lease_expire_ns, 0);
    TY_FETCH_SUB(&topic->shm->bytes_in_use, cls->chunk_size);
    ty_pool_push(cls, reg, idx);
    return TY_OK;
}

/* ------------------------------------------------------------------- query */

ty_err_t ty_pool_class_info(ty_topic_t* topic, ty_pool_class_info_t* out, size_t n) {
    if (!topic || !out) return TY_ERR_INVAL;
    size_t count = n < TY_SIZE_CLASSES ? n : TY_SIZE_CLASSES;
    for (size_t i = 0; i < count; ++i) {
        ty_pool_class_t* cls = &topic->pool->classes[i];
        out[i].chunk_size  = cls->chunk_size;
        out[i].chunk_count = cls->chunk_count;
        out[i].free_count  = 0;
        /* Walk the list once; called for diagnostics and never on the hot path. */
        uint32_t idx = ty_tag_idx(TY_LOAD_RELAXED(&cls->free_head));
        uint64_t k = 0;
        while (idx != TY_NIL_IDX && k < cls->chunk_count + 8) {
            ++k;
            idx = TY_LOAD_RELAXED(&ty_chunk_addr(cls, topic->reg, idx)->next);
        }
        out[i].free_count = k;
    }
    return TY_OK;
}
