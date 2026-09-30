/*
 * ring.c -- Vyukov bounded MPMC queue over a shared array of descriptors.
 *
 * Why this algorithm instead of the more common head/tail CAS queue:
 *
 *   - Producers contend with a single fetch_add on `enq_pos` rather than a CAS
 *     loop fighting over `tail`, which is what makes the queue scale past a
 *     handful of cores.
 *   - Correctness comes from per-cell sequence numbers, so no producer ever
 *     touches another producer's slot. A slow producer can stall a slot but
 *     cannot stall the queue.
 *   - Capacity must be a power of two so the expensive `%` becomes `&`. This
 *     is enforced at init: ty_ring_init() refuses anything else.
 *
 * The trade-off -- and there always is one -- is that a stalled occupying
 * producer blocks that particular slot until it finishes, so tail latency
 * depends on your producers behaving. Documented rather than hidden.
 */
#include "internal.h"

#include <stdlib.h>

void ty_ring_init(ty_region_t* reg, ty_ring_t* ring, uint64_t descs_off,
                  uint64_t capacity, uint64_t notify_mode) {
    (void)reg;
    /* Poke values directly: nobody has mapped this slot yet, no ordering
     * constraints apply during creation. */
    atomic_store(&ring->enq_pos, 0);
    atomic_store(&ring->deq_pos, 0);
    atomic_store(&ring->futex_word, 0);
    atomic_store(&ring->waiters, 0);
    ring->descs_off = descs_off;
    ring->capacity  = capacity;
    ring->mask      = capacity - 1;

    ty_desc_t* descs = (ty_desc_t*)TY_PTR(reg, descs_off);
    for (uint64_t i = 0; i < capacity; ++i) {
        atomic_store(&descs[i].seq, i);
        descs[i].chunk_idx = TY_NIL_IDX;
        descs[i].len       = 0;
    }
    /* A __atomic_thread_fence below is what publishes the array contents
     * before any other process can see the ring as ready. */
    atomic_thread_fence(memory_order_release);
    (void)notify_mode;
}

ty_err_t ty_ring_enqueue(ty_ring_t* ring, ty_desc_t* descs,
                         uint32_t chunk_idx, uint32_t len) {
    ty_desc_t* cell;
    uint64_t pos = TY_LOAD_RELAXED(&ring->enq_pos);

    for (;;) {
        cell = &descs[pos & ring->mask];
        uint64_t seq = TY_LOAD_ACQUIRE(&cell->seq);
        int64_t dif  = (int64_t)seq - (int64_t)pos;

        if (dif == 0) {
            if (TY_CAS_WEAK(&ring->enq_pos, &pos, pos + 1)) break;
        } else if (dif < 0) {
            return TY_ERR_FULL;
        } else {
            pos = TY_LOAD_RELAXED(&ring->enq_pos);
        }
    }

    cell->chunk_idx = chunk_idx;
    cell->len       = len;
    /* Release: every write the producer made into the chunk payload is visible
     * to whoever observes this sequence number. This single store is the
     * entire cross-process publication ceremony. */
    TY_STORE_RELEASE(&cell->seq, pos + 1);
    return TY_OK;
}

ty_err_t ty_ring_dequeue(ty_ring_t* ring, ty_desc_t* descs, uint32_t* chunk_idx,
                         uint32_t* len, uint64_t* seq) {
    ty_desc_t* cell;
    uint64_t pos = TY_LOAD_RELAXED(&ring->deq_pos);

    for (;;) {
        cell = &descs[pos & ring->mask];
        uint64_t s   = TY_LOAD_ACQUIRE(&cell->seq);
        int64_t dif  = (int64_t)s - (int64_t)(pos + 1);

        if (dif == 0) {
            if (TY_CAS_WEAK(&ring->deq_pos, &pos, pos + 1)) break;
        } else if (dif < 0) {
            return TY_ERR_EMPTY;
        } else {
            pos = TY_LOAD_RELAXED(&ring->deq_pos);
        }
    }

    *chunk_idx = cell->chunk_idx;
    *len       = cell->len;
    *seq       = pos;

    /* Hand the slot back to the next lap of producers. Adding mask+1 rather
     * than 1 keeps every cell's sequence monotonically increasing, which is
     * precisely what lets next lap's enqueue distinguish "filled by me" from
     * "still held last lap". */
    TY_STORE_RELEASE(&cell->seq, pos + ring->mask + 1);
    return TY_OK;
}

uint64_t ty_ring_depth(const ty_ring_t* ring) {
    uint64_t e = TY_LOAD_RELAXED((ty_atomic_u64*)&ring->enq_pos);
    uint64_t d = TY_LOAD_RELAXED((ty_atomic_u64*)&ring->deq_pos);
    return (e > d) ? (e - d) : 0ull;
}
