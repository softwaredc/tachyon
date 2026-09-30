/*
 * reaper.c -- recovering chunks abandoned by crashed processes.
 *
 * This file is the reason tachyon is usable in production rather than merely
 * fast. A process can be SIGKILLed at any instruction, and there is no
 * `finally` in C: no atexit hook runs, no destructor fires, nothing cleans up.
 * The loan happens to be outstanding at exactly that instant, and the chunk
 * would be lost forever -- the pool slowly bleeds until every publish returns
 * NOMEM, hours later, under load, for reasons nobody can see.
 *
 * So ownership is leased, not held:
 *
 *   loan marks lease_expire_ns = now + lease_ns and records owner_pid
 *   publish/release clears the lease
 *   the reaper reclaims whatever is still leased past its deadline
 *
 * A per-class bitmap tells us which chunks are worth inspecting, so a sweep
 * costs one pass over candidate slots rather than a pass over the whole pool.
 *
 * Two independent facts make reclaiming safe:
 *   1. the owner process is provably gone (kill(pid,0) == ESRCH), or
 *   2. the lease expired AND the owner's heartbeat stopped advancing.
 *
 * Case 1 shortcuts the wait: a dead process needs no grace period, its chunk
 * can go back immediately. Case 2 protects the honest-but-slow producer that
 * is blocked in a syscall for longer than lease_ns.
 *
 * Note the lease is NOT wall-clock bounded for a *live, heartbeating* process.
 * If you loan and then sit on a chunk for a minute while pumping heartbeats,
 * the reaper leaves you alone -- deliberately, because reclaiming a chunk that
 * somebody is about to publish would corrupt live data. Set lease_ns to bound
 * how long that can last for your workload.
 */
#include "internal.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

/**
 * Is this owner still making progress? Only meaningful for registered
 * processes: an unregistered one cannot prove liveness, so we answer "no" and
 * let the lease deadline be the sole arbiter. Table-full situations land here,
 * which is documented in the README rather than silently swallowed.
 */
static int ty_owner_progressing(ty_region_t* reg, int pid, uint64_t now_ns) {
    ty_proc_slot_t* slots = (ty_proc_slot_t*)TY_PTR(reg, TY_PROCS_OFFSET);
    for (int i = 0; i < TACHYON_MAX_PROCS; ++i) {
        if (TY_LOAD_RELAXED(&slots[i].state) != TY_PROC_LIVE) continue;
        if (TY_LOAD_RELAXED(&slots[i].pid) != pid) continue;

        uint64_t last  = TY_LOAD_RELAXED(&slots[i].heartbeat_ns);
        uint64_t grace = TY_LOAD_RELAXED(&reg->hdr->lease_ns);
        return (now_ns - last) < grace;
    }
    return 0;
}

/**
 * The single decision that makes crash recovery safe. Reclaim only when we can
 * prove nobody still intends to use this chunk:
 *
 *   no owner recorded      -> reclaim
 *   owner provably gone    -> reclaim immediately, no grace period needed
 *   lease not expired      -> leave it alone, the owner is working
 *   lease expired, owner's heartbeat still advancing -> leave it alone, it is
 *                            merely blocked longer than we expected
 *   lease expired, heartbeat stale -> reclaim
 */
static int ty_can_reclaim(ty_region_t* reg, int pid, uint64_t expiry,
                          uint64_t now_ns) {
    if (pid <= 0) return 1;
    if (!ty__pid_alive(pid)) return 1;
    if ((int64_t)(now_ns - expiry) < 0) return 0;
    return !ty_owner_progressing(reg, pid, now_ns);
}

/**
 * Sweep one topic for abandoned chunks.
 *
 * Exactly one caller does work at a time: `reclaim_lock` is a trylock, so a
 * second reaper in another process observes contention and goes back to sleep
 * instead of duplicating work (and double-freeing chunks).
 */
uint64_t ty__reclaim_topic(ty_topic_t* topic, uint64_t now_ns, int dry_run) {
    if (!topic || !topic->shm) return 0;

    uint32_t expect = 0;
    if (!atomic_compare_exchange_strong_explicit(&topic->shm->reclaim_lock, &expect,
                                                 1u, memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return 0; /* somebody else is sweeping right now */
    }

    ty_region_t* reg  = topic->reg;
    ty_pool_t*   pool = topic->pool;
    uint64_t     found = 0;

    for (uint32_t c = 0; c < TY_SIZE_CLASSES; ++c) {
        ty_pool_class_t* cls   = &pool->classes[c];
        ty_atomic_u64*   words = (ty_atomic_u64*)TY_PTR(reg, cls->bitmap_off);

        for (uint64_t w = 0; w < cls->bitmap_words; ++w) {
            uint64_t word = TY_LOAD_RELAXED(&words[w]);
            while (word) {
                int b = __builtin_ctzll(word);
                word &= (word - 1);

                uint32_t idx = (uint32_t)(w * 64 + (uint64_t)b);
                if (idx >= cls->chunk_count) continue;

                ty_chunk_t* ch = ty_chunk_addr(cls, reg, idx);
                if (ch->magic != TY_CHUNK_MAGIC) continue;

                uint32_t st = TY_LOAD_ACQUIRE(&ch->state);
                /* PUBLISHED chunks are owned by the ring, not by anybody who
                 * can crash -- they are waiting to be consumed, which is a
                 * legitimate indefinite state. Leave them alone. */
                if (st != TY_CHUNK_LOANED && st != TY_CHUNK_CONSUMED) continue;

                int32_t  owner   = TY_LOAD_RELAXED(&ch->owner_pid);
                uint64_t expiry  = TY_LOAD_RELAXED(&ch->lease_expire_ns);

                if (!ty_can_reclaim(reg, owner, expiry, now_ns)) continue;

                if (dry_run) { ++found; continue; }

                if (!atomic_compare_exchange_strong_explicit(
                        &ch->state, &st, TY_CHUNK_FREE,
                        memory_order_acq_rel, memory_order_relaxed)) {
                    continue; /* state moved; the owner finished its work */
                }

                atomic_fetch_and_explicit(&words[w],
                                          ~((uint64_t)1 << (uint64_t)b),
                                          memory_order_acq_rel);
                TY_STORE_RELAXED(&ch->owner_pid, 0);
                TY_STORE_RELAXED(&ch->lease_expire_ns, 0);
                TY_FETCH_SUB(&topic->shm->bytes_in_use, cls->chunk_size);

                /* Push back onto the free list through the same tagged CAS the
                 * fast path uses. */
                uint64_t head = TY_LOAD_RELAXED(&cls->free_head);
                uint64_t newh;
                do {
                    TY_STORE_RELAXED(&ch->next, ty_tag_idx(head));
                    newh = ty_pack_tag(idx, ty_tag_gen(head) + 1u);
                } while (!TY_CAS_WEAK(&cls->free_head, &head, newh));

                TY_FETCH_ADD(&topic->shm->reclaimed, 1);
                ++found;
            }
        }
    }

    atomic_store_explicit(&topic->shm->reclaim_lock, 0u, memory_order_release);
    TY_STORE_RELAXED(&topic->shm->last_reclaim_ns, now_ns);
    return found;
}
