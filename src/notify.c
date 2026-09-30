/*
 * notify.c -- how a consumer learns that a partition stopped being empty.
 *
 * Three policies, all available per topic:
 *
 *   SPIN    never sleeps. ~100ns handoff, one whole core burned per waiter.
 *   FUTEX   always sleeps. Zero idle CPU, ~2-5us wake-up because of the
 *           kernel round trip.
 *   HYBRID  spins briefly, then sleeps. The default, because workloads are
 *           bursty: during a burst you get spin latency, during a lull you
 *           pay no CPU.
 *
 * The subtlety worth reading twice is the lost-wakeup race.
 *
 *   waiter:    waiters++ --fence--> recheck queue --fence--> futex_wait
 *   producer:  enqueue   --fence--> read waiters --> maybe futex_wake
 *
 * Because the recheck happens AFTER registration, a producer that sampled
 * waiters==0 must have enqueued before our increment, so our recheck cannot
 * miss its message. A producer that sampled >=1 guarantees a wake. Omit the
 * recheck and you deadlock once every few billion messages -- a bug that only
 * reproduces in production at 3am. That is why the wait function below owns
 * the recheck rather than delegating it to the caller.
 *
 * The second optimisation: skip the syscall entirely when nobody waits. On a
 * saturated stream this alone dominates throughput.
 */
#include "internal.h"

#include <errno.h>
#include <stdlib.h>

#define TY_SPIN_ROUNDS 128

static inline uint32_t ty_wait_arm(ty_ring_t* ring) {
    atomic_fetch_add_explicit(&ring->waiters, 1u, memory_order_acq_rel);
    return atomic_load_explicit(&ring->futex_word, memory_order_acquire);
}

static inline void ty_wait_disarm(ty_ring_t* ring) {
    atomic_fetch_sub_explicit(&ring->waiters, 1u, memory_order_acq_rel);
}

void ty_ring_wake(ty_topic_t* topic, ty_ring_t* ring) {
    /* Cheap read first: in the common case nobody sleeps, so we skip two
     * syscalls. Missing a wake here is impossible thanks to the ordering
     * documented at the top of this file. */
    if (TY_LOAD_RELAXED(&ring->waiters) == 0) return;

    atomic_fetch_add_explicit(&ring->futex_word, 1u, memory_order_release);
    ty__futex_wake(&ring->futex_word, 1u);
    TY_FETCH_ADD(&topic->shm->wait_wakeups, 1);
}

/** Busy-wait `rounds` times, returning 1 as soon as a message lands. */
int ty_spin_once(ty_ring_t* ring, ty_desc_t* descs, uint32_t* idx, uint32_t* len,
                 uint64_t* seq, int rounds, ty_topic_t* topic) {
    for (int i = 0; i < rounds; ++i) {
        if (ty_ring_dequeue(ring, descs, idx, len, seq) == TY_OK) {
            TY_FETCH_ADD(&topic->shm->spin_spins, (uint64_t)(i + 1));
            return 1;
        }
        for (int r = 0; r < 3; ++r) ty_cpu_relax();
    }
    return 0;
}

/**
 * Register, recheck, then block. Owns all three steps because splitting them
 * across functions is exactly how lost wakeups get introduced.
 *
 * @return TY_OK when the recheck found and dequeued a message,
 *         TY_ERR_EMPTY when the caller should spin/retry,
 *         TY_ERR_TIMEOUT when the deadline passed.
 */
ty_err_t ty_wait_for_msg(ty_topic_t* topic, ty_ring_t* ring, ty_desc_t* descs,
                         uint32_t* idx, uint32_t* len, uint64_t* seq,
                         int64_t timeout_ns, uint64_t deadline_ns) {
    if (topic->shm->notify_mode == TY_NOTIFY_SPIN) {
        /* The caller's outer loop owns the retry; just keep the core civil. */
        for (int i = 0; i < 8; ++i) ty_cpu_relax();
        if (timeout_ns > 0 && (int64_t)(deadline_ns - ty_now_ns()) <= 0) {
            return TY_ERR_TIMEOUT;
        }
        return TY_ERR_EMPTY;
    }

    int64_t remaining = -1;
    if (timeout_ns > 0) {
        remaining = (int64_t)(deadline_ns - ty_now_ns());
        if (remaining <= 0) return TY_ERR_TIMEOUT;
    }

    uint32_t gen = ty_wait_arm(ring);

    /* Protocol step 2. If this hits we must disarm before returning, or the
     * producer starts doing pointless syscalls on our behalf forever. */
    if (ty_ring_dequeue(ring, descs, idx, len, seq) == TY_OK) {
        ty_wait_disarm(ring);
        return TY_OK;
    }

    int rc = ty__futex_wait(&ring->futex_word, gen, remaining);
    ty_wait_disarm(ring);

    if (rc == ETIMEDOUT) return TY_ERR_TIMEOUT;
    return TY_ERR_EMPTY; /* EAGAIN / EINTR / spurious wake: caller retries */
}
