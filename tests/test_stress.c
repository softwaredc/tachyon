/*
 * test_stress.c -- many producers, many consumers, nothing lost or duplicated.
 *
 * Single process, so it can run under ThreadSanitizer. Cross-process behaviour
 * is covered by test_crash and test_chaos instead; this one is about the ring
 * and the pool surviving real concurrency.
 *
 * The assertion that matters is at the end: every message sent is received
 * exactly once. Off-by-one errors in a ring usually show up as a message
 * delivered twice or vanishing at a wrap boundary, which only appears once you
 * push far more messages than the ring has slots.
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

#define PRODUCERS   4
#define CONSUMERS   4
#define PER_PRODUCER 20000
#define TOTAL_MSGS  (PRODUCERS * PER_PRODUCER)
#define PARTITIONS  8
#define CAPACITY    128      /* far fewer slots than messages, so it wraps */

static int failures = 0;
static int checks   = 0;

#define CHECK(cond, ...)                                                    \
    do {                                                                    \
        ++checks;                                                           \
        if (!(cond)) {                                                      \
            ++failures;                                                     \
            printf("  FAIL %s:%d: ", __func__, __LINE__);                   \
            printf(__VA_ARGS__);                                            \
            printf("\n");                                                   \
        }                                                                   \
    } while (0)

static ty_topic_t* g_topic;
static long g_received = 0;
static unsigned char* g_seen;          /* 1 byte per message id */
static long g_duplicates = 0;
static long g_corrupt = 0;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

static void* producer(void* arg) {
    long id = (long)arg;
    unsigned int payload[8];

    for (int i = 0; i < PER_PRODUCER; ++i) {
        /* Encode producer id and sequence so the consumer can verify both the
         * count and the content of what it received. */
        payload[0] = (unsigned int)id;
        payload[1] = (unsigned int)i;
        for (int k = 2; k < 8; ++k) payload[k] = (unsigned int)(i * k + id);

        ty_err_t rc;
        for (;;) {
            rc = ty_send(g_topic, payload, sizeof payload, (uint64_t)(i * 31 + id));
            if (rc == TY_OK) break;
            if (rc == TY_ERR_FULL || rc == TY_ERR_NOMEM) {
                usleep(50);
                continue;
            }
            printf("  producer %ld: unexpected %s\n", id, ty_strerror(rc));
            return NULL;
        }
    }
    return NULL;
}

static void* consumer(void* arg) {
    (void)arg;
    uint32_t hint = 0;

    for (;;) {
        ty_msg_t m;
        ty_err_t rc = ty_subscribe_any(g_topic, &hint, &m, 100000000LL); /* 100ms */
        if (rc != TY_OK) {
            pthread_mutex_lock(&g_lock);
            long done = g_received;
            pthread_mutex_unlock(&g_lock);
            if (done >= TOTAL_MSGS) break;
            continue;
        }

        unsigned int* p = (unsigned int*)m.data;
        int bad = 0;
        if (m.len != 32) bad = 1;
        else {
            unsigned int pid = p[0], seq = p[1];
            for (int k = 2; k < 8; ++k) {
                if (p[k] != (unsigned int)(seq * k + pid)) bad = 1;
            }
            pthread_mutex_lock(&g_lock);
            if (pid >= PRODUCERS || seq >= PER_PRODUCER) {
                bad = 1;
            } else {
                unsigned long idx = (unsigned long)pid * PER_PRODUCER + seq;
                if (g_seen[idx]) g_duplicates++;
                g_seen[idx] = 1;
            }
            if (bad) g_corrupt++;
            g_received++;
            pthread_mutex_unlock(&g_lock);
        }

        ty_release(g_topic, &m);
    }
    return NULL;
}

int main(void) {
    printf("tachyon stress: %d producers x %d msgs, %d consumers, "
           "%d partitions x %d slots\n",
           PRODUCERS, PER_PRODUCER, CONSUMERS, PARTITIONS, CAPACITY);

    ty_region_remove("ty_stress", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 128u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    CHECK(ty_region_open("ty_stress", &rc, &reg) == TY_OK, "region open");

    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = PARTITIONS;
    tc.capacity   = CAPACITY;
    tc.pool_bytes = 64u << 20;
    tc.notify     = TY_NOTIFY_HYBRID;
    CHECK(ty_topic_create(reg, "stream", &tc, &g_topic) == TY_OK, "topic create");

    g_seen = (unsigned char*)calloc(TOTAL_MSGS, 1);
    CHECK(g_seen != NULL, "allocate seen bitmap");

    uint64_t t0 = ty_now_ns();

    pthread_t prod[PRODUCERS], cons[CONSUMERS];
    for (long i = 0; i < PRODUCERS; ++i) pthread_create(&prod[i], NULL, producer, (void*)i);
    for (long i = 0; i < CONSUMERS; ++i) pthread_create(&cons[i], NULL, consumer, (void*)i);

    for (int i = 0; i < PRODUCERS; ++i) pthread_join(prod[i], NULL);
    for (int i = 0; i < CONSUMERS; ++i) pthread_join(cons[i], NULL);

    uint64_t elapsed = ty_now_ns() - t0;

    CHECK(g_received == TOTAL_MSGS, "received %ld of %d", g_received, TOTAL_MSGS);
    CHECK(g_duplicates == 0, "duplicated deliveries: %ld", g_duplicates);
    CHECK(g_corrupt == 0, "corrupted payloads: %ld", g_corrupt);

    long missing = 0;
    for (long i = 0; i < TOTAL_MSGS; ++i) if (!g_seen[i]) missing++;
    CHECK(missing == 0, "missing messages: %ld", missing);

    ty_stats_t st;
    ty_topic_stats(g_topic, &st);
    CHECK(st.published == (uint64_t)TOTAL_MSGS,
          "published %llu of %d", (unsigned long long)st.published, TOTAL_MSGS);

    double secs = (double)elapsed / 1e9;
    printf("  %.3fs elapsed, %.0f msgs/s across %d producers\n",
           secs, (double)TOTAL_MSGS / secs, PRODUCERS);
    printf("  published=%llu consumed=%llu dropped=%llu reclaimed=%llu\n",
           (unsigned long long)st.published, (unsigned long long)st.consumed,
           (unsigned long long)st.dropped, (unsigned long long)st.reclaimed);

    free(g_seen);
    ty_topic_close(g_topic);
    ty_region_close(reg);
    ty_region_remove("ty_stress", NULL);

    printf("\n%d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
