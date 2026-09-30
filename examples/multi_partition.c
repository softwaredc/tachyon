/*
 * multi_partition.c -- one consumer process per partition.
 *
 * This is the horizontal scaling story in one file: a key always hashes to the
 * same partition, so each consumer owns a disjoint slice of the key space and
 * the four of them run without touching each other's cache lines.
 *
 * The parent produces 40k messages across 4 partitions; four children drain
 * one partition each. The final table shows per-partition throughput, which is
 * what you actually want to look at when sizing a service.
 */

/* usleep() is POSIX, not C11; without this it is undeclared under -std=c11. */
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

#define PARTITIONS 4u
#define PER_CONSUMER 10000

static void check(const char* what, ty_err_t err) {
    if (err != TY_OK) {
        fprintf(stderr, "%s failed: %s\n", what, ty_strerror(err));
        exit(1);
    }
}

static uint64_t run_consumer(unsigned part) {
    ty_region_t* reg = NULL;
    ty_region_conf_t cc = ty_region_conf_default;
    cc.create       = 0;
    cc.spawn_reaper = 0;
    check("attach", ty_region_open("mpart", &cc, &reg));

    ty_topic_t* t = NULL;
    check("topic", ty_topic_open(reg, "events", &t));

    uint64_t got = 0, sum = 0;
    uint64_t t0 = ty_now_ns();
    while (got < PER_CONSUMER) {
        ty_msg_t m;
        ty_err_t r = ty_subscribe(t, part, &m, 5000000000LL);
        if (r != TY_OK) break;
        /* Cheap checksum so the compiler cannot elide the read. */
        sum += (uint64_t)m.len + (uint64_t)((const unsigned char*)m.data)[0];
        ty_release(t, &m);
        ++got;
    }
    uint64_t elapsed = ty_now_ns() - t0;
    (void)sum;

    printf("  partition %u: %llu msgs in %.1f ms (%.0f msg/s)\n",
           part, (unsigned long long)got, elapsed / 1e6,
           (double)got / ((double)elapsed / 1e9));

    ty_region_close(reg);
    return got;
}

int main(void) {
    ty_region_remove("mpart", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 64u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    check("region_open", ty_region_open("mpart", &rc, &reg));

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = PARTITIONS;
    tc.capacity   = 4096;
    tc.pool_bytes = 32u << 20;
    tc.notify     = TY_NOTIFY_HYBRID;
    check("topic_create", ty_topic_create(reg, "events", &tc, &t));

    printf("topic has %u partitions; routing samples:\n", ty_topic_partitions(t));
    for (uint64_t k = 0; k < 6; ++k)
        printf("  key %llu -> partition %u\n", (unsigned long long)k,
               ty_route(t, k));

    pid_t kids[PARTITIONS];
    for (unsigned p = 0; p < PARTITIONS; ++p) {
        pid_t pid = fork();
        if (pid == 0) {
            run_consumer(p);
            fflush(stdout);
            _exit(0);
        }
        kids[p] = pid;
    }

    usleep(100000); /* let the children attach */

    char buf[128];
    memset(buf, 'e', sizeof buf);

    /* The hash does not split keys perfectly evenly, so we do not guess: we
     * walk keys once and bucket them by the partition they actually route to.
     * Every consumer then gets exactly PER_CONSUMER messages and the numbers
     * below add up without leftovers. */
    uint64_t* keys[PARTITIONS];
    uint64_t  nkeys[PARTITIONS];
    for (unsigned p = 0; p < PARTITIONS; ++p) {
        keys[p]  = (uint64_t*)malloc(PER_CONSUMER * sizeof(uint64_t));
        nkeys[p] = 0;
    }
    for (uint64_t k = 0;; ++k) {
        unsigned p = ty_route(t, k);
        if (nkeys[p] < PER_CONSUMER) keys[p][nkeys[p]++] = k;
        int full = 1;
        for (unsigned q = 0; q < PARTITIONS; ++q)
            if (nkeys[q] < PER_CONSUMER) full = 0;
        if (full) break;
    }

    uint64_t t0 = ty_now_ns();
    uint64_t sent = 0;
    /* Interleave the partitions so all four consumers are busy at once. */
    for (uint64_t i = 0; i < PER_CONSUMER; ++i) {
        for (unsigned p = 0; p < PARTITIONS; ++p) {
            uint64_t key = keys[p][i];
            for (;;) {
                ty_err_t r = ty_send(t, buf, sizeof buf, key);
                if (r == TY_OK) { ++sent; break; }
                if (r == TY_ERR_FULL || r == TY_ERR_NOMEM) {
                    ty_wait_room(t, key, 1000000LL);
                    continue;
                }
                fprintf(stderr, "send failed: %s\n", ty_strerror(r));
                return 1;
            }
        }
    }
    uint64_t elapsed = ty_now_ns() - t0;
    for (unsigned p = 0; p < PARTITIONS; ++p) free(keys[p]);

    printf("producer: %llu msgs in %.1f ms (%.0f msg/s, %.2f GB/s)\n",
           (unsigned long long)sent, elapsed / 1e6,
           (double)sent / ((double)elapsed / 1e9),
           (double)(sent * sizeof buf) / ((double)elapsed / 1e9) / 1e9);

    for (unsigned p = 0; p < PARTITIONS; ++p) {
        int st = 0;
        waitpid(kids[p], &st, 0);
    }

    ty_stats_t s;
    ty_topic_stats(t, &s);
    printf("topic totals: published=%llu consumed=%llu dropped=%llu\n",
           (unsigned long long)s.published, (unsigned long long)s.consumed,
           (unsigned long long)s.dropped);

    ty_topic_close(t);
    ty_region_close(reg);
    ty_region_remove("mpart", NULL);
    return 0;
}
