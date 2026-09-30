/*
 * key_ordering.c -- prove the one ordering guarantee tachyon actually makes.
 *
 * Contract: messages sharing a key land in the same partition and are
 * delivered in publish order. Messages with different keys may interleave,
 * because they live in different rings and are consumed independently.
 *
 * This example publishes sequence numbers 0..N-1 under four distinct keys,
 * then verifies that per key the sequence numbers arrive strictly increasing.
 * If the invariant ever breaks, it prints the offending pair and exits 1.
 *
 * Run it in CI. It is the cheapest way to catch a routing regression.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

#define KEYS      4
#define PER_KEY   5000

static void check(const char* what, ty_err_t err) {
    if (err != TY_OK) {
        fprintf(stderr, "%s failed: %s\n", what, ty_strerror(err));
        exit(1);
    }
}

typedef struct {
    uint64_t key;
    uint64_t seq;
} envelope;

int main(void) {
    ty_region_remove("keyorder", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    check("region_open", ty_region_open("keyorder", &rc, &reg));

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 8;                 /* more partitions than keys on purpose */
    tc.capacity   = 4096;
    tc.pool_bytes = 16u << 20;
    tc.notify     = TY_NOTIFY_HYBRID;
    check("topic_create", ty_topic_create(reg, "ordered", &tc, &t));

    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create       = 0;
        cc.spawn_reaper = 0;
        check("child attach", ty_region_open("keyorder", &cc, &creg));
        ty_topic_t* ct = NULL;
        check("child topic", ty_topic_open(creg, "ordered", &ct));

        uint64_t last[KEYS];
        uint64_t seen[KEYS];
        for (int i = 0; i < KEYS; ++i) { last[i] = UINT64_MAX; seen[i] = 0; }

        uint64_t total = 0;
        uint64_t want  = (uint64_t)KEYS * PER_KEY;
        uint32_t hint  = 0;

        while (total < want) {
            ty_msg_t m;
            ty_err_t r = ty_subscribe_any(ct, &hint, &m, 5000000000LL);
            if (r != TY_OK) {
                fprintf(stderr, "child: gave up after %llu/%llu (%s)\n",
                        (unsigned long long)total, (unsigned long long)want,
                        ty_strerror(r));
                fflush(stdout);
                _exit(1);
            }
            envelope e;
            memcpy(&e, m.data, sizeof e);
            ty_release(ct, &m);
            ++total;

            if (e.key >= (uint64_t)KEYS) {
                fprintf(stderr, "child: bogus key %llu\n",
                        (unsigned long long)e.key);
                fflush(stdout);
                _exit(1);
            }
            if (seen[e.key] && e.seq <= last[e.key]) {
                fprintf(stderr,
                        "ORDER VIOLATION key=%llu: got seq %llu after %llu\n",
                        (unsigned long long)e.key,
                        (unsigned long long)e.seq,
                        (unsigned long long)last[e.key]);
                fflush(stdout);
                _exit(1);
            }
            last[e.key] = e.seq;
            ++seen[e.key];
        }

        printf("child: %llu messages, per-key counts:", (unsigned long long)total);
        for (int i = 0; i < KEYS; ++i)
            printf(" key%u=%llu", i, (unsigned long long)seen[i]);
        printf("\n");
        printf("child: per-key ordering holds\n");
        ty_region_close(creg);
        fflush(stdout);
        _exit(0);
    }

    for (uint64_t s = 0; s < PER_KEY; ++s) {
        for (uint64_t k = 0; k < KEYS; ++k) {
            envelope e;
            e.key = k;
            e.seq = s;
            for (;;) {
                ty_err_t r = ty_send(t, &e, sizeof e, k);
                if (r == TY_OK) break;
                if (r == TY_ERR_FULL) { ty_wait_room(t, k, 1000000LL); continue; }
                fprintf(stderr, "send failed: %s\n", ty_strerror(r));
                return 1;
            }
        }
    }
    printf("parent: published %d messages under %d keys\n",
           KEYS * PER_KEY, KEYS);

    int status = 0;
    waitpid(pid, &status, 0);

    ty_topic_close(t);
    ty_region_close(reg);
    ty_region_remove("keyorder", NULL);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 1;
}
