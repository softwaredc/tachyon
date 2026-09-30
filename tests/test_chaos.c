/*
 * test_chaos.c -- kill processes mid-operation, thousands of times, and prove
 * that not a single chunk was lost.
 *
 * This is the test that makes the crash-safety claim in the README more than
 * marketing. Each round forks a child that loans a random number of chunks and
 * then dies in one of three ways:
 *
 *   SIGKILL mid-loan    -- the hard case, nothing runs at all
 *   SIGKILL mid-consume -- received messages never released
 *   clean exit          -- control group, must leak nothing either
 *
 * Between rounds the reaper sweeps. At the end the pool must be back to
 * exactly its starting free count. A single missing chunk fails the run.
 *
 * Usage: test_chaos [rounds]
 */
#define _GNU_SOURCE
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

static int    g_failures = 0;
static int    g_checks   = 0;
static long   g_rounds   = 2000;

#define CHECK(cond, ...)                                                    \
    do {                                                                    \
        ++g_checks;                                                         \
        if (!(cond)) {                                                      \
            ++g_failures;                                                   \
            printf("  FAIL %s:%d: ", __func__, __LINE__);                   \
            printf(__VA_ARGS__);                                             \
            printf("\n");                                                   \
        }                                                                   \
    } while (0)

#define REGION_NAME "ty_chaos"
#define TOPIC_NAME  "chaos"
#define LOAN_SIZE   4096
#define CLS         3          /* 4096-byte size class */
#define MAX_LOANS   16

static uint64_t free_count(ty_topic_t* t, size_t cls) {
    ty_pool_class_info_t info[TACHYON_SIZE_CLASSES];
    memset(info, 0, sizeof info);
    ty_pool_info(t, info, TACHYON_SIZE_CLASSES);
    return info[cls].free_count;
}

int main(int argc, char** argv) {
    if (argc > 1) g_rounds = atol(argv[1]);

    ty_region_remove(REGION_NAME, NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 64u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 1;                        /* let the sweeper do the work */
    rc.lease_ns     = 300ull * 1000000ull;      /* 300ms -> ~75ms tick */
    if (ty_region_open(REGION_NAME, &rc, &reg) != TY_OK) {
        printf("cannot open region\n");
        return 2;
    }

    ty_topic_t* topic = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 2;
    tc.capacity   = 256;
    tc.pool_bytes = 32u << 20;
    tc.notify     = TY_NOTIFY_SPIN;
    if (ty_topic_create(reg, TOPIC_NAME, &tc, &topic) != TY_OK) {
        printf("cannot create topic\n");
        return 2;
    }

    uint64_t baseline = free_count(topic, CLS);
    printf("chaos: %ld rounds, baseline free chunks in %zu-byte class = %llu\n",
           g_rounds, (size_t)LOAN_SIZE, (unsigned long long)baseline);

    unsigned seed = 12345;
    long killed_mid_loan = 0, killed_mid_consume = 0, clean = 0;

    /* Zeroed once, outside the loop: if ty_topic_open fails on some round the
     * matching slot is never written, and the cleanup pass below would read an
     * indeterminate pointer. */
    ty_topic_t* topics[TACHYON_MAX_TOPICS];
    memset(topics, 0, sizeof topics);

    for (long r = 0; r < g_rounds; ++r) {
        seed = seed * 1103515245u + 12345u;
        int mode = (int)((seed >> 16) % 3);
        int nloans = (int)((seed >> 8) % MAX_LOANS) + 1;

        /* Seed a few messages sometimes, so mode 1 has something to consume. */
        if (mode == 1) {
            char buf[LOAN_SIZE];
            memset(buf, 'c', sizeof buf);
            for (int i = 0; i < nloans; ++i) ty_send(topic, buf, sizeof buf, (uint64_t)i);
        }

        pid_t pid = fork();
        if (pid == 0) {
            ty_region_t* creg = NULL;
            ty_region_conf_t cc = ty_region_conf_default;
            cc.create = 0; cc.spawn_reaper = 0;
            if (ty_region_open(REGION_NAME, &cc, &creg) != TY_OK) _exit(3);
            ty_topic_t* ct = NULL;
            if (ty_topic_open(creg, TOPIC_NAME, &ct) != TY_OK) _exit(4);

            if (mode == 1) {
                ty_msg_t m;
                for (int i = 0; i < nloans; ++i) {
                    if (ty_subscribe_any(ct, NULL, &m, 0) != TY_OK) break;
                    /* deliberately no ty_release */
                }
                kill(getpid(), SIGKILL);
            } else {
                void* held[MAX_LOANS];
                for (int i = 0; i < nloans; ++i) ty_loan(ct, LOAN_SIZE, &held[i], NULL);
                if (mode == 0) {
                    kill(getpid(), SIGKILL);
                } else {
                    /* Exit without aborting: exercises lease expiry rather
                     * than the fast "owner is gone" path. */
                    _exit(0);
                }
            }
            _exit(0);
        }

        int status = 0;
        waitpid(pid, &status, 0);
        if (mode == 0) ++killed_mid_loan;
        else if (mode == 1) ++killed_mid_consume;
        else ++clean;

        /* Give the reaper a chance every so often; it runs on its own tick. */
        for (int i = 0; i < TACHYON_MAX_TOPICS; ++i) topics[i] = NULL;

        if ((r & 15) == 0) usleep(2000);
        if ((r & 255) == 0) {
            ty_stats_t s;
            ty_topic_stats(topic, &s);
            printf("  round %ld: free=%llu reclaimed=%llu\n", r,
                   (unsigned long long)free_count(topic, CLS),
                   (unsigned long long)s.reclaimed);
        }
    }

    /* Final sweep and settle. */
    for (int i = 0; i < 40; ++i) {
        ty_reclaim_once(topic);
        usleep(50000);
        if (free_count(topic, CLS) >= baseline) break;
    }

    uint64_t final = free_count(topic, CLS);
    ty_stats_t st;
    ty_topic_stats(topic, &st);

    printf("\nrounds: killed-mid-loan=%ld killed-mid-consume=%ld clean=%ld\n",
           killed_mid_loan, killed_mid_consume, clean);
    printf("final free=%llu (baseline %llu), reclaimed=%llu\n",
           (unsigned long long)final, (unsigned long long)baseline,
           (unsigned long long)st.reclaimed);

    CHECK(final >= baseline, "pool recovered: final=%llu baseline=%llu",
          (unsigned long long)final, (unsigned long long)baseline);
    CHECK(st.reclaimed > 0, "reaper reclaimed something (%llu)",
          (unsigned long long)st.reclaimed);

    ty_topic_close(topic);
    ty_region_close(reg);
    ty_region_remove(REGION_NAME, NULL);

    printf("%d checks, %d failures\n", g_checks, g_failures);
    if (g_failures == 0) printf("CHAOS OK: no chunk lost in %ld rounds\n", g_rounds);
    return g_failures ? 1 : 0;
}
