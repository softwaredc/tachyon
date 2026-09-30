/*
 * crash_recovery.c -- what happens when a producer is SIGKILLed mid-loan.
 *
 * SIGKILL cannot be caught, so there is no cleanup hook to run. The chunk the
 * dead process had borrowed would leak forever in a naive implementation.
 * tachyon gives every loan a lease; a reaper thread (spawned by the first
 * process that opens the region, or run as `tachyond`) walks the loan bitmap
 * and returns chunks whose owner has stopped beating.
 *
 * This example:
 *   1. starts a region with the reaper enabled,
 *   2. forks a child that borrows chunks and then kills itself with SIGKILL,
 *   3. waits past the lease,
 *   4. asserts the pool is back to its baseline free count.
 *
 * If step 4 fails, crash safety is broken and the whole value proposition of
 * the library is gone.
 */

/* kill() and usleep() are POSIX, not C11; without this they are undeclared
 * under a strict -std=c11 build. */
#define _DEFAULT_SOURCE

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

static void check(const char* what, ty_err_t err) {
    if (err != TY_OK) {
        fprintf(stderr, "%s failed: %s\n", what, ty_strerror(err));
        exit(1);
    }
}

static uint64_t free_chunks(ty_topic_t* t, size_t cls) {
    ty_pool_class_info_t info[8];
    memset(info, 0, sizeof info);
    check("pool_info", ty_pool_info(t, info, 8));
    return info[cls].free_count;
}

static void kill_self(void) {
    /* Raise, don't abort: we want the uncatchable one. */
    kill(getpid(), SIGKILL);
    _exit(99); /* not reached */
}

int main(void) {
    ty_region_remove("crashdemo", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 1;       /* reclaim in-process, no external daemon */
    rc.lease_ns     = 500ull * 1000ull * 1000ull;  /* 0.5s for a fast demo */
    check("region_open", ty_region_open("crashdemo", &rc, &reg));

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 64;
    tc.pool_bytes = 8u << 20;
    tc.notify     = TY_NOTIFY_HYBRID;
    check("topic_create", ty_topic_create(reg, "fragile", &tc, &t));

    size_t cls = 1; /* 256-byte class */
    uint64_t baseline = free_chunks(t, cls);
    printf("baseline free chunks in class %zu: %llu\n", cls,
           (unsigned long long)baseline);

    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create       = 0;
        cc.spawn_reaper = 0;
        check("child attach", ty_region_open("crashdemo", &cc, &creg));
        ty_topic_t* ct = NULL;
        check("child topic", ty_topic_open(creg, "fragile", &ct));

        /* Borrow chunks and die holding them. No publish, no abort, no
         * atexit -- this is the scenario the reaper exists for. */
        /* Held on purpose: never published, never aborted. That is the state
         * the reaper has to clean up after SIGKILL takes us out. */
        int n = 0;
        for (int i = 0; i < 16; ++i) {
            void* p = NULL;
            if (ty_loan(ct, 256, &p, NULL) == TY_OK) {
                memset(p, 0xAB, 256); /* make sure the loan is real work */
                ++n;
            }
        }
        printf("child: borrowed %d chunks, now SIGKILLing itself\n", n);
        fflush(stdout);
        kill_self();
    }

    int status = 0;
    waitpid(pid, &status, 0);
    printf("child died from signal %d\n", WTERMSIG(status));

    uint64_t right_after = free_chunks(t, cls);
    printf("immediately after death: %llu free (leaked %llu)\n",
           (unsigned long long)right_after,
           (unsigned long long)(baseline - right_after));

    printf("waiting for the lease to expire...\n");
    for (int i = 0; i < 40; ++i) {
        usleep(100000);
        uint64_t now = free_chunks(t, cls);
        if (now >= baseline) break;
    }

    uint64_t after = free_chunks(t, cls);
    ty_stats_t st;
    ty_topic_stats(t, &st);
    printf("after recovery: %llu free, reaper reclaimed %llu chunks\n",
           (unsigned long long)after, (unsigned long long)st.reclaimed);

    int ok = (after >= baseline);
    printf("%s\n", ok ? "PASS: pool fully recovered"
                      : "FAIL: chunks still missing");

    ty_topic_close(t);
    ty_region_close(reg);
    ty_region_remove("crashdemo", NULL);
    return ok ? 0 : 1;
}
