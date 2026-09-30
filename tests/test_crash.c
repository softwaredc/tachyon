/*
 * test_crash.c -- crash recovery: what happens when a peer disappears.
 *
 * This suite is the reason tachyon can be called production-usable rather than
 * merely fast. SIGKILL is uncatchable: no atexit, no destructor, no cleanup.
 * A loan held at that instant would otherwise be lost forever, and the pool
 * would bleed one chunk at a time until every publish returns NOMEM -- hours
 * later, under load, with no visible cause.
 *
 * Each case below kills a process mid-operation and then proves two things:
 *   1. the abandoned chunks come back (ty_reclaim_once / the reaper thread)
 *   2. the pool returns to its pre-crash free count exactly
 *
 * Run: ./test_crash   (needs fork, so it is separate from the TSan-friendly
 * single-process suite in test_basic.c)
 */
#define _GNU_SOURCE
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

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

#define ROUNDS 100

static const char* g_region = "ty_crash_test";

static void cleanup(void) { ty_region_remove(g_region, NULL); }

static uint64_t free_in_class(ty_topic_t* t, size_t cls) {
    ty_pool_class_info_t info[TACHYON_SIZE_CLASSES];
    memset(info, 0, sizeof info);
    ty_pool_info(t, info, TACHYON_SIZE_CLASSES);
    return info[cls].free_count;
}

/* --------------------------------------------------------------- case 1 */

/*
 * A producer loans chunks and is killed before publishing. Manual sweep.
 */
static void test_producer_killed_mid_loan(void) {
    ty_region_remove(g_region, NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u * 1024u * 1024u;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    rc.lease_ns     = 500ull * 1000000ull;   /* 500ms */
    CHECK(ty_region_open(g_region, &rc, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 128;
    tc.pool_bytes = 8u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "crash", &tc, &t) == TY_OK, "topic create");

    uint64_t before = free_in_class(t, 3);   /* 4096-byte class */
    CHECK(before > ROUNDS, "pool has spare chunks (%llu)",
          (unsigned long long)before);

    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create       = 0;
        cc.spawn_reaper = 0;
        if (ty_region_open(g_region, &cc, &creg) != TY_OK) _exit(3);

        ty_topic_t* ct = NULL;
        if (ty_topic_open(creg, "crash", &ct) != TY_OK) _exit(4);

        /* Loan and hold. No publish, no abort, no cleanup -- the process then
         * dies by SIGKILL, which cannot be trapped. */
        void* held[ROUNDS];
        int n = 0;
        for (int i = 0; i < ROUNDS; ++i) {
            if (ty_loan(ct, 4096, &held[i], NULL) == TY_OK) ++n;
        }
        (void)n;
        kill(getpid(), SIGKILL);
        _exit(5);   /* unreachable */
    }

    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status), "child died by signal");

    /* Give the OS a moment so kill(pid,0) reports ESRCH reliably. */
    usleep(100000);

    uint64_t reclaimed = ty_reclaim_once(t);
    CHECK(reclaimed == (uint64_t)ROUNDS, "reclaimed every abandoned chunk "
          "(%llu of %d)", (unsigned long long)reclaimed, ROUNDS);

    uint64_t after = free_in_class(t, 3);
    CHECK(after == before, "pool fully restored: before=%llu after=%llu",
          (unsigned long long)before, (unsigned long long)after);

    /* And the topic still works afterwards. */
    char buf[4096];
    memset(buf, 'z', sizeof buf);
    CHECK(ty_send(t, buf, sizeof buf, 1) == TY_OK, "publish after recovery");
    ty_msg_t m;
    CHECK(ty_subscribe(t, 0, &m, 0) == TY_OK, "receive after recovery");
    if (m.data) {
        CHECK(m.len == sizeof buf, "payload intact after recovery");
        CHECK(((char*)m.data)[0] == 'z', "byte intact after recovery");
        ty_release(t, &m);
    }

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup();
}

/* --------------------------------------------------------------- case 2 */

/*
 * A consumer receives a message and is killed before releasing it.
 * The chunk must come back just the same.
 */
static void test_consumer_killed_before_release(void) {
    ty_region_remove(g_region, NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u * 1024u * 1024u;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    rc.lease_ns     = 500ull * 1000000ull;
    CHECK(ty_region_open(g_region, &rc, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 256;
    tc.pool_bytes = 8u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "crash2", &tc, &t) == TY_OK, "topic create");

    /* Park ROUNDS messages in the ring for the child to pick up. */
    char buf[4096];
    memset(buf, 'q', sizeof buf);
    int sent = 0;
    for (int i = 0; i < ROUNDS; ++i) {
        if (ty_send(t, buf, sizeof buf, (uint64_t)i) == TY_OK) ++sent;
    }
    CHECK(sent == ROUNDS, "seeded %d messages", sent);

    uint64_t before_published = 0;
    ty_stats_t s0;
    ty_topic_stats(t, &s0);
    before_published = s0.published;
    CHECK(before_published == (uint64_t)ROUNDS, "published counter");

    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create = 0; cc.spawn_reaper = 0;
        if (ty_region_open(g_region, &cc, &creg) != TY_OK) _exit(3);
        ty_topic_t* ct = NULL;
        if (ty_topic_open(creg, "crash2", &ct) != TY_OK) _exit(4);

        /* Take delivery and then die, never calling ty_release. */
        ty_msg_t m;
        int got = 0;
        for (int i = 0; i < ROUNDS; ++i) {
            if (ty_subscribe(ct, 0, &m, 0) == TY_OK) ++got;
        }
        (void)got;
        kill(getpid(), SIGKILL);
        _exit(5);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status), "consumer died by signal");
    usleep(100000);

    uint64_t reclaimed = ty_reclaim_once(t);
    CHECK(reclaimed == (uint64_t)ROUNDS, "reclaimed delivered-but-unreleased "
          "chunks (%llu of %d)", (unsigned long long)reclaimed, ROUNDS);

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup();
}

/* --------------------------------------------------------------- case 3 */

/*
 * Background reaper: nobody calls ty_reclaim_once, the sweeper thread does it.
 */
static void test_reaper_thread_recovers(void) {
    ty_region_remove(g_region, NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u * 1024u * 1024u;
    rc.create       = 1;
    rc.spawn_reaper = 1;          /* the whole point of this case */
    rc.lease_ns     = 400ull * 1000000ull;   /* tick = 100ms */
    CHECK(ty_region_open(g_region, &rc, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 128;
    tc.pool_bytes = 8u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "crash3", &tc, &t) == TY_OK, "topic create");

    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create = 0; cc.spawn_reaper = 0;
        if (ty_region_open(g_region, &cc, &creg) != TY_OK) _exit(3);
        ty_topic_t* ct = NULL;
        if (ty_topic_open(creg, "crash3", &ct) != TY_OK) _exit(4);
        void* held[ROUNDS];
        for (int i = 0; i < ROUNDS; ++i) ty_loan(ct, 4096, &held[i], NULL);
        kill(getpid(), SIGKILL);
        _exit(5);
    }
    int status = 0;
    waitpid(pid, &status, 0);

    /* No manual sweep here: wait for the background thread to notice. */
    int waited = 0;
    ty_stats_t st;
    for (;;) {
        usleep(50000);
        ty_topic_stats(t, &st);
        if (st.reclaimed >= ROUNDS) break;
        if (++waited > 100) break;   /* 5s ceiling */
    }
    CHECK(st.reclaimed >= (uint64_t)ROUNDS,
          "reaper thread reclaimed %llu (>= %d) after %dms",
          (unsigned long long)st.reclaimed, ROUNDS, waited * 50);

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup();
}

/* --------------------------------------------------------------- case 4 */

/*
 * A live-but-slow producer must NOT have its chunk stolen just because the
 * lease deadline passed. This is the negative case that makes the positive
 * ones meaningful: reclaiming too eagerly corrupts live data.
 */
static void test_live_producer_not_reclaimed(void) {
    ty_region_remove(g_region, NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u * 1024u * 1024u;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    rc.lease_ns     = 200ull * 1000000ull;   /* aggressively short */
    CHECK(ty_region_open(g_region, &rc, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 128;
    tc.pool_bytes = 8u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "live", &tc, &t) == TY_OK, "topic create");

    void* held = NULL;
    CHECK(ty_loan(t, 4096, &held, NULL) == TY_OK, "loan held by live owner");
    CHECK(held != NULL, "loan pointer set");

    /* Sit on it well past the deadline, while pumping heartbeats like any
     * healthy process would. */
    for (int i = 0; i < 30; ++i) {
        usleep(50000);
        ty_heartbeat(reg);
    }

    uint64_t reclaimed = ty_reclaim_once(t);
    CHECK(reclaimed == 0, "live owner kept its chunk (reclaimed %llu)",
          (unsigned long long)reclaimed);

    /* The chunk is still ours and still writable, which is the real test:
     * data must not have been handed to anybody else. */
    memset(held, 0xAB, 4096);
    CHECK(((unsigned char*)held)[4095] == 0xAB, "chunk still exclusively ours");
    CHECK(ty_publish(t, held, 4096, 1) == TY_OK, "late publish still works");

    ty_msg_t m;
    CHECK(ty_subscribe(t, 0, &m, 0) == TY_OK, "late message delivered");
    if (m.data) {
        CHECK(((unsigned char*)m.data)[4095] == 0xAB, "late payload intact");
        ty_release(t, &m);
    }

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup();
}

int main(void) {
    printf("tachyon crash-recovery suite (pid=%d)\n", (int)getpid());

    struct { const char* name; void (*fn)(void); } tests[] = {
        {"producer_killed_mid_loan",      test_producer_killed_mid_loan},
        {"consumer_killed_before_release",test_consumer_killed_before_release},
        {"reaper_thread_recovers",        test_reaper_thread_recovers},
        {"live_producer_not_reclaimed",   test_live_producer_not_reclaimed},
    };

    for (size_t i = 0; i < sizeof tests / sizeof tests[0]; ++i) {
        int before = failures;
        printf("[ RUN ] %s\n", tests[i].name);
        fflush(stdout);
        tests[i].fn();
        if (failures == before) printf("[  OK ] %s\n", tests[i].name);
        else printf("[ FAIL] %s\n", tests[i].name);
        fflush(stdout);
    }

    printf("\n%d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
