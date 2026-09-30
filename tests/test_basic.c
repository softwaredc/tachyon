/*
 * test_basic.c -- single-process correctness suite.
 *
 * No external test framework on purpose: this library has zero dependencies
 * and the test suite inherits that. `make check` must work on a machine with
 * nothing but a C compiler and libc.
 *
 * Run under the tests target, or from CI under ASan/UBSan/TSan builds. It is
 * deliberately single-process so it can run under TSan, which does not
 * understand our shared-memory atomics; cross-process behaviour is covered by
 * test_crash and the stress harness instead.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
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

#define CHECK_EQ(got, want, label)                                          \
    CHECK((got) == (want), "%s: got %lld, want %lld", label,                \
          (long long)(got), (long long)(want))

/* Name buffers must outlive the calls, so keep a small rotating pool. */
static const char* fresh_name(void) {
    static char pool[32][64];
    static int   next = 0;
    int slot = next;
    next = (next + 1) % 32;
    snprintf(pool[slot], sizeof pool[slot], "ty_test_%ld_%d", (long)getpid(), slot);
    return pool[slot];
}

/* ------------------------------------------------------------------ helpers */

static void cleanup(const char* name) { ty_region_remove(name, NULL); }

static void test_open_close(void) {
    const char* name = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 4u * 1024u * 1024u;
    conf.spawn_reaper = 0;

    CHECK(ty_region_open(name, &conf, &reg) == TY_OK, "open should succeed");
    CHECK(reg != NULL, "handle must be set");
    CHECK(ty_region_size(reg) >= sizeof(void*) * 8, "region has a size");
    CHECK(strcmp(ty_region_name(reg), name) == 0, "name round-trips");
    CHECK(ty_region_close(reg) == TY_OK, "close should succeed");

    /* close() frees the handle, exactly as fclose()/curl_easy_cleanup() do.
     * Touching it afterwards is undefined behaviour by contract, so there is no
     * assertion here -- a test that dereferences a freed handle would "pass"
     * only by luck and would trip ASan. The rule is documented in docs/API.md
     * instead: one close, then the handle is dead. */

    cleanup(name);
}

/*
 * A region must outlive the process that created it, otherwise the most useful
 * property of shared memory disappears: a producer cannot publish and exit.
 * Removal is therefore explicit, never a side effect of closing.
 */
static void test_region_survives_creator(void) {
    const char* name = fresh_name();
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 4u * 1024u * 1024u;
    conf.spawn_reaper = 0;

    ty_region_t* a = NULL;
    CHECK(ty_region_open(name, &conf, &a) == TY_OK, "create ok");
    CHECK(ty_region_close(a) == TY_OK, "creator closes");

    /* Second attachment sees the same bytes. */
    ty_region_conf_t attach = ty_region_conf_default;
    attach.create = 0;
    attach.spawn_reaper = 0;
    ty_region_t* b = NULL;
    CHECK(ty_region_open(name, &attach, &b) == TY_OK, "re-attach after creator exit");
    if (b) {
        CHECK(ty_region_size(b) >= sizeof(void*) * 8, "existing region intact");
        ty_region_close(b);
    }

    /* Explicit removal, then re-attach must fail. */
    CHECK(ty_region_remove(name, NULL) == TY_OK, "explicit remove");
    ty_region_t* c = NULL;
    CHECK(ty_region_open(name, &attach, &c) != TY_OK, "gone after remove");
}

static void test_topic_lifecycle(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 8u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 4;
    tc.capacity   = 256;
    tc.pool_bytes = 2u * 1024u * 1024u;

    CHECK(ty_topic_create(reg, "orders", &tc, &t) == TY_OK, "create topic");
    CHECK(t != NULL, "topic handle set");
    CHECK(ty_topic_partitions(t) == 4, "partitions preserved");
    CHECK(ty_topic_capacity(t) == 256, "capacity preserved");

    /* Duplicate creation is refused rather than silently reusing it. */
    ty_topic_t* dup = NULL;
    CHECK(ty_topic_create(reg, "orders", &tc, &dup) == TY_ERR_EXIST,
          "duplicate create rejected");

    /* Open by name from a second handle, as another process would. */
    ty_topic_t* again = NULL;
    CHECK(ty_topic_open(reg, "nosuch", &again) == TY_ERR_NOENT,
          "missing topic reports NOENT");

    ty_topic_close(t);
    ty_region_close(reg);
    ty_region_remove(rname, NULL);
}

static void test_send_recv(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 8u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 2;
    tc.capacity   = 128;
    tc.pool_bytes = 1u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "pipe", &tc, &t) == TY_OK, "create");

    const char* msg = "hello zero-copy";
    CHECK(ty_send(t, msg, strlen(msg) + 1, 42) == TY_OK, "send");

    ty_msg_t m;
    CHECK(ty_subscribe(t, ty_route(t, 42), &m, 0) == TY_OK, "recv");
    CHECK(strcmp((const char*)m.data, msg) == 0, "payload intact: '%s'",
          (const char*)m.data);
    CHECK(m.len == strlen(msg) + 1, "length preserved");
    CHECK(m.key == 42, "key survives the trip");
    CHECK(m.capacity >= m.len, "capacity >= len");
    CHECK(ty_release(t, &m) == TY_OK, "release");

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

static void test_zero_copy_semantics(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 8u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 64;
    tc.pool_bytes = 1u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "zc", &tc, &t) == TY_OK, "create");

    /* The whole point: write straight into shared memory, publish without a
     * copy, and observe that the reader gets the identical address. */
    void* buf = NULL;
    size_t cap = 0;
    CHECK(ty_loan(t, 2048, &buf, &cap) == TY_OK, "loan 2048");
    CHECK(buf != NULL, "buffer returned");
    CHECK(cap >= 2048, "capacity covers the request");

    unsigned int* stamps = (unsigned int*)buf;
    unsigned int pattern = 0xDEADBEEFu;
    stamps[0] = pattern;
    stamps[511] = pattern; /* touches past one cache line */

    CHECK(ty_publish(t, buf, 2048, 7) == TY_OK, "publish");

    ty_msg_t m;
    CHECK(ty_subscribe(t, 0, &m, 0) == TY_OK, "receive");
    CHECK(m.data == buf, "same address -> zero copy really happened");
    CHECK(((unsigned int*)m.data)[0] == pattern, "first word intact");
    CHECK(((unsigned int*)m.data)[511] == pattern, "last word intact");
    CHECK(ty_release(t, &m) == TY_OK, "release");

    /* Aborting a loan returns the chunk without publishing anything. */
    void* b2 = NULL;
    CHECK(ty_loan(t, 128, &b2, NULL) == TY_OK, "loan again");
    CHECK(ty_abort(t, b2) == TY_OK, "abort");

    /* Oversized payloads are refused instead of corrupting the pool. */
    void* b3 = NULL;
    CHECK(ty_loan(t, TACHYON_MAX_MSG_SIZE + 1, &b3, NULL) == TY_ERR_TOOBIG,
          "oversize rejected");

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

static void test_key_routing(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 16u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 8;
    tc.capacity   = 1024;
    tc.pool_bytes = 4u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "keys", &tc, &t) == TY_OK, "create");

    /* Same key must always land on the same partition, otherwise ordering is
     * not guaranteed and nothing else in this library matters. */
    for (uint64_t k = 0; k < 1000; ++k) {
        CHECK(ty_route(t, k) == ty_route(t, k), "routing is deterministic");
    }
    CHECK(ty_route(t, 0) < 8, "routing stays inside the partition range");

    /* Deterministic partition choice is part of the contract. */
    CHECK(ty_route(t, 12345) == ty_route(t, 12345), "stable across calls");

    /* Publish 200 messages under key 999 and read them back in order. */
    const int N = 200;
    int written = 0;
    for (int i = 0; i < N; ++i) {
        unsigned int v = (unsigned int)i;
        if (ty_send(t, &v, sizeof v, 999) == TY_OK) ++written;
    }
    CHECK_EQ(written, N, "all publishes succeeded");

    uint32_t part = ty_route(t, 999);
    uint64_t expect = 0;
    int got = 0;
    int ordered = 1;
    for (;;) {
        ty_msg_t m;
        ty_err_t rc = ty_subscribe(t, part, &m, 0);
        if (rc != TY_OK) break;
        unsigned int v = *(unsigned int*)m.data;
        if (v != (unsigned int)expect) ordered = 0;
        ++expect;
        ++got;
        ty_release(t, &m);
    }
    CHECK_EQ(got, N, "every message came back");
    CHECK(ordered, "same-key messages arrived in FIFO order");

    CHECK(ty_subscribe(t, part, &(ty_msg_t){0}, 0) == TY_ERR_EMPTY,
          "drained topic reports EMPTY on poll");

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

static void test_size_classes(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 32u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 64;
    tc.pool_bytes = 16u * 1024u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "sizes", &tc, &t) == TY_OK, "create");

    /* Every advertised class must be loanable and must round-trip its tail
     * byte -- a classic place for off-by-one chunk sizing bugs to hide. */
    for (size_t c = 0; c < ty_size_class_count(); ++c) {
        size_t sz = ty_size_class(c);
        void* buf = NULL;
        size_t cap = 0;
        CHECK(ty_loan(t, sz, &buf, &cap) == TY_OK, "loan class %zu (%zu bytes)",
              c, sz);
        if (!buf) continue;
        CHECK(cap >= sz, "class %zu capacity covers request", c);

        memset(buf, 0xA5, sz);
        ((unsigned char*)buf)[sz - 1] = 0x5A;
        CHECK(ty_publish(t, buf, sz, 1) == TY_OK, "publish class %zu", c);

        ty_msg_t m;
        CHECK(ty_subscribe(t, 0, &m, 0) == TY_OK, "recv class %zu", c);
        CHECK(m.len == sz, "class %zu length", c);
        CHECK(((unsigned char*)m.data)[sz - 1] == 0x5A, "class %zu tail intact", c);
        ty_release(t, &m);
    }

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

static void test_timeouts(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 4u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 8;
    tc.pool_bytes = 512u * 1024u;
    tc.notify     = TY_NOTIFY_HYBRID;
    CHECK(ty_topic_create(reg, "slow", &tc, &t) == TY_OK, "create");

    /* Blocking on an empty topic must respect the deadline under every notify
     * mode; this is where a broken futex protocol hangs forever. */
    ty_msg_t m;
    uint64_t t0 = ty_now_ns();
    ty_err_t rc = ty_subscribe(t, 0, &m, 50ull * 1000000ull); /* 50ms */
    uint64_t elapsed = ty_now_ns() - t0;

    CHECK(rc == TY_ERR_TIMEOUT, "empty topic times out");
    CHECK(elapsed >= 45000000ull, "waited at least ~45ms, waited %llu ns",
          (unsigned long long)elapsed);
    CHECK(elapsed < 2000000000ull, "did not hang, waited %llu ns",
          (unsigned long long)elapsed);

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

static void test_ring_full(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 4u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 8;          /* tiny on purpose */
    tc.pool_bytes = 512u * 1024u;
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "full", &tc, &t) == TY_OK, "create");

    /* Fill past capacity: expect exactly 8 successes then FULL, never a hang
     * and never silent overwrite. */
    char payload[64];
    memset(payload, 'x', sizeof payload);
    int ok = 0, full = 0;
    for (int i = 0; i < 100; ++i) {
        ty_err_t rc = ty_send(t, payload, sizeof payload, 1);
        if (rc == TY_OK) ++ok;
        else if (rc == TY_ERR_FULL) { ++full; break; }
        else CHECK(0, "unexpected error %s", ty_strerror(rc));
    }
    CHECK(ok == 8, "exactly capacity messages accepted, got %d", ok);
    CHECK(full == 1, "then FULL reported");

    /* Drain and confirm the ring accepts work again. */
    ty_msg_t m;
    int drained = 0;
    while (ty_subscribe(t, 0, &m, 0) == TY_OK) { ty_release(t, &m); ++drained; }
    CHECK_EQ(drained, 8, "all eight came back");
    CHECK(ty_send(t, payload, sizeof payload, 1) == TY_OK, "ring reusable");

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

static void test_pool_exhaustion(void) {
    const char* rname = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.size = 8u * 1024u * 1024u;
    conf.spawn_reaper = 0;
    CHECK(ty_region_open(rname, &conf, &reg) == TY_OK, "region open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 4096;
    tc.pool_bytes = 1u * 1024u * 1024u;  /* small pool, many loans */
    tc.notify     = TY_NOTIFY_SPIN;
    CHECK(ty_topic_create(reg, "exhaust", &tc, &t) == TY_OK, "create");

    /* Loan everything without publishing. The library must report NOMEM --
     * running out of chunks is normal in a real system and must never crash. */
    enum { MAX_LOANS = 512 };
    void* held[MAX_LOANS];
    int n = 0;
    while (n < MAX_LOANS) {
        void* p = NULL;
        if (ty_loan(t, 4096, &p, NULL) != TY_OK) break;
        held[n++] = p;
    }
    CHECK(n > 0, "some loans succeeded");

    void* extra = NULL;
    CHECK(ty_loan(t, 4096, &extra, NULL) == TY_ERR_NOMEM,
          "exhausted pool reports NOMEM");

    /* Give everything back through the abort path. */
    for (int i = 0; i < n; ++i) {
        CHECK(ty_abort(t, held[i]) == TY_OK, "abort loan %d", i);
    }
    CHECK(ty_loan(t, 4096, &extra, NULL) == TY_OK, "pool recovers after aborts");
    ty_abort(t, extra);

    ty_topic_close(t);
    ty_region_close(reg);
    cleanup(rname);
}

/*
 * Closing a topic after its region is the normal outcome of any garbage
 * collected binding: destructors run in an arbitrary order, and ty_region_t
 * outlives the munmap only because ty_topic_close still holds a reference.
 * Historically this wrote a refcount into unmapped memory and segfaulted.
 */
static void test_close_order(void) {
    const char* name = fresh_name();
    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 16u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    CHECK(ty_region_open(name, &rc, &reg) == TY_OK, "open");

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 2;
    tc.capacity   = 64;
    tc.pool_bytes = 1u << 20;
    CHECK(ty_topic_create(reg, "late", &tc, &t) == TY_OK, "create topic");

    /* Region first, topic second. Must not fault. */
    CHECK(ty_region_close(reg) == TY_OK, "close region first");
    CHECK(ty_topic_close(t) == TY_OK, "close topic after region is safe");

    cleanup(name);
}

static void test_invalid_args(void) {
    CHECK(ty_region_open(NULL, NULL, NULL) == TY_ERR_INVAL, "null name");
    ty_region_t* r = NULL;
    CHECK(ty_region_open("has/slash", NULL, &r) == TY_ERR_INVAL,
          "slashes rejected in names");
    CHECK(ty_strerror(TY_ERR_FULL) != NULL, "strerror covers codes");
    CHECK(strlen(ty_strerror(TY_ERR_TIMEOUT)) > 0, "strerror is meaningful");
}

int main(void) {
    printf("tachyon basic suite\n");
    printf("cpus=%d tsc_usable=%d now_ns=%llu\n", ty_cpu_count(), ty_tsc_usable(),
           (unsigned long long)ty_now_ns());

    struct { const char* name; void (*fn)(void); } tests[] = {
        {"region_open_close",  test_open_close},
        {"region_survives",    test_region_survives_creator},
        {"topic_lifecycle",    test_topic_lifecycle},
        {"send_recv",          test_send_recv},
        {"zero_copy",          test_zero_copy_semantics},
        {"key_routing",        test_key_routing},
        {"size_classes",       test_size_classes},
        {"timeouts",           test_timeouts},
        {"ring_full",          test_ring_full},
        {"pool_exhaustion",    test_pool_exhaustion},
        {"invalid_args",       test_invalid_args},
        {"close_order",        test_close_order},
    };

    for (size_t i = 0; i < sizeof tests / sizeof tests[0]; ++i) {
        int before = failures;
        printf("[ RUN ] %s\n", tests[i].name);
        tests[i].fn();
        if (failures == before) printf("[  OK ] %s\n", tests[i].name);
        else printf("[ FAIL] %s\n", tests[i].name);
    }

    printf("\n%d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
