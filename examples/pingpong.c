/*
 * pingpong.c -- measure real round-trip latency between two processes.
 *
 * The parent sends, the child replies, and the parent times the round trip.
 * Unlike `benchmarks/bench_ipc.c` this prints percentiles instead of just the
 * mean, because the mean hides the tail and the tail is what wakes people up
 * at 3am.
 *
 *   ./pingpong            # 256-byte payload, 20000 round trips
 *   ./pingpong 4096 5000  # 4 KiB payload, 5000 round trips
 */
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

static int cmp_u64(const void* a, const void* b) {
    uint64_t x = *(const uint64_t*)a, y = *(const uint64_t*)b;
    return (x > y) - (x < y);
}

static uint64_t pct(uint64_t* sorted, size_t n, double p) {
    if (n == 0) return 0;
    size_t i = (size_t)(p * (double)(n - 1) + 0.5);
    return sorted[i < n ? i : n - 1];
}

int main(int argc, char** argv) {
    size_t   size  = (argc > 1) ? (size_t)strtoul(argv[1], NULL, 10) : 256;
    uint64_t count = (argc > 2) ? strtoull(argv[2], NULL, 10) : 20000;
    if (size == 0) size = 256;
    if (count == 0) count = 20000;

    ty_region_remove("pingpong", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    check("region_open", ty_region_open("pingpong", &rc, &reg));

    ty_topic_t* out = NULL;  /* parent -> child */
    ty_topic_t* in  = NULL;  /* child  -> parent */
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 1;
    tc.capacity   = 1024;
    tc.pool_bytes = 8u << 20;
    tc.notify     = TY_NOTIFY_SPIN; /* lowest latency; burns a core */
    check("p2c", ty_topic_create(reg, "p2c", &tc, &out));
    check("c2p", ty_topic_create(reg, "c2p", &tc, &in));

    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create       = 0;
        cc.spawn_reaper = 0;
        check("child attach", ty_region_open("pingpong", &cc, &creg));

        ty_topic_t* cin  = NULL;
        ty_topic_t* cout = NULL;
        check("child p2c", ty_topic_open(creg, "p2c", &cin));
        check("child c2p", ty_topic_open(creg, "c2p", &cout));

        for (;;) {
            ty_msg_t m;
            ty_err_t r = ty_subscribe(cin, 0, &m, 5000000000LL);
            if (r != TY_OK) break;               /* parent is done with us */
            ty_err_t s = ty_send(cout, m.data, m.len, 0);
            ty_release(cin, &m);
            if (s != TY_OK) break;
        }
        ty_region_close(creg);
        fflush(stdout);
        _exit(0);
    }

    char* payload = (char*)malloc(size);
    if (!payload) { perror("malloc"); return 1; }
    memset(payload, 'x', size);

    uint64_t* samples = (uint64_t*)malloc(count * sizeof(uint64_t));
    if (!samples) { perror("malloc"); return 1; }

    /* Warm up: first touches page in the pool, the ring, and the branch
     * predictors. Without this the first few samples are meaningless. */
    for (int i = 0; i < 1000; ++i) {
        check("warm send", ty_send(out, payload, size, 0));
        ty_msg_t m;
        check("warm recv", ty_subscribe(in, 0, &m, 5000000000LL));
        ty_release(in, &m);
    }

    for (uint64_t i = 0; i < count; ++i) {
        uint64_t t0 = ty_now_ns();
        check("send", ty_send(out, payload, size, 0));
        ty_msg_t m;
        check("recv", ty_subscribe(in, 0, &m, 5000000000LL));
        uint64_t t1 = ty_now_ns();
        ty_release(in, &m);
        samples[i] = t1 - t0;
    }

    qsort(samples, (size_t)count, sizeof(uint64_t), cmp_u64);
    double mean = 0;
    for (uint64_t i = 0; i < count; ++i) mean += (double)samples[i];
    mean /= (double)count;

    printf("payload %zu B, %llu round trips\n", size, (unsigned long long)count);
    printf("  mean  %8llu ns\n", (unsigned long long)mean);
    printf("  p50   %8llu ns\n", (unsigned long long)pct(samples, count, 0.50));
    printf("  p90   %8llu ns\n", (unsigned long long)pct(samples, count, 0.90));
    printf("  p99   %8llu ns\n", (unsigned long long)pct(samples, count, 0.99));
    printf("  p99.9 %8llu ns\n", (unsigned long long)pct(samples, count, 0.999));
    printf("  min   %8llu ns\n", (unsigned long long)samples[0]);
    printf("  max   %8llu ns\n", (unsigned long long)samples[count - 1]);
    printf("  (divide by 2 for one-way latency)\n");

    ty_stats_t st;
    ty_topic_stats(out, &st);
    printf("  published=%llu consumed=%llu dropped=%llu\n",
           (unsigned long long)st.published, (unsigned long long)st.consumed,
           (unsigned long long)st.dropped);

    free(samples);
    free(payload);
    ty_topic_close(out);
    ty_topic_close(in);
    ty_region_close(reg);
    ty_region_remove("pingpong", NULL);
    return 0;
}
