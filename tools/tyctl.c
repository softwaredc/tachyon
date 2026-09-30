/*
 * tyctl -- inspect a live region.
 *
 * A shared-memory IPC system is opaque by nature: `ls /dev/shm` tells you a
 * region exists and nothing about what is happening inside it. This tool is
 * the difference between "throughput dropped and nobody knows why" and a
 * two-second answer.
 *
 *   tyctl list <region>                 topics and their occupancy
 *   tyctl stats <region> <topic>        counters
 *   tyctl pool  <region> <topic>        per-class chunk usage
 *   tyctl watch <region> <topic> [ms]   repeat stats
 */
/* usleep() is not visible under strict -std=c11 without a feature macro. */
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

#define MAX_TOPICS TACHYON_MAX_TOPICS

static void die(const char* what, ty_err_t e) {
    fprintf(stderr, "tyctl: %s: %s\n", what, ty_strerror(e));
    exit(1);
}

static ty_region_t* attach(const char* region) {
    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.create       = 0;
    conf.spawn_reaper = 0;
    ty_err_t rc = ty_region_open(region, &conf, &reg);
    if (rc != TY_OK) die(region, rc);
    return reg;
}

static void cmd_list(const char* region) {
    ty_region_t* reg = attach(region);

    char(*names)[TACHYON_MAX_NAME + 1] = calloc(MAX_TOPICS, TACHYON_MAX_NAME + 1);
    int n = ty_topic_list(reg, names, MAX_TOPICS);
    if (n < 0) die("ty_topic_list", (ty_err_t)n);

    printf("%-32s %8s %8s %10s %10s\n",
           "TOPIC", "PARTS", "CAP", "DEPTH", "PUBLISHED");
    for (int i = 0; i < n && i < MAX_TOPICS; ++i) {
        ty_topic_t* t = NULL;
        if (ty_topic_open(reg, names[i], &t) != TY_OK) continue;
        uint64_t depth = 0;
        ty_depth(t, -1, &depth);
        ty_stats_t s;
        ty_topic_stats(t, &s);
        printf("%-32s %8u %8u %10llu %10llu\n", names[i],
               ty_topic_partitions(t), ty_topic_capacity(t),
               (unsigned long long)depth, (unsigned long long)s.published);
        ty_topic_close(t);
    }
    if (n == 0) printf("(no topics)\n");

    free(names);
    ty_region_close(reg);
}

static void cmd_stats(const char* region, const char* topic_name) {
    ty_region_t* reg = attach(region);
    ty_topic_t* t = NULL;
    ty_err_t rc = ty_topic_open(reg, topic_name, &t);
    if (rc != TY_OK) die(topic_name, rc);

    ty_stats_t s;
    ty_topic_stats(t, &s);
    uint64_t depth = 0;
    ty_depth(t, -1, &depth);

    printf("topic            : %s\n", topic_name);
    printf("partitions       : %u (capacity %u each)\n",
           ty_topic_partitions(t), ty_topic_capacity(t));
    printf("depth            : %llu\n", (unsigned long long)depth);
    printf("published        : %llu\n", (unsigned long long)s.published);
    printf("consumed         : %llu\n", (unsigned long long)s.consumed);
    printf("dropped (full)   : %llu\n", (unsigned long long)s.dropped);
    printf("loans ok         : %llu\n", (unsigned long long)s.loan_ok);
    printf("loans nomem      : %llu\n", (unsigned long long)s.loan_nomem);
    printf("reclaimed        : %llu\n", (unsigned long long)s.reclaimed);
    printf("futex wakeups    : %llu\n", (unsigned long long)s.wait_wakeups);
    printf("spin iterations  : %llu\n", (unsigned long long)s.spin_spins);

    /* Two counters are worth calling out, because a number alone does not tell
     * you what to do about it. */
    if (s.reclaimed > 0) {
        printf("\nnote: reclaimed > 0 means processes holding loans are being\n"
               "      killed; check whether that is expected in your system.\n");
    }
    if (s.loan_nomem > 0) {
        printf("\nnote: loan_nomem > 0 means the pool ran dry. Either the\n"
               "      consumer fell behind, or pool_bytes is too small for\n"
               "      this message mix.\n");
    }

    ty_topic_close(t);
    ty_region_close(reg);
}

static void cmd_pool(const char* region, const char* topic_name) {
    ty_region_t* reg = attach(region);
    ty_topic_t* t = NULL;
    ty_err_t rc = ty_topic_open(reg, topic_name, &t);
    if (rc != TY_OK) die(topic_name, rc);

    ty_pool_class_info_t info[TACHYON_SIZE_CLASSES];
    memset(info, 0, sizeof info);
    ty_pool_info(t, info, TACHYON_SIZE_CLASSES);

    printf("%-12s %10s %10s %8s\n", "CLASS", "COUNT", "FREE", "USED%");
    for (size_t i = 0; i < TACHYON_SIZE_CLASSES; ++i) {
        double pct = info[i].chunk_count
                   ? 100.0 * (double)(info[i].chunk_count - info[i].free_count)
                         / (double)info[i].chunk_count
                   : 0.0;
        printf("%-12llu %10llu %10llu %7.1f%%\n",
               (unsigned long long)info[i].chunk_size,
               (unsigned long long)info[i].chunk_count,
               (unsigned long long)info[i].free_count, pct);
    }
    printf("\nclasses are filled from the smallest that fits; a loan may borrow\n"
           "upward when its own class is empty.\n");

    ty_topic_close(t);
    ty_region_close(reg);
}

static void cmd_watch(const char* region, const char* topic_name, int interval_ms) {
    ty_region_t* reg = attach(region);
    ty_topic_t* t = NULL;
    ty_err_t rc = ty_topic_open(reg, topic_name, &t);
    if (rc != TY_OK) die(topic_name, rc);

    printf("watching %s/%s every %d ms (ctrl-c to stop)\n",
           region, topic_name, interval_ms);
    printf("%12s %12s %12s %12s %12s\n",
           "PUB/s", "CONSUMED/s", "DEPTH", "DROPPED", "RECLAIMED");

    ty_stats_t prev, cur;
    ty_topic_stats(t, &prev);

    for (;;) {
        usleep((useconds_t)interval_ms * 1000);
        ty_topic_stats(t, &cur);
        uint64_t depth = 0;
        ty_depth(t, -1, &depth);

        double secs = (double)interval_ms / 1000.0;
        printf("%12.0f %12.0f %12llu %12llu %12llu\n",
               (double)(cur.published - prev.published) / secs,
               (double)(cur.consumed - prev.consumed) / secs,
               (unsigned long long)depth,
               (unsigned long long)(cur.dropped - prev.dropped),
               (unsigned long long)(cur.reclaimed - prev.reclaimed));
        prev = cur;
    }
}

static void usage(const char* prog) {
    fprintf(stderr,
        "usage: %s <command> [args]\n\n"
        "  list  <region>                topics and occupancy\n"
        "  stats <region> <topic>        counter snapshot\n"
        "  pool  <region> <topic>        per-class chunk usage\n"
        "  watch <region> <topic> [ms]   live rates (default 1000ms)\n",
        prog);
    exit(2);
}

int main(int argc, char** argv) {
    if (argc < 3) usage(argv[0]);

    const char* cmd = argv[1];
    const char* region = argv[2];

    if (!strcmp(cmd, "list")) {
        cmd_list(region);
    } else if (!strcmp(cmd, "stats") && argc >= 4) {
        cmd_stats(region, argv[3]);
    } else if (!strcmp(cmd, "pool") && argc >= 4) {
        cmd_pool(region, argv[3]);
    } else if (!strcmp(cmd, "watch") && argc >= 4) {
        cmd_watch(region, argv[3], argc >= 5 ? atoi(argv[4]) : 1000);
    } else {
        usage(argv[0]);
    }
    return 0;
}
