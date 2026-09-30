/*
 * tachyond -- standalone reaper daemon.
 *
 * Sweeps every topic in a region and reclaims chunks abandoned by processes
 * that were killed. Run one per region; more than one is harmless because
 * ty_reclaim_once() is a trylock and only one caller does the work.
 *
 * Prefer this over the in-process reaper thread when your application forks:
 * a background thread plus fork() is a well known source of deadlocks in the
 * child, and a separate process avoids the question entirely.
 *
 *   tachyond <region> [interval_ms]
 */
/* usleep() is not visible under strict -std=c11 without a feature macro. */
#define _DEFAULT_SOURCE

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <region> [interval_ms]\n", argv[0]);
        return 2;
    }

    const char* region_name = argv[1];
    int interval_ms = (argc >= 3) ? atoi(argv[2]) : 250;
    if (interval_ms < 10)  interval_ms = 10;
    if (interval_ms > 60000) interval_ms = 60000;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    ty_region_t* reg = NULL;
    ty_region_conf_t conf = ty_region_conf_default;
    conf.create       = 0;          /* must already exist */
    conf.spawn_reaper = 0;          /* we are the reaper */
    ty_err_t rc = ty_region_open(region_name, &conf, &reg);
    if (rc != TY_OK) {
        fprintf(stderr, "tachyond: cannot attach to '%s': %s\n",
                region_name, ty_strerror(rc));
        return 1;
    }

    printf("tachyond: watching region '%s' every %d ms\n",
           region_name, interval_ms);
    fflush(stdout);

    char(*names)[TACHYON_MAX_NAME + 1] = calloc(TACHYON_MAX_TOPICS,
                                                TACHYON_MAX_NAME + 1);
    if (!names) {
        fprintf(stderr, "tachyond: out of memory\n");
        return 1;
    }

    /* Zeroed every round: a topic that fails to open must leave a NULL slot,
     * or the close loop below would read an indeterminate pointer. */
    ty_topic_t* topics[TACHYON_MAX_TOPICS];

    while (!g_stop) {
        memset(topics, 0, sizeof topics);
        int n = ty_topic_list(reg, names, TACHYON_MAX_TOPICS);
        if (n < 0) {
            fprintf(stderr, "tachyond: topic list failed (%s)\n",
                    ty_strerror((ty_err_t)n));
            break;
        }

        uint64_t total = 0;
        for (int i = 0; i < n && i < TACHYON_MAX_TOPICS; ++i) {
            ty_topic_t* t = NULL;
            /* Open handles once and keep them: topics cannot disappear while
             * we hold the region, and re-opening every tick is wasted work. */
            if (ty_topic_open(reg, names[i], &t) != TY_OK) continue;
            total += ty_reclaim_once(t);
            topics[i] = t;
        }

        if (total > 0) {
            printf("tachyond: reclaimed %llu chunk(s)\n",
                   (unsigned long long)total);
            fflush(stdout);
        }

        for (int i = 0; i < n && i < TACHYON_MAX_TOPICS; ++i) {
            if (topics[i]) ty_topic_close(topics[i]);
        }

        usleep((useconds_t)interval_ms * 1000);
    }

    printf("tachyond: stopping\n");
    free(names);
    ty_region_close(reg);
    return 0;
}
