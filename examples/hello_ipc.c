/*
 * hello_ipc.c -- the smallest program that proves the point.
 *
 * Forks a child, publishes one message from the parent, and lets the child
 * read it out of shared memory. There is no socket, no pipe, and no copy:
 * the bytes the parent wrote are the exact bytes the child reads.
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

int main(void) {
    /* Start from a clean slate so re-running the example is idempotent. */
    ty_region_remove("hello", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 16u << 20;      /* 16 MiB */
    rc.create       = 1;
    rc.spawn_reaper = 0;              /* single short-lived demo, no sweeper */
    check("region_open", ty_region_open("hello", &rc, &reg));

    ty_topic_t* topic = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 4;
    tc.capacity   = 256;
    tc.pool_bytes = 4u << 20;
    tc.notify     = TY_NOTIFY_HYBRID;
    check("topic_create", ty_topic_create(reg, "greetings", &tc, &topic));

    printf("topic '%s': %u partitions, %u slots each\n",
           ty_topic_name(topic), ty_topic_partitions(topic),
           ty_topic_capacity(topic));

    pid_t pid = fork();
    if (pid == 0) {
        /* Child attaches to the same region, exactly as a separate service
         * would -- no inherited handles, no shared file descriptors. */
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create       = 0;
        cc.spawn_reaper = 0;
        check("child attach", ty_region_open("hello", &cc, &creg));

        ty_topic_t* ct = NULL;
        check("child topic", ty_topic_open(creg, "greetings", &ct));

        ty_msg_t m;
        ty_err_t r = ty_subscribe_any(ct, NULL, &m, 2000000000LL);
        if (r == TY_OK) {
            printf("child received: \"%.*s\" (len=%u key=%llu partition=%u)\n",
                   (int)m.len, (const char*)m.data, m.len,
                   (unsigned long long)m.key, m.partition);
            ty_release(ct, &m);
        } else {
            printf("child: nothing arrived (%s)\n", ty_strerror(r));
        }
        ty_region_close(creg);
        fflush(stdout);
        _exit(0);
    }

    const char* greeting = "hello from the parent process";
    check("send", ty_send(topic, greeting, strlen(greeting) + 1, 42));
    printf("parent sent: \"%s\" (routed to partition %u)\n",
           greeting, ty_route(topic, 42));

    int status = 0;
    waitpid(pid, &status, 0);

    ty_topic_close(topic);
    ty_region_close(reg);
    ty_region_remove("hello", NULL);
    return 0;
}
