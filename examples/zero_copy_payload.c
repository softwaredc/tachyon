/*
 * zero_copy_payload.c -- the loan/publish path, and why it exists.
 *
 * ty_send() copies your buffer into shared memory. ty_loan() hands you a
 * pointer *into* shared memory instead, so you can build the message there
 * directly and publish it with no copy at all. For a 4 KiB message produced by
 * serialisation, that is one less memcpy per message.
 *
 * The example writes a struct straight into the loaned chunk, publishes it,
 * and separately shows ty_abort(), which is what you call when you have
 * borrowed a chunk and then changed your mind (validation failed, downstream
 * unavailable, and so on). Aborting returns the chunk to the pool immediately;
 * forgetting to do so costs you a lease timeout before the reaper recovers it.
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

typedef struct {
    uint64_t order_id;
    uint64_t user_id;
    double   price;
    char     symbol[8];
} order_t;

int main(void) {
    ty_region_remove("zerocopy", NULL);

    ty_region_t* reg = NULL;
    ty_region_conf_t rc = ty_region_conf_default;
    rc.size         = 32u << 20;
    rc.create       = 1;
    rc.spawn_reaper = 0;
    check("region_open", ty_region_open("zerocopy", &rc, &reg));

    ty_topic_t* t = NULL;
    ty_topic_conf_t tc = ty_topic_conf_default;
    tc.partitions = 2;
    tc.capacity   = 64;
    tc.pool_bytes = 8u << 20;
    tc.notify     = TY_NOTIFY_HYBRID;
    check("topic_create", ty_topic_create(reg, "orders", &tc, &t));

    /* --- loan + build in place + publish ------------------------------- */
    void*  ptr = NULL;
    size_t cap = 0;
    check("loan", ty_loan(t, sizeof(order_t), &ptr, &cap));
    printf("loaned %p, usable capacity %zu bytes (asked for %zu)\n",
           ptr, cap, sizeof(order_t));

    order_t* o = (order_t*)ptr;   /* the struct lives in shared memory now */
    o->order_id = 20261001;
    o->user_id  = 9911;
    o->price    = 412.75;
    memcpy(o->symbol, "AAPL", 5);

    check("publish", ty_publish(t, ptr, sizeof(order_t), /*key=*/o->user_id));
    printf("published order %llu to partition %u\n",
           (unsigned long long)o->order_id, ty_route(t, o->user_id));

    /* --- loan + abort --------------------------------------------------- */
    void* scratch = NULL;
    check("loan2", ty_loan(t, 4096, &scratch, NULL));
    memset(scratch, 0, 4096);
    printf("borrowed a 4 KiB chunk, then aborted it\n");
    check("abort", ty_abort(t, scratch));

    /* --- read it back in a child process -------------------------------- */
    pid_t pid = fork();
    if (pid == 0) {
        ty_region_t* creg = NULL;
        ty_region_conf_t cc = ty_region_conf_default;
        cc.create       = 0;
        cc.spawn_reaper = 0;
        check("child attach", ty_region_open("zerocopy", &cc, &creg));
        ty_topic_t* ct = NULL;
        check("child topic", ty_topic_open(creg, "orders", &ct));

        ty_msg_t m;
        ty_err_t r = ty_subscribe_any(ct, NULL, &m, 2000000000LL);
        if (r != TY_OK) {
            fprintf(stderr, "child: nothing arrived (%s)\n", ty_strerror(r));
            fflush(stdout);
            _exit(1);
        }
        const order_t* got = (const order_t*)m.data;
        printf("child read order %llu user=%llu %.2f %s "
               "(same physical bytes, no copy)\n",
               (unsigned long long)got->order_id,
               (unsigned long long)got->user_id, got->price, got->symbol);
        ty_release(ct, &m);
        ty_region_close(creg);
        fflush(stdout);
        _exit(0);
    }

    int status = 0;
    waitpid(pid, &status, 0);

    ty_pool_class_info_t info[8];
    memset(info, 0, sizeof info);
    ty_pool_info(t, info, 8);
    printf("pool after the run:\n");
    for (int i = 0; i < 8; ++i)
        if (info[i].chunk_count)
            printf("  class %d: %6llu B x %5llu chunks, %5llu free\n", i,
                   (unsigned long long)info[i].chunk_size,
                   (unsigned long long)info[i].chunk_count,
                   (unsigned long long)info[i].free_count);

    ty_topic_close(t);
    ty_region_close(reg);
    ty_region_remove("zerocopy", NULL);
    return 0;
}
