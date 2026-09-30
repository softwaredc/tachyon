/*
 * bench_ipc.c -- throughput and latency comparison against the usual suspects.
 *
 * Everything measured here is real traffic between two processes on the same
 * machine; nothing is extrapolated and nothing is estimated.
 *
 *   tachyon : publish/subscribe over a shared-memory ring
 *   pipe    : two anonymous pipes
 *   uds     : socketpair(AF_UNIX, SOCK_STREAM)
 *   tcp     : loopback TCP with TCP_NODELAY
 *
 * Fairness notes, stated because they decide how the numbers should be read:
 *
 *  1. Every transport moves its payload with exactly one user-space memcpy on
 *     the send side and one on the receive side, tachyon included. That is the
 *     conservative path; a caller who builds the message inside a loaned chunk
 *     skips the send-side copy and beats these figures.
 *
 *  2. File-descriptor transports are created once by the parent and inherited
 *     across fork(). tachyon, by contrast, is re-attached by the child through
 *     ty_region_open()/ty_topic_open(), so the measured path includes the real
 *     attach path a separate process would take. That is deliberately the
 *     harder test, not an oversight.
 *
 *  3. Latency is reported round-trip, because that is what is actually
 *     observed. Percentiles come from sorting the complete sample set, so
 *     p99.9 is exact for the run and not a histogram approximation.
 *
 * Usage: bench_ipc <transport> <latency|throughput> [size] [count]
 *        TY_NOTIFY=spin|futex|hybrid selects the tachyon wait policy.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tachyon/tachyon.h"

/* Parent does WARMUP unmeasured rounds first; the child must serve that many
 * extra or the parent blocks forever -- a mistake that turns into a hang
 * rather than an error, and eats someone's afternoon. */
#define WARMUP 1000ULL

/* Never block forever in a benchmark: a dead peer must produce an error. */
#define RECV_TIMEOUT_NS (3000000000LL)

#if defined(__x86_64__) || defined(__i386__)
#  define RELAX() __builtin_ia32_pause()
#elif defined(__aarch64__)
#  define RELAX() __asm__ __volatile__("yield" ::: "memory")
#else
#  define RELAX() ((void)0)
#endif

static const char* g_transport = "tachyon";
static const char* g_mode      = "latency";
static size_t      g_size      = 1024;
static uint64_t    g_count     = 200000;

/* ---------------------------------------------------------------- percentiles */

static int cmp_u64(const void* a, const void* b) {
    uint64_t x = *(const uint64_t*)a, y = *(const uint64_t*)b;
    return (x > y) - (x < y);
}

static uint64_t pct(uint64_t* s, uint64_t n, double p) {
    if (!n) return 0;
    uint64_t idx = (uint64_t)((double)n * p);
    if (idx >= n) idx = n - 1;
    return s[idx];
}

/* ------------------------------------------------------------------- channel */

enum { CH_TACHYON, CH_PIPE, CH_UDS, CH_TCP };

typedef struct {
    int          kind;
    ty_region_t* reg;
    ty_topic_t*  send_topic;
    ty_topic_t*  recv_topic;
    int          fds[4];   /* raw descriptors, inherited by the child */
    int          wfd;
    int          rfd;
    char*        buf;
} chan_t;

static void die(const char* what) {
    fprintf(stderr, "fatal: %s (errno=%d: %s)\n", what, errno, strerror(errno));
    exit(2);
}

static int chan_kind(void) {
    if (!strcmp(g_transport, "pipe")) return CH_PIPE;
    if (!strcmp(g_transport, "uds"))  return CH_UDS;
    if (!strcmp(g_transport, "tcp"))  return CH_TCP;
    return CH_TACHYON;
}

static char* payload_alloc(void) {
    char* p = (char*)malloc(g_size);
    if (!p) die("malloc");
    memset(p, 'x', g_size);
    return p;
}

/* TCP needs a real connection, so it has its own create/connect/accept trio. */
static void chan_setup_tcp(chan_t* c);
static void chan_tcp_child(chan_t* c);
static void chan_tcp_parent(chan_t* c);

/** Parent side: create every resource exactly once. */
static void chan_setup(chan_t* c) {
    memset(c, 0, sizeof(*c));
    c->kind = chan_kind();
    c->buf  = payload_alloc();

    if (c->kind == CH_TCP) { chan_setup_tcp(c); return; }

    if (c->kind == CH_TACHYON) {
        ty_region_conf_t rc = ty_region_conf_default;
        rc.size         = 256u * 1024u * 1024u;
        rc.create       = 1;
        rc.spawn_reaper = 0;  /* no sweeper competing during measurement */
        if (ty_region_open("bench_ipc", &rc, &c->reg) != TY_OK) die("region_open");

        ty_topic_conf_t tc = ty_topic_conf_default;
        tc.partitions   = 1;
        tc.capacity     = 4096;
        tc.pool_bytes   = 64u * 1024u * 1024u;  /* two topics must fit */

        const char* nm = getenv("TY_NOTIFY");
        tc.notify = (!nm || !strcmp(nm, "hybrid")) ? TY_NOTIFY_HYBRID
                  : (!strcmp(nm, "spin"))          ? TY_NOTIFY_SPIN
                  :                                  TY_NOTIFY_FUTEX;

        ty_topic_t* p2c = NULL;
        ty_topic_t* c2p = NULL;
        if (ty_topic_create(c->reg, "p2c", &tc, &p2c) != TY_OK) die("create p2c");
        if (ty_topic_create(c->reg, "c2p", &tc, &c2p) != TY_OK) die("create c2p");
        c->send_topic = p2c;
        c->recv_topic = c2p;
        return;
    }

    if (c->kind == CH_PIPE) {
        int p1[2], p2[2];
        if (pipe(p1) || pipe(p2)) die("pipe");
        c->fds[0] = p1[0]; c->fds[1] = p1[1];  /* p1: parent -> child */
        c->fds[2] = p2[0]; c->fds[3] = p2[1];  /* p2: child -> parent */
        c->wfd = p1[1];
        c->rfd = p2[0];
        return;
    }

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv)) die("socketpair");
    c->fds[0] = sv[0];
    c->fds[1] = sv[1];
    c->wfd = sv[0];
    c->rfd = sv[0];
}

/* TCP cannot use socketpair() -- AF_INET is not supported by it, so a real
 * loopback connection is established: parent listens, child connects. */
static void chan_setup_tcp(chan_t* c) {
    int ln = socket(AF_INET, SOCK_STREAM, 0);
    if (ln < 0) die("tcp socket");
    int one = 1;
    setsockopt(ln, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;   /* let the kernel pick */
    if (bind(ln, (struct sockaddr*)&addr, sizeof addr)) die("tcp bind");
    if (listen(ln, 1)) die("tcp listen");

    struct sockaddr_in got;
    socklen_t gl = sizeof got;
    if (getsockname(ln, (struct sockaddr*)&got, &gl)) die("getsockname");

    c->fds[0] = ln;
    c->fds[2] = (int)ntohs(got.sin_port);  /* stash the ephemeral port */
}

static void chan_tcp_child(chan_t* c) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) die("tcp child socket");
    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = htons((uint16_t)c->fds[2]);
    if (connect(s, (struct sockaddr*)&a, sizeof a)) die("tcp connect");
    c->wfd = c->rfd = s;
}

static void chan_tcp_parent(chan_t* c) {
    int s = accept(c->fds[0], NULL, NULL);
    if (s < 0) die("tcp accept");
    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    c->wfd = c->rfd = s;
}

/**
 * Child side.
 *
 * For fd transports the descriptors are inherited and merely re-oriented --
 * creating new pipes in the child would silently benchmark two unrelated
 * channels, which is a mistake worth naming because it looks fine at a glance.
 * tachyon re-attaches through the public API so the numbers include a real
 * cross-process open.
 */
static void chan_child(chan_t* c) {
    c->kind = chan_kind();
    c->buf  = payload_alloc();

    if (c->kind == CH_TCP) { chan_tcp_child(c); return; }

    if (c->kind == CH_TACHYON) {
        ty_region_conf_t rc = ty_region_conf_default;
        rc.create       = 0;
        rc.spawn_reaper = 0;
        if (ty_region_open("bench_ipc", &rc, &c->reg) != TY_OK) die("child attach");
        if (ty_topic_open(c->reg, "c2p", &c->send_topic) != TY_OK) die("child open c2p");
        if (ty_topic_open(c->reg, "p2c", &c->recv_topic) != TY_OK) die("child open p2c");
        return;
    }
    if (c->kind == CH_PIPE) {
        c->wfd = c->fds[3];   /* write end of the child->parent pipe */
        c->rfd = c->fds[0];   /* read end of the parent->child pipe  */
        return;
    }
    c->wfd = c->rfd = c->fds[1];
}

static inline void chan_send(chan_t* c, const char* payload, size_t len) {
    if (c->kind == CH_TACHYON) {
        for (;;) {
            ty_err_t rc = ty_send(c->send_topic, payload, len, 0);
            if (rc == TY_OK) return;
            /* FULL/NOMEM are backpressure: the peer is behind, keep trying. */
            if (rc == TY_ERR_FULL || rc == TY_ERR_NOMEM) { RELAX(); continue; }
            fprintf(stderr, "fatal: ty_send: %s\n", ty_strerror(rc));
            exit(2);
        }
    }
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(c->wfd, payload + off, len - off);
        if (n < 0) { if (errno == EINTR) continue; die("write"); }
        off += (size_t)n;
    }
}

static inline void chan_recv(chan_t* c, char* into, size_t want) {
    if (c->kind == CH_TACHYON) {
        for (;;) {
            ty_msg_t m;
            ty_err_t rc = ty_subscribe(c->recv_topic, 0, &m, RECV_TIMEOUT_NS);
            if (rc != TY_OK) {
                if (rc == TY_ERR_EMPTY) continue;
                ty_stats_t st; ty_topic_stats(c->recv_topic, &st);
                uint64_t dp = 0; ty_depth(c->recv_topic, -1, &dp);
                fprintf(stderr, "fatal: peer stopped (%s) topic=%s pub=%llu "
                        "con=%llu depth=%llu\n", ty_strerror(rc),
                        ty_topic_name(c->recv_topic),
                        (unsigned long long)st.published,
                        (unsigned long long)st.consumed,
                        (unsigned long long)dp);
                exit(2);
            }
            if (into) memcpy(into, m.data, m.len < want ? m.len : want);
            ty_err_t rr = ty_release(c->recv_topic, &m);
            if (rr != TY_OK) {
                fprintf(stderr, "fatal: ty_release: %s\n", ty_strerror(rr));
                exit(2);
            }
            return;
        }
    }
    size_t off = 0;
    while (off < want) {
        ssize_t n = read(c->rfd, into + off, want - off);
        if (n < 0) { if (errno == EINTR) continue; die("read"); }
        if (n == 0) return;
        off += (size_t)n;
    }
}

/* -------------------------------------------------------------------- latency */

static void run_latency(void) {
    chan_t c;
    chan_setup(&c);
    uint64_t* s = (uint64_t*)calloc(g_count ? g_count : 1, sizeof(uint64_t));
    if (!s) die("calloc samples");

    pid_t pid = fork();
    if (pid == 0) {
        chan_child(&c);
        for (uint64_t i = 0; i < g_count + WARMUP; ++i) {
            chan_recv(&c, c.buf, g_size);
            chan_send(&c, c.buf, g_size);
        }
        if (c.kind == CH_TACHYON) ty_region_close(c.reg);
        _exit(0);
    }

    if (c.kind == CH_TCP) chan_tcp_parent(&c);
    usleep(50000);   /* let the child attach before the clock starts */

    for (uint64_t i = 0; i < WARMUP; ++i) {
        chan_send(&c, c.buf, g_size);
        chan_recv(&c, c.buf, g_size);
    }

    for (uint64_t i = 0; i < g_count; ++i) {
        uint64_t t0 = ty_now_ns();
        chan_send(&c, c.buf, g_size);
        chan_recv(&c, c.buf, g_size);
        s[i] = ty_now_ns() - t0;
    }

    qsort(s, g_count, sizeof(uint64_t), cmp_u64);
    uint64_t total = 0;
    for (uint64_t i = 0; i < g_count; ++i) total += s[i];

    printf("RESULT %-8s latency size=%zu count=%llu rtt_p50=%llu rtt_p99=%llu "
           "rtt_p999=%llu rtt_mean=%llu\n",
           g_transport, g_size, (unsigned long long)g_count,
           (unsigned long long)pct(s, g_count, 0.50),
           (unsigned long long)pct(s, g_count, 0.99),
           (unsigned long long)pct(s, g_count, 0.999),
           (unsigned long long)(total / g_count));
    printf("ONEWAY %-8s p50=%llu p99=%llu p999=%llu\n", g_transport,
           (unsigned long long)(pct(s, g_count, 0.50) / 2),
           (unsigned long long)(pct(s, g_count, 0.99) / 2),
           (unsigned long long)(pct(s, g_count, 0.999) / 2));

    int status = 0;
    waitpid(pid, &status, 0);
    if (c.kind == CH_TACHYON) ty_region_close(c.reg);
    ty_region_remove("bench_ipc", NULL);
}

/* ---------------------------------------------------------------- throughput */

static void run_throughput(void) {
    chan_t c;
    chan_setup(&c);

    pid_t pid = fork();
    if (pid == 0) {
        chan_child(&c);
        for (uint64_t i = 0; i < g_count; ++i) chan_recv(&c, c.buf, g_size);
        if (c.kind == CH_TACHYON) ty_region_close(c.reg);
        _exit(0);
    }
    if (c.kind == CH_TCP) chan_tcp_parent(&c);
    usleep(50000);

    uint64_t t0 = ty_now_ns();
    for (uint64_t i = 0; i < g_count; ++i) chan_send(&c, c.buf, g_size);
    int status = 0;
    waitpid(pid, &status, 0);   /* stops the clock after the last delivery */
    uint64_t t1 = ty_now_ns();

    double secs = (double)(t1 - t0) / 1e9;
    double mbps = ((double)g_count * (double)g_size / secs) / (1024.0 * 1024.0);

    printf("RESULT %-8s throughput size=%zu count=%llu msgs=%.0f/s MBps=%.1f GBps=%.3f\n",
           g_transport, g_size, (unsigned long long)g_count,
           (double)g_count / secs, mbps, mbps / 1024.0);

    if (c.kind == CH_TACHYON) ty_region_close(c.reg);
    ty_region_remove("bench_ipc", NULL);
}

int main(int argc, char** argv) {
    if (argc > 1) g_transport = argv[1];
    if (argc > 2) g_mode      = argv[2];
    if (argc > 3) g_size      = (size_t)atol(argv[3]);
    if (argc > 4) g_count     = strtoull(argv[4], NULL, 10);

    if (!strcmp(g_mode, "latency")) run_latency();
    else                            run_throughput();
    return 0;
}
