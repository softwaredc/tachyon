/*
 * util.c -- clock sources, futex glue, hashing, process liveness.
 *
 * Everything here exists because the "obvious" libc version costs too much:
 * clock_gettime goes through a vDSO call we can often skip, backtrace-friendly
 * slow paths show up in every benchmark, and TSC conversion is the difference
 * between 20ns and 2ns per timestamp.
 */
#define _GNU_SOURCE
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#ifndef SYS_futex
#  if defined(__x86_64__)
#    define SYS_futex 202
#  elif defined(__aarch64__)
#    define SYS_futex 98
#  else
#    error "SYS_futex undefined for this architecture"
#  endif
#endif

#define FUTEX_WAIT_ 0
#define FUTEX_WAKE_ 1

const uint64_t ty_size_classes[TY_SIZE_CLASSES] = {
    64u, 256u, 1024u, 4096u, 16384u, 65536u, 262144u, 1048576u
};

const ty_region_conf_t ty_region_conf_default = {
    .size         = 64u * 1024u * 1024u,
    .create       = 1,
    .exclusive    = 0,
    .hugepage     = 0,
    .spawn_reaper = 1,
    .lease_ns     = 0, /* -> TY_LEASE_NS_DEFAULT */
    .dir          = NULL
};

const ty_topic_conf_t ty_topic_conf_default = {
    .partitions = 8,
    .capacity   = 1024,
    .pool_bytes = 16u * 1024u * 1024u,
    .notify     = TY_NOTIFY_HYBRID,
    .flags      = 0
};

#define TY_LEASE_NS_DEFAULT (5ull * 1000ull * 1000ull * 1000ull)

/* ------------------------------------------------------------------ errors */

const char* ty_strerror(ty_err_t err) {
    switch (err) {
    case TY_OK:           return "ok";
    case TY_ERR_INVAL:    return "invalid argument";
    case TY_ERR_EXIST:    return "already exists";
    case TY_ERR_NOENT:    return "no such region or topic";
    case TY_ERR_NOMEM:    return "not enough room in the pool or region";
    case TY_ERR_FULL:     return "partition ring is full";
    case TY_ERR_EMPTY:    return "no message available";
    case TY_ERR_TIMEOUT:  return "deadline expired";
    case TY_ERR_TOOBIG:   return "payload exceeds the largest size class";
    case TY_ERR_STATE:    return "object is in the wrong state";
    case TY_ERR_ABI:      return "region created by an incompatible version";
    case TY_ERR_IO:       return "operating system error";
    case TY_ERR_MAP:      return "mmap failed";
    case TY_ERR_BUSY:     return "resource is busy";
    case TY_ERR_INTERNAL: return "internal invariant violated";
    default:              return "unknown error";
    }
}

/* ------------------------------------------------------------------- clock */

static inline uint64_t ty_rdtsc(void) {
#if defined(__x86_64__)
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    /* The fence keeps the CPU from hoisting the read above the work we are
     * trying to time; rdtsc is not ordered on its own. */
    __asm__ __volatile__("lfence" ::: "memory");
    return ((uint64_t)hi << 32) | (uint64_t)lo;
#elif defined(__aarch64__)
    uint64_t val;
    __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(val));
    return val;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

/* ARM's counter has a fixed denominator read from CNTFRQ, so no calibration
 * drift is possible. x86 needs a measured ratio. */
static uint64_t g_tsc_freq_hz = 0;

__attribute__((unused))
static uint64_t ty_read_cntfrq(void) {
    FILE* f = fopen("/sys/devices/system/cpu/cpu0/regs/counter_freq", "rb");
    if (f) {
        uint64_t v = 0;
        if (fscanf(f, "%llu", (unsigned long long*)&v) == 1 && v > 0) {
            fclose(f);
            return v;
        }
        fclose(f);
    }
    f = fopen("/proc/device-tree/timer/stub", "rb"); /* not a thing, keep going */
    return 0;
}

int ty_tsc_usable(void) {
    static int cached = -1;
    if (cached >= 0) return cached;

    int usable = 0;
#if defined(__x86_64__)
    FILE* f = fopen("/proc/cpuinfo", "rb");
    if (f) {
        char line[512];
        int constant = 0, nonstop = 0;
        while (fgets(line, sizeof line, f)) {
            if (strncmp(line, "flags", 5) == 0) {
                if (strstr(line, " constant_tsc")) constant = 1;
                if (strstr(line, " nonstop_tsc")) nonstop = 1;
            }
        }
        fclose(f);
        usable = constant && nonstop;
    }
#elif defined(__aarch64__)
    usable = ty_read_cntfrq() > 0;
#endif
    cached = usable;
    return usable;
}

static pthread_once_t g_clock_once = PTHREAD_ONCE_INIT;

static void ty_calibrate_once(void) {
#if defined(__aarch64__)
    g_tsc_freq_hz = ty_read_cntfrq();
    if (!g_tsc_freq_hz) g_tsc_freq_hz = 1000000000ull; /* best effort */
#else
    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts0);
    uint64_t c0 = ty_rdtsc();
    struct timespec target = ts0;
    target.tv_nsec += 20000000; /* 20ms of drift-free sampling */
    if (target.tv_nsec >= 1000000000L) {
        target.tv_nsec -= 1000000000L;
        target.tv_sec += 1;
    }
    clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &target, NULL);
    uint64_t c1 = ty_rdtsc();
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts1);

    uint64_t ns0 = (uint64_t)ts0.tv_sec * 1000000000ull + (uint64_t)ts0.tv_nsec;
    uint64_t ns1 = (uint64_t)ts1.tv_sec * 1000000000ull + (uint64_t)ts1.tv_nsec;
    if (c1 > c0 && ns1 > ns0) {
        long double ticks_per_ns = (long double)(c1 - c0) / (long double)(ns1 - ns0);
        g_tsc_freq_hz = (uint64_t)(ticks_per_ns * 1000000000.0L);
    }
    if (!g_tsc_freq_hz) g_tsc_freq_hz = 3000000000ull;
#endif
}

uint64_t ty_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

uint64_t ty_now_ns_fast(void) {
    pthread_once(&g_clock_once, ty_calibrate_once);
    if (!g_tsc_freq_hz) return ty_now_ns();
    return (uint64_t)(((long double)ty_rdtsc() * 1000000000.0L) / (long double)g_tsc_freq_hz);
}

uint64_t ty__now_ns(void) { return ty_now_ns(); }

int ty_cpu_count(void) {
    static int n = 0;
    if (n > 0) return n;
    long v = sysconf(_SC_NPROCESSORS_ONLN);
    n = (v > 0) ? (int)v : 1;
    return n;
}

/* -------------------------------------------------------------- size class */

size_t ty_size_class(size_t index) {
    if (index >= TY_SIZE_CLASSES) return 0;
    return (size_t)ty_size_classes[index];
}

size_t ty_size_class_count(void) { return TY_SIZE_CLASSES; }

/* ------------------------------------------------------------------- futex */

int ty__futex_wait(ty_atomic_u32* word, uint32_t expected, int64_t timeout_ns) {
    struct timespec ts, *pts = NULL;
    if (timeout_ns >= 0) {
        ts.tv_sec  = (time_t)(timeout_ns / 1000000000LL);
        ts.tv_nsec = (long)(timeout_ns % 1000000000LL);
        pts = &ts;
    }
    errno = 0;
    long rc = syscall(SYS_futex, (void*)word, FUTEX_WAIT_, (int)expected,
                      pts, NULL, 0);
    if (rc == 0) return 0;
    return (errno == ETIMEDOUT) ? ETIMEDOUT : errno;
}

int ty__futex_wake(ty_atomic_u32* word, uint32_t count) {
    long rc = syscall(SYS_futex, (void*)word, FUTEX_WAKE_, (int)count,
                      NULL, NULL, 0);
    if (rc >= 0) return (int)rc;
    return -errno;
}

/* --------------------------------------------------------------- liveness */

int ty__pid_alive(int pid) {
    if (pid <= 0) return 0;
    int rc = kill(pid, 0);
    if (rc == 0) return 1;
    /* ESRCH: genuinely gone. EPERM: alive but owned by another uid. */
    return (errno == EPERM) ? 1 : 0;
}

/* --------------------------------------------------------------- Heartbeat */

void ty__touch_heartbeat(ty_region_t* reg) {
    if (!reg || !reg->hdr || reg->proc_slot < 0) return;
    ty_proc_slot_t* slot = &reg->hdr->procs[reg->proc_slot];
    TY_STORE_RELAXED(&slot->heartbeat_ns, ty_now_ns());
}

void ty_heartbeat(ty_region_t* reg) { ty__touch_heartbeat(reg); }
