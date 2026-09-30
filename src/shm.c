/*
 * shm.c -- region lifecycle: create, map, attach, tear down.
 *
 * The region is one mapping shared by every participant. Two naming modes are
 * supported because they solve different problems:
 *
 *   shm_open("/tachyon_<name>")   default; lives in /dev/shm, is cleaned up by
 *                                 the OS on reboot, and is visible to any
 *                                 process with the same uid.
 *   open(dir/name)                when conf->dir is set, useful when you want
 *                                 the region on a specific tmpfs, or inside a
 *                                 container mount that is private to you.
 *
 * Note what this file deliberately does NOT do: there is no background thread
 * holding locks, no reference count maintained in a side channel, no cleanup
 * hook. The only cross-process resources are the mapping and the process table
 * inside it, and both survive their creator exiting -- which is the entire
 * point. A region outliving its creator is a feature: a producer can publish,
 * exit, and its messages remain readable.
 */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define TY_LEASE_NS_DEFAULT (5ull * 1000000000ull)
#define TY_REAPER_TICK_MIN_NS  (50ull * 1000000ull)

/* ------------------------------------------------------------------ helpers */

static int ty_valid_name(const char* name) {
    if (!name || !*name) return 0;
    size_t n = strlen(name);
    if (n == 0 || n > TACHYON_MAX_NAME) return 0;
    for (size_t i = 0; i < n; ++i) {
        char c = name[i];
        if (c == '/' || c == '\\' || c == ' ' || c == '\t' || c == '\n') return 0;
    }
    return 1;
}

static void ty_build_path(const char* name, const char* dir,
                          char* buf, size_t cap) {
    if (dir && *dir) {
        snprintf(buf, cap, "%s/%s", dir, name);
    } else {
        snprintf(buf, cap, "/tachyon_%s", name);
    }
}

/* ------------------------------------------------------------ process table */

int ty__proc_slot_acquire(ty_region_t* reg) {
    ty_proc_slot_t* slots = (ty_proc_slot_t*)TY_PTR(reg, TY_PROCS_OFFSET);
    int pid = (int)getpid();

    for (int i = 0; i < TACHYON_MAX_PROCS; ++i) {
        uint32_t expect = 0;
        if (TY_LOAD_RELAXED(&slots[i].state) == TY_PROC_LIVE) continue;
        if (atomic_compare_exchange_strong_explicit(&slots[i].state, &expect,
                (uint32_t)TY_PROC_LIVE,
                memory_order_acq_rel, memory_order_relaxed)) {
            TY_STORE_RELAXED(&slots[i].pid, pid);
            TY_STORE_RELAXED(&slots[i].heartbeat_ns, ty_now_ns());
            return i;
        }
    }
    return -1; /* table full: liveness proofs degrade to lease deadlines */
}

void ty__proc_slot_release(ty_region_t* reg) {
    if (reg->proc_slot < 0) return;
    ty_proc_slot_t* slots = (ty_proc_slot_t*)TY_PTR(reg, TY_PROCS_OFFSET);
    TY_STORE_RELAXED(&slots[reg->proc_slot].state, TY_PROC_FREE);
    TY_STORE_RELAXED(&slots[reg->proc_slot].pid, 0);
    reg->proc_slot = -1;
}

/* -------------------------------------------------------------- reaper loop */

/**
 * Sweeps every topic in the region for abandoned chunks.
 *
 * Runs at least four times per lease period, so a crashed writer's chunks come
 * back within ~lease_ns/4 in the worst case and immediately in the common case
 * (a provably dead pid needs no deadline at all).
 */
static void* ty_reaper_thread(void* arg) {
    ty_region_t* reg = (ty_region_t*)arg;
    uint64_t tick_ns = reg->lease_ns / 4;
    if (tick_ns < TY_REAPER_TICK_MIN_NS) tick_ns = TY_REAPER_TICK_MIN_NS;

    while (!TY_LOAD_RELAXED(&reg->closing)) {
        usleep((useconds_t)(tick_ns / 1000));

        /* Deadline-aware sleep would be nicer, but periodic is fine: the lease
         * itself is the correctness anchor, this is only about latency. */
        ty__touch_heartbeat(reg);
        TY_STORE_RELAXED(&reg->hdr->reaper_heartbeat_ns, ty_now_ns());

        uint64_t now = ty_now_ns();
        ty_topic_dir_t* dir = (ty_topic_dir_t*)TY_PTR(reg, TY_TOPICS_OFFSET);

        for (int i = 0; i < TACHYON_MAX_TOPICS; ++i) {
            if (TY_LOAD_RELAXED(&dir[i].state) != TY_TOPIC_READY) continue;

            ty_topic_shm_t* ts = (ty_topic_shm_t*)TY_PTR(reg, dir[i].off);
            if (ts->magic != TY_TOPIC_MAGIC) continue;

            /* Borrow a stack handle; no allocation on the reaper path. */
            struct ty_topic tmp;
            tmp.reg   = reg;
            tmp.shm   = ts;
            tmp.off   = dir[i].off;
            tmp.rings = (ty_ring_t*)TY_PTR(reg, ts->rings_off);
            tmp.pool  = (ty_pool_t*)TY_PTR(reg, ts->pool_off);
            snprintf(tmp.name, sizeof tmp.name, "%s", dir[i].name);

            ty__reclaim_topic(&tmp, now, 0);
        }
    }
    return NULL;
}

/* ------------------------------------------------------------- open / close */

ty_err_t ty_region_open(const char* name, const ty_region_conf_t* conf,
                        ty_region_t** out) {
    if (!ty_valid_name(name) || !out) return TY_ERR_INVAL;

    const ty_region_conf_t* c = conf ? conf : &ty_region_conf_default;
    size_t size = c->size ? c->size : ty_region_conf_default.size;
    size_t page = (size_t)sysconf(_SC_PAGESIZE);
    size = TY_ALIGN(size, page);

    ty_region_t* reg = (ty_region_t*)calloc(1, sizeof(ty_region_t));
    if (!reg) return TY_ERR_NOMEM;

    snprintf(reg->name, sizeof reg->name, "%s", name);
    reg->magic        = TY_HANDLE_MAGIC;
    reg->lease_ns     = c->lease_ns ? c->lease_ns : TY_LEASE_NS_DEFAULT;
    reg->spawn_reaper = c->spawn_reaper;
    reg->conf         = *c;
    reg->proc_slot    = -1;
    TY_STORE_RELAXED(&reg->handle_refs, 1u);  /* the opener's own reference */

    char path[256];
    ty_build_path(name, c->dir, path, sizeof path);
    snprintf(reg->shm_path, sizeof reg->shm_path, "%s", path);

    int created = 0;
    int fd;
    int use_shm = !(c->dir && *c->dir);
    reg->use_shm = use_shm;

    if (use_shm) {
        fd = shm_open(path, O_RDWR, 0666);
        if (fd < 0 && errno == ENOENT && c->create) {
            int flags = O_RDWR | O_CREAT | (c->exclusive ? O_EXCL : 0);
            fd = shm_open(path, flags, 0666);
            created = 1;
        }
    } else {
        fd = open(path, O_RDWR, 0666);
        if (fd < 0 && errno == ENOENT && c->create) {
            int flags = O_RDWR | O_CREAT | (c->exclusive ? O_EXCL : 0);
            fd = open(path, flags, 0666);
            created = 1;
        }
    }
    if (fd < 0) {
        free(reg);
        return (errno == EEXIST) ? TY_ERR_EXIST : TY_ERR_IO;
    }
    reg->fd      = fd;
    reg->created = created;

    if (created) {
        if (ftruncate(fd, (off_t)size) != 0) {
            close(fd);
            free(reg);
            return TY_ERR_IO;
        }
    } else {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            close(fd);
            free(reg);
            return TY_ERR_IO;
        }
        size = (size_t)st.st_size;
        if (size < sizeof(ty_region_hdr_t)) {
            close(fd);
            free(reg);
            return TY_ERR_ABI;
        }
    }
    reg->size = size;

    int prot = PROT_READ | PROT_WRITE;
    int mflags = MAP_SHARED;
#ifdef MAP_HUGETLB
    if (c->hugepage && created) mflags |= MAP_HUGETLB;
#endif
    void* base = mmap(NULL, size, prot, mflags, fd, 0);
    if (base == MAP_FAILED) {
#ifdef MAP_HUGETLB
        if (c->hugepage && created) {
            /* Huge pages are best-effort: silently fall back rather than
             * failing to open for a cosmetic optimisation. */
            base = mmap(NULL, size, prot, MAP_SHARED, fd, 0);
        }
#endif
        if (base == MAP_FAILED) {
            close(fd);
            free(reg);
            return TY_ERR_MAP;
        }
    }
    reg->base = base;
    reg->hdr  = (ty_region_hdr_t*)base;

#ifdef MADV_HUGEPAGE
    if (c->hugepage) madvise(base, size, MADV_HUGEPAGE);
#endif

    if (created) {
        ty_region_hdr_t* hdr = reg->hdr;
        memset(hdr, 0, sizeof(*hdr));

        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
        pthread_mutex_init(&hdr->ctl_lock, &attr);
        pthread_mutexattr_destroy(&attr);

        hdr->magic          = TY_MAGIC;
        hdr->abi_version    = TACHYON_ABI_VERSION;
        hdr->version_packed = (TACHYON_VERSION_MAJOR << 16) |
                              (TACHYON_VERSION_MINOR << 8) | TACHYON_VERSION_PATCH;
        hdr->region_size    = size;
        hdr->page_size      = page;
        hdr->lease_ns       = reg->lease_ns;
        hdr->create_time_ns = ty_now_ns();
        atomic_store(&hdr->bump_off, TY_ALIGN64(sizeof(ty_region_hdr_t)));
        atomic_store(&hdr->creator_pid, (uint32_t)getpid());
        atomic_store(&hdr->attached, 0u);
        atomic_store(&hdr->reaper_pid, 0u);
        atomic_store(&hdr->reaper_heartbeat_ns, (uint64_t)0);
        atomic_thread_fence(memory_order_release);
    } else {
        ty_region_hdr_t* hdr = reg->hdr;
        if (hdr->magic != TY_MAGIC) {
            munmap(base, size);
            close(fd);
            free(reg);
            return TY_ERR_STATE;
        }
        if (hdr->abi_version != TACHYON_ABI_VERSION) {
            munmap(base, size);
            close(fd);
            free(reg);
            return TY_ERR_ABI;
        }
        if (hdr->lease_ns) reg->lease_ns = hdr->lease_ns;
    }

    TY_FETCH_ADD(&reg->hdr->attached, 1);
    reg->proc_slot = ty__proc_slot_acquire(reg);
    ty__touch_heartbeat(reg);

    if (reg->spawn_reaper) {
        uint32_t expect = 0;
        if (atomic_compare_exchange_strong_explicit(&reg->hdr->reaper_pid, &expect,
                (uint32_t)getpid(), memory_order_acq_rel, memory_order_relaxed)) {
            atomic_store(&reg->reaper_running, 1u);
            TY_STORE_RELAXED(&reg->hdr->reaper_heartbeat_ns, ty_now_ns());
            if (pthread_create(&reg->reaper_tid, NULL, ty_reaper_thread, reg) != 0) {
                TY_STORE_RELAXED(&reg->hdr->reaper_pid, 0);
                atomic_store(&reg->reaper_running, 0u);
            }
        }
    }

    *out = reg;
    return TY_OK;
}

ty_err_t ty_region_close(ty_region_t* reg) {
    if (!reg || reg->magic != TY_HANDLE_MAGIC) return TY_ERR_INVAL;

    /* Clear before tearing down: a second close, or any other call that gets
     * handed this handle afterwards, now fails cleanly. */
    reg->magic = 0;

    TY_STORE_RELAXED(&reg->closing, 1);
    if (TY_LOAD_RELAXED(&reg->reaper_running)) {
        pthread_join(reg->reaper_tid, NULL);
        TY_STORE_RELAXED(&reg->hdr->reaper_pid, 0);
    }
    ty__proc_slot_release(reg);
    TY_FETCH_ADD(&reg->hdr->attached, (uint32_t)-1);

    munmap(reg->base, reg->size);
    close(reg->fd);
    reg->base = NULL;

    /* Outstanding topic handles still hold a reference and may still call
     * ty_topic_close(), which has to be able to reach this struct. Free only
     * when we were the last holder. */
    if (TY_FETCH_SUB(&reg->handle_refs, 1u) == 1u) free(reg);

    /* Deliberately does NOT unlink.
     *
     * Detaching must not destroy data. A producer that publishes and exits has
     * to leave those messages readable, otherwise the most useful property of a
     * shared region -- surviving its creator -- disappears. Lifetime is
     * explicit: call ty_region_remove() when you actually mean "delete this".
     * That also matches POSIX shm semantics, where removal is always manual.
     */
    return TY_OK;
}

ty_err_t ty_region_remove(const char* name, const char* dir) {
    if (!ty_valid_name(name)) return TY_ERR_INVAL;
    char path[256];
    if (dir && *dir) {
        snprintf(path, sizeof path, "%s/%s", dir, name);
        if (unlink(path) != 0) return (errno == ENOENT) ? TY_ERR_NOENT : TY_ERR_IO;
        return TY_OK;
    }
    snprintf(path, sizeof path, "/tachyon_%s", name);
    if (shm_unlink(path) != 0) return (errno == ENOENT) ? TY_ERR_NOENT : TY_ERR_IO;
    return TY_OK;
}

void* ty_region_base(ty_region_t* reg) {
    return (reg && reg->magic == TY_HANDLE_MAGIC) ? reg->base : NULL;
}
size_t ty_region_size(ty_region_t* reg) {
    return (reg && reg->magic == TY_HANDLE_MAGIC) ? reg->size : 0;
}
const char* ty_region_name(ty_region_t* reg) {
    return (reg && reg->magic == TY_HANDLE_MAGIC) ? reg->name : NULL;
}
