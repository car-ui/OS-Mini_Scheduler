/* scheduler.c — Process scheduling engine
 *
 * OS Concepts demonstrated:
 *   CONCURRENCY CONTROL  — pthread_mutex protects the process queue.
 *   DATA CONSISTENCY     — mutex prevents race conditions / lost updates.
 *   IPC (fork + pipe)    — child worker forked per slice; parent blocks on
 *                          read(); child writes 'D' when done.
 *   SIGNALS              — scheduler_kill() sends SIGKILL to worker child.
 *
 * Context-switch visibility (new):
 *   • Every DISPATCH / PREEMPT / FINISHED / KILLED event is stored in a
 *     ring buffer and printed to the server console in colour.
 *   • "watch" clients receive live push messages for every context switch.
 *   • "events" command dumps the ring buffer on demand.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <time.h>
#include <pthread.h>
#include "scheduler.h"
#include "logger.h"
#include "common.h"

/* ── constants ───────────────────────────────────────────────────────────── */
#define MAX_EVENTS   30
#define MAX_WATCHERS 10

/* ANSI colours */
#define C0  "\033[0m"
#define CG  "\033[32m"
#define CY  "\033[33m"
#define CR  "\033[31m"
#define CC  "\033[36m"
#define CB  "\033[1m"

/* ═══════════════════════════════════════════════════════════════════════════
 * Shared state — ALL access under queue_mutex
 * ═══════════════════════════════════════════════════════════════════════════ */
static Process  *q_head     = NULL;
static Process  *q_tail     = NULL;
static Algorithm g_algo     = RR;
static int       g_next_pid = 1;
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ── event ring buffer ───────────────────────────────────────────────────── */
static char ev_buf[MAX_EVENTS][256];
static int  ev_head  = 0;
static int  ev_count = 0;
static pthread_mutex_t ev_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ── watcher list ────────────────────────────────────────────────────────── */
static int watcher_fds[MAX_WATCHERS];
static int watcher_count = 0;
static pthread_mutex_t watch_mutex = PTHREAD_MUTEX_INITIALIZER;

void scheduler_add_watcher(int fd) {  
    pthread_mutex_lock(&watch_mutex);
    if (watcher_count < MAX_WATCHERS) watcher_fds[watcher_count++] = fd;
    pthread_mutex_unlock(&watch_mutex);
}
void scheduler_remove_watcher(int fd) {
    pthread_mutex_lock(&watch_mutex);
    for (int i = 0; i < watcher_count; i++)
        if (watcher_fds[i] == fd) { watcher_fds[i] = watcher_fds[--watcher_count]; break; }
    pthread_mutex_unlock(&watch_mutex);
}

/* ── timestamp ───────────────────────────────────────────────────────────── */
static void ts(char *out, int sz) {
    time_t t = time(NULL);
    strftime(out, sz, "%H:%M:%S", localtime(&t));
}

/* ── count_ready  (call with queue_mutex held) ───────────────────────────── */
static int count_ready(void) {
    int n = 0;
    for (Process *p = q_head; p; p = p->next) if (p->state == READY) n++;
    return n;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * record_event — ring buffer + server console + push to watchers
 *   plain   : no ANSI codes (for storage / log file)
 *   coloured: with ANSI (for console and watching clients)
 * ═══════════════════════════════════════════════════════════════════════════ */
static void record_event(const char *plain, const char *coloured) {
    char t[16]; ts(t, sizeof(t));

    /* ring buffer */
    char full[256];
    snprintf(full, sizeof(full), "[%s] %s", t, plain);
    pthread_mutex_lock(&ev_mutex);
    snprintf(ev_buf[ev_head], sizeof(ev_buf[0]), "%s", full);
    ev_head = (ev_head + 1) % MAX_EVENTS;
    if (ev_count < MAX_EVENTS) ev_count++;
    pthread_mutex_unlock(&ev_mutex);

    /* log file */
    log_event(plain);

    /* server console */
    printf(CB "[%s]" C0 " %s\n", t, coloured);
    fflush(stdout);

    /* push to watching clients */
    char push[300];
    snprintf(push, sizeof(push), "[LIVE][%s] %s\n", t, plain);
    pthread_mutex_lock(&watch_mutex);
    for (int i = 0; i < watcher_count; ) {
        if (send(watcher_fds[i], push, strlen(push), MSG_NOSIGNAL) < 0)
            watcher_fds[i] = watcher_fds[--watcher_count];
        else i++;
    }
    pthread_mutex_unlock(&watch_mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Queue helpers
 * ═══════════════════════════════════════════════════════════════════════════ */
static void enqueue(Process *p) {
    pthread_mutex_lock(&queue_mutex);
    p->next = NULL;
    if (q_tail) q_tail->next = p; else q_head = p;
    q_tail = p;
    pthread_mutex_unlock(&queue_mutex);
}

static void move_to_tail(Process *p) {
    if (!q_head || !q_tail || p == q_tail) return;

    /* if p is head */
    if (p == q_head) {
        q_head = p->next;
    } else {
        Process *prev = q_head;
        while (prev && prev->next != p) prev = prev->next;
        if (prev) prev->next = p->next;
    }

    /* move p to tail */
    q_tail->next = p;
    p->next = NULL;
    q_tail = p;
}

static const char *state_str(ProcState s) {
    switch (s) {
        case READY:    return "READY   ";
        case RUNNING:  return "RUNNING ";
        case FINISHED: return "FINISHED";
        default:       return "UNKNOWN ";
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * run_slice — fork worker, use pipe for IPC, block until done or killed
 * ═══════════════════════════════════════════════════════════════════════════ */
static void run_slice(Process *p, int secs) {
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return; }

    pid_t child = fork();
    if (child < 0) {
        perror("fork"); close(pipefd[0]); close(pipefd[1]); return;
    }

    if (child == 0) {
        /* Child — simulate CPU execution */
        close(pipefd[0]);
        sleep(secs);              /* simulate burst */
        write(pipefd[1], "D", 1); /* IPC: notify parent */
        close(pipefd[1]);
        exit(0);
    }

    /* Parent — scheduler thread */
    close(pipefd[1]);

    pthread_mutex_lock(&queue_mutex);
    p->real_pid = (int)child;
    pthread_mutex_unlock(&queue_mutex);

    char result = 0;
    read(pipefd[0], &result, 1); /* blocks until child writes or pipe closes */
    close(pipefd[0]);
    waitpid(child, NULL, 0);     /* reap — no zombies */

    pthread_mutex_lock(&queue_mutex);
    p->real_pid = 0;
    pthread_mutex_unlock(&queue_mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Public API
 * ═══════════════════════════════════════════════════════════════════════════ */
void scheduler_init(void) {}

void scheduler_set_algo(Algorithm algo) {
    pthread_mutex_lock(&queue_mutex);
    g_algo = algo;
    pthread_mutex_unlock(&queue_mutex);
    char pl[64], cl[128];
    snprintf(pl, sizeof(pl), "Algorithm changed to %s", algo == RR ? "Round Robin" : "FCFS");
    snprintf(cl, sizeof(cl), CC "⚙  %s" C0, pl);
    record_event(pl, cl);
}

Algorithm scheduler_get_algo(void) {
    pthread_mutex_lock(&queue_mutex);
    Algorithm a = g_algo;
    pthread_mutex_unlock(&queue_mutex);
    return a;
}

int scheduler_submit(int burst, int owner_fd) {
    if (burst <= 0 || burst > 60) return -1;
    Process *p = calloc(1, sizeof(Process));
    if (!p) return -1;

    pthread_mutex_lock(&queue_mutex);
    p->pid = g_next_pid++; p->burst = burst; p->remaining = burst;
    p->state = READY; p->owner = owner_fd;
    pthread_mutex_unlock(&queue_mutex);

    enqueue(p);

    char pl[128], cl[200];
    snprintf(pl, sizeof(pl), "SUBMIT   PID=%-3d  burst=%ds", p->pid, burst);
    snprintf(cl, sizeof(cl), CG "⊕  SUBMIT  " C0 "PID=" CB "%-3d" C0 "  burst=%ds", p->pid, burst);
    record_event(pl, cl);
    return p->pid;
}

int scheduler_kill(int pid) {
    pthread_mutex_lock(&queue_mutex);
    for (Process *p = q_head; p; p = p->next) {
        if (p->pid == pid) {
            if (p->state == FINISHED) { pthread_mutex_unlock(&queue_mutex); return 0; }
            if (p->state == RUNNING && p->real_pid > 0)
                kill((pid_t)p->real_pid, SIGKILL); /* signal */
            p->state = FINISHED; p->remaining = 0;
            pthread_mutex_unlock(&queue_mutex);

            char pl[64], cl[128];
            snprintf(pl, sizeof(pl), "KILLED   PID=%-3d  (by admin)", pid);
            snprintf(cl, sizeof(cl), CR "✗  KILLED  " C0 "PID=" CB "%-3d" C0 "  (by admin)", pid);
            record_event(pl, cl);
            return 1;
        }
    }
    pthread_mutex_unlock(&queue_mutex);
    return 0;
}

void scheduler_status(char *buf, int maxlen) {
    pthread_mutex_lock(&queue_mutex);
    int w = snprintf(buf, maxlen,
        "Algo: %-14s  Quantum: %ds\n"
        "%-5s %-7s %-9s %-10s %-8s\n"
        "%-5s %-7s %-9s %-10s %-8s\n",
        g_algo == RR ? "Round Robin" : "FCFS", TIME_QUANTUM,
        "PID","BURST","REMAINING","STATE","OWNER",
        "---","-----","---------","--------","-----");
    for (Process *p = q_head; p && w < maxlen - 80; p = p->next)
        w += snprintf(buf + w, maxlen - w,
            "%-5d %-7d %-9d %-10s fd=%-4d\n",
            p->pid, p->burst, p->remaining, state_str(p->state), p->owner);
    if (!q_head) snprintf(buf + w, maxlen - w, "(no processes)\n");
    pthread_mutex_unlock(&queue_mutex);
}

void scheduler_events(char *buf, int maxlen) {
    pthread_mutex_lock(&ev_mutex);
    int w = snprintf(buf, maxlen, "=== Context Switch Log (last %d events) ===\n", ev_count);
    int start = (ev_count < MAX_EVENTS) ? 0 : ev_head;
    for (int i = 0; i < ev_count && w < maxlen - 260; i++) {
        int idx = (start + i) % MAX_EVENTS;
        w += snprintf(buf + w, maxlen - w, "  %s\n", ev_buf[idx]);
    }
    if (ev_count == 0) snprintf(buf + w, maxlen - w, "  (no events yet)\n");
    pthread_mutex_unlock(&ev_mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Scheduler thread
 * ═══════════════════════════════════════════════════════════════════════════ */
void *scheduler_run(void *arg) {
    (void)arg;
    record_event("Scheduler started", CC "⚙  Scheduler started" C0);

    while (1) {
       pthread_mutex_lock(&queue_mutex);

      Process *p = q_head;
        Process *start = q_head;
        int found = 0;

        if (p) {
            do {
                if (p->state == READY) {
                    found = 1;
                    break;
                }
                p = p->next ? p->next : q_head;
            } while (p != start);
        }

        if (!found) {
            pthread_mutex_unlock(&queue_mutex);
            usleep(200000);
            continue;
        }

        int slice = (g_algo == RR)
            ? (p->remaining < TIME_QUANTUM ? p->remaining : TIME_QUANTUM)
            : p->remaining;

        int rdy = count_ready();
        Algorithm snap = g_algo;
        p->state = RUNNING;
        pthread_mutex_unlock(&queue_mutex);

        /* ── DISPATCH event ── */
        char pl[220], cl[320];
        snprintf(pl, sizeof(pl),
            "DISPATCH  PID=%-3d  remaining=%-3ds  slice=%-2ds  "
            "ready=%d  algo=%s",
            p->pid, p->remaining, slice, rdy, snap == RR ? "RR" : "FCFS");
        snprintf(cl, sizeof(cl),
            CG "→ DISPATCH" C0 "  PID=" CB "%-3d" C0
            "  remaining=" CY "%-3ds" C0 "  slice=%-2ds  ready=%d  [%s]",
            p->pid, p->remaining, slice, rdy, snap == RR ? "RR" : "FCFS");
        record_event(pl, cl);

        /* ── Run slice (fork+pipe IPC) ── */
        run_slice(p, slice);

        /* ── Post-slice update ── */
        pthread_mutex_lock(&queue_mutex);

        if (p->state == FINISHED) {
            /* killed by admin mid-run */
            pthread_mutex_unlock(&queue_mutex);
            continue;
        }

        p->remaining -= slice;

        if (p->remaining <= 0) {
            p->remaining = 0; p->state = FINISHED;
            int burst_snap = p->burst;
            pthread_mutex_unlock(&queue_mutex);

            snprintf(pl, sizeof(pl),
                "FINISHED  PID=%-3d  burst=%ds  all slices complete", p->pid, burst_snap);
            snprintf(cl, sizeof(cl),
                CG "✓ FINISHED" C0 "  PID=" CB "%-3d" C0
                "  burst=%ds  " CG "all slices complete" C0, p->pid, burst_snap);
            record_event(pl, cl);

        } else {
            int rem = p->remaining;
            p->state = READY;           /* goes to back of queue for next round */
            move_to_tail(p);
            pthread_mutex_unlock(&queue_mutex);

            snprintf(pl, sizeof(pl),
                "PREEMPT   PID=%-3d  used=%-2ds  remaining=%-3ds  → back to queue",
                p->pid, slice, rem);
            snprintf(cl, sizeof(cl),
                CY "← PREEMPT " C0 "  PID=" CB "%-3d" C0
                "  used=%-2ds  remaining=" CY "%-3ds" C0 "  → back to queue",
                p->pid, slice, rem);
            record_event(pl, cl);
        }
    }
    return NULL;
}
