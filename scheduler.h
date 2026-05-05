/* scheduler.h — Process queue and scheduling engine interface */
#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "common.h"

/* ── Lifecycle ────────────────────────────────────────────────────────────── */
void  scheduler_init(void);
void *scheduler_run(void *arg);   /* pthread entry point */

/* ── Algorithm control (admin only) ──────────────────────────────────────── */
void      scheduler_set_algo(Algorithm algo);
Algorithm scheduler_get_algo(void);

/* ── Client operations ───────────────────────────────────────────────────── */
/* Submit a new process; returns the assigned PID, or -1 on error. */
int  scheduler_submit(int burst, int owner_fd);

/* Kill a process by scheduler PID; returns 1 if found, 0 if not. */
int  scheduler_kill(int pid);

/* Fill buf with a human-readable status table (mutex-safe). */
void scheduler_status(char *buf, int maxlen);

/* Fill buf with the last N scheduler events (context switches, preemptions). */
void scheduler_events(char *buf, int maxlen);

/* Register / unregister a client fd to receive live push events (watch mode). */
void scheduler_add_watcher(int fd);
void scheduler_remove_watcher(int fd);

#endif /* SCHEDULER_H */
