/* logger.c — Timestamped logger with fcntl advisory file locking
 *
 * OS Concept demonstrated: FILE LOCKING
 *   Multiple client threads call log_event() concurrently.
 *   fcntl(F_SETLKW) acquires an exclusive write lock before each write,
 *   ensuring no two threads interleave their log lines.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "logger.h"
#include "common.h"

static int log_fd = -1;  /* file descriptor kept open for lifetime of server */

/* ── logger_init ──────────────────────────────────────────────────────────── */
void logger_init(void) {
    log_fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0) {
        perror("[logger] Failed to open log file");
        exit(EXIT_FAILURE);
    }
    log_event("=== Scheduler server started ===");
}

/* ── logger_close ─────────────────────────────────────────────────────────── */
void logger_close(void) {
    if (log_fd >= 0) {
        log_event("=== Scheduler server stopped ===");
        close(log_fd);
        log_fd = -1;
    }
}

/* ── log_event ────────────────────────────────────────────────────────────── */
void log_event(const char *event) {
    if (log_fd < 0) return;

    /* Build timestamped line */
    time_t now = time(NULL);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&now));

    char line[BUF_SIZE + 64];
    int  len = snprintf(line, sizeof(line), "[%s] %s\n", ts, event);

    /* ── fcntl write lock (blocks until acquired) ── */
    struct flock fl;
    fl.l_type   = F_WRLCK;   /* exclusive write lock  */
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 0;          /* 0 = lock entire file  */

    if (fcntl(log_fd, F_SETLKW, &fl) == -1) {
        perror("[logger] fcntl lock");
        return;
    }

    write(log_fd, line, len);

    /* ── release lock ── */
    fl.l_type = F_UNLCK;
    fcntl(log_fd, F_SETLK, &fl);
}
