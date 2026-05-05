/* server.c — Multi-threaded TCP scheduling server
 *
 * OS Concepts demonstrated:
 *   SOCKET PROGRAMMING     — TCP server accepts clients on PORT 8080.
 *   CONCURRENCY CONTROL    — each accepted client gets its own pthread;
 *                            shared state is mutex-protected in scheduler.c.
 *   ROLE-BASED AUTH        — each session tracks ROLE_GUEST / USER / ADMIN;
 *                            privileged commands are rejected for lower roles.
 *   FILE LOCKING           — all events logged via logger (fcntl lock inside).
 *
 * Commands (client sends plain text lines):
 *   login <user|admin>        — authenticate this session
 *   submit <burst_seconds>    — submit a process (user/admin)
 *   status                    — view the process queue (user/admin)
 *   set algo <RR|FCFS>        — change algorithm (admin only)
 *   kill <pid>                — kill a process (admin only)
 *   quit                      — disconnect
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "common.h"
#include "logger.h"
#include "scheduler.h"

/* ── watch mode: client receives live events until "unwatch" or quit ── */

/* ── Per-client state passed to the handler thread ── */
typedef struct {
    int  sockfd;
    int  role;        /* ROLE_GUEST / ROLE_USER / ROLE_ADMIN */
    char addr[INET_ADDRSTRLEN];
} ClientInfo;


/* ── send_str ─────────────────────────────────────────────────────────────── */
static void send_str(int fd, const char *msg) {
    send(fd, msg, strlen(msg), 0);
}

/* ── send_end ─────────────────────────────────────────────────────────────── */
/* Every server response ends with END_MARKER so the client knows it's done. */
static void send_end(int fd) {
    send_str(fd, END_MARKER);
}

/* ── trim ─────────────────────────────────────────────────────────────────── */
static void trim(char *s) {
    char *p = s + strlen(s) - 1;
    while (p >= s && (*p == '\r' || *p == '\n' || *p == ' ')) *p-- = '\0';
}


/* ═══════════════════════════════════════════════════════════════════════════
 * Command handlers
 * ═══════════════════════════════════════════════════════════════════════════ */

static void cmd_login(ClientInfo *ci, const char *arg) {
    char resp[128];
    if (strcmp(arg, "admin") == 0) {
        ci->role = ROLE_ADMIN;
        snprintf(resp, sizeof(resp), "Logged in as ADMIN.\n");
        printf("\033[1m\033[35m[LOGIN]\033[0m  ADMIN logged in from %s (fd=%d)\n",
               ci->addr, ci->sockfd);
        fflush(stdout);
        log_event("Admin login");
    } else if (strcmp(arg, "user") == 0) {
        ci->role = ROLE_USER;
        snprintf(resp, sizeof(resp), "Logged in as USER.\n");
        printf("\033[1m\033[34m[LOGIN]\033[0m  USER  logged in from %s (fd=%d)\n",
               ci->addr, ci->sockfd);
        fflush(stdout);
        log_event("User login");
    } else {
        snprintf(resp, sizeof(resp),
                 "Unknown role '%s'. Use: login <user|admin>\n", arg);
    }
    send_str(ci->sockfd, resp);
    send_end(ci->sockfd);
}

static void cmd_submit(ClientInfo *ci, const char *arg) {
    if (ci->role < ROLE_USER) {
        send_str(ci->sockfd, "Error: please login first.\n");
        send_end(ci->sockfd);
        return;
    }
    int burst = atoi(arg);
    if (burst <= 0 || burst > 60) {
        send_str(ci->sockfd, "Error: burst must be 1–60 seconds.\n");
        send_end(ci->sockfd);
        return;
    }
    int pid = scheduler_submit(burst, ci->sockfd);
    char resp[128];
    if (pid < 0)
        snprintf(resp, sizeof(resp), "Error: could not submit process.\n");
    else
        snprintf(resp, sizeof(resp),
                 "Process submitted. Assigned PID = %d  (burst = %ds)\n",
                 pid, burst);
    send_str(ci->sockfd, resp);
    send_end(ci->sockfd);
}

static void cmd_status(ClientInfo *ci) {
    if (ci->role < ROLE_USER) {
        send_str(ci->sockfd, "Error: please login first.\n");
        send_end(ci->sockfd);
        return;
    }
    char buf[BUF_SIZE * 4];
    scheduler_status(buf, sizeof(buf));
    send_str(ci->sockfd, buf);
    send_end(ci->sockfd);
}

static void cmd_set_algo(ClientInfo *ci, const char *arg) {
    if (ci->role < ROLE_ADMIN) {
        send_str(ci->sockfd, "Error: admin only.\n");
        send_end(ci->sockfd);
        return;
    }
    char resp[128];
    if (strcmp(arg, "RR") == 0) {
        scheduler_set_algo(RR);
        snprintf(resp, sizeof(resp), "Algorithm set to Round Robin.\n");
    } else if (strcmp(arg, "FCFS") == 0) {
        scheduler_set_algo(FCFS);
        snprintf(resp, sizeof(resp), "Algorithm set to FCFS.\n");
    } else {
        snprintf(resp, sizeof(resp),
                 "Unknown algorithm '%s'. Use: set algo <RR|FCFS>\n", arg);
    }
    send_str(ci->sockfd, resp);
    send_end(ci->sockfd);
}

static void cmd_kill(ClientInfo *ci, const char *arg) {
    if (ci->role < ROLE_ADMIN) {
        send_str(ci->sockfd, "Error: admin only.\n");
        send_end(ci->sockfd);
        return;
    }
    int pid = atoi(arg);
    char resp[128];
    if (pid <= 0) {
        snprintf(resp, sizeof(resp), "Error: invalid PID '%s'.\n", arg);
    } else if (scheduler_kill(pid)) {
        snprintf(resp, sizeof(resp), "PID %d killed.\n", pid);
    } else {
        snprintf(resp, sizeof(resp),
                 "PID %d not found or already finished.\n", pid);
    }
    send_str(ci->sockfd, resp);
    send_end(ci->sockfd);
}

static void cmd_events(ClientInfo *ci) {
    if (ci->role < ROLE_USER) {
        send_str(ci->sockfd, "Error: please login first.\n");
        send_end(ci->sockfd); return;
    }
    char buf[BUF_SIZE * 8];
    scheduler_events(buf, sizeof(buf));
    send_str(ci->sockfd, buf);
    send_end(ci->sockfd);
}

/* watch: registers this client to receive live push events.
   The client stays in a blocking recv loop waiting for "unwatch".
   Every context switch is pushed to them by record_event() in scheduler.c. */
static void cmd_watch(ClientInfo *ci) {
    if (ci->role < ROLE_USER) {
        send_str(ci->sockfd, "Error: please login first.\n");
        send_end(ci->sockfd); return;
    }
    send_str(ci->sockfd,
        "=== LIVE SCHEDULER FEED (type \'unwatch\' to stop) ===\n"
        "  → DISPATCH  = process given CPU\n"
        "  ← PREEMPT   = process taken off CPU (RR slice done)\n"
        "  ✓ FINISHED  = process completed\n"
        "  ✗ KILLED    = process killed by admin\n"
        "  ⊕ SUBMIT    = new process queued\n"
        "=====================================================\n");
    /* no END_MARKER here — we want client to keep listening */

    scheduler_add_watcher(ci->sockfd);

    /* Block until client sends "unwatch" or disconnects */
    char tmp[64];
    while (1) {
        int n = recv(ci->sockfd, tmp, sizeof(tmp)-1, 0);
        if (n <= 0) break;
        tmp[n] = '\0';
        /* strip newline */
        tmp[strcspn(tmp, "\r\n")] = '\0';
        if (strcmp(tmp, "unwatch") == 0) {
            send_str(ci->sockfd, "\n=== Stopped watching ===\n");
            send_end(ci->sockfd);
            break;
        }
    }
    scheduler_remove_watcher(ci->sockfd);
}

static void cmd_help(int fd) {
    send_str(fd,
        "Commands:\n"
        "  login <user|admin>   — authenticate\n"
        "  submit <seconds>     — submit process  (user+)\n"
        "  status               — view queue      (user+)\n"
        "  events               — context-switch log (user+)\n"
        "  watch                — live RR feed    (user+)\n"
        "  unwatch              — stop live feed\n"
        "  set algo <RR|FCFS>   — change algo     (admin)\n"
        "  kill <pid>           — kill process    (admin)\n"
        "  logout               \n"
        "  quit                 — disconnect\n");
    send_end(fd);
}


/* ═══════════════════════════════════════════════════════════════════════════
 * Client handler thread — one per connected client
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *handle_client(void *arg) {
    ClientInfo *ci = (ClientInfo *)arg;
    char buf[BUF_SIZE];

    /* Detach so we don't need to join */
    pthread_detach(pthread_self());

    char welcome[128];
    snprintf(welcome, sizeof(welcome),
             "=== Mini OS Scheduler  |  client from %s ===\n"
             "Type 'help' for commands.\n", ci->addr);
    send_str(ci->sockfd, welcome);
    send_end(ci->sockfd);

    char log_msg[128];
    snprintf(log_msg, sizeof(log_msg), "Client connected: %s", ci->addr);
    log_event(log_msg);

    while (1) {
        int n = recv(ci->sockfd, buf, BUF_SIZE - 1, 0);
        if (n <= 0) break;   /* client disconnected or error */
        buf[n] = '\0';
        trim(buf);
        if (buf[0] == '\0') continue;

        /* ── Route command ── */
        if (strncmp(buf, "login ", 6) == 0) {
            cmd_login(ci, buf + 6);

        } else if (strncmp(buf, "submit ", 7) == 0) {
            cmd_submit(ci, buf + 7);

        } else if (strcmp(buf, "status") == 0) {
            cmd_status(ci);

        } else if (strncmp(buf, "set algo ", 9) == 0) {
            cmd_set_algo(ci, buf + 9);

        } else if (strncmp(buf, "kill ", 5) == 0) {
            cmd_kill(ci, buf + 5);

        } else if (strcmp(buf, "events") == 0) {
            cmd_events(ci);

        } else if (strcmp(buf, "watch") == 0) {
            cmd_watch(ci);

        } else if (strcmp(buf, "help") == 0) {
            cmd_help(ci->sockfd);
        }else if (strcmp(buf, "logout") == 0) {
            ci->role = ROLE_GUEST;

            send_str(ci->sockfd, "Logged out successfully.\n");
            send_end(ci->sockfd);

            printf("[LOGOUT] %s (fd=%d)\n", ci->addr, ci->sockfd);
            fflush(stdout);
            log_event("User logged out");
        } else if (strcmp(buf, "quit") == 0) {
            send_str(ci->sockfd, "Goodbye.\n");
            send_end(ci->sockfd);
            break;

        } else {
            send_str(ci->sockfd, "Unknown command. Type 'help'.\n");
            send_end(ci->sockfd);
        }
    }

    scheduler_remove_watcher(ci->sockfd);   /* safety: remove if was watching */
    printf("\033[1m\033[90m[DISCONNECT]\033[0m  %s (fd=%d)\n", ci->addr, ci->sockfd);
    fflush(stdout);
    snprintf(log_msg, sizeof(log_msg), "Client disconnected: %s", ci->addr);
    log_event(log_msg);

    close(ci->sockfd);
    free(ci);
    return NULL;
}


/* ═══════════════════════════════════════════════════════════════════════════
 * main — bind, listen, accept clients, spawn threads
 * ═══════════════════════════════════════════════════════════════════════════ */
int main(void) {
    /* Initialise subsystems */
    logger_init();
    scheduler_init();

    /* ── Start scheduler thread ── */
    pthread_t sched_tid;
    if (pthread_create(&sched_tid, NULL, scheduler_run, NULL) != 0) {
        perror("pthread_create scheduler");
        exit(EXIT_FAILURE);
    }
    pthread_detach(sched_tid);

    /* ── Create TCP socket ── */
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    /* Allow fast restart (avoid "address already in use") */
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("[server] Listening on port %d  (Ctrl+C to stop)\n", PORT);
    log_event("Server listening");

    /* ── Accept loop ── */
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(server_fd,
                               (struct sockaddr *)&client_addr, &addrlen);
        if (client_fd < 0) {
            if (errno == EINTR) continue;   /* interrupted by signal, retry */
            perror("accept");
            continue;
        }

        /* Allocate client info — freed by handler thread */
        ClientInfo *ci = calloc(1, sizeof(ClientInfo));
        if (!ci) { close(client_fd); continue; }
        ci->sockfd = client_fd;
        ci->role   = ROLE_GUEST;
        inet_ntop(AF_INET, &client_addr.sin_addr, ci->addr, sizeof(ci->addr));

        /* Spawn a dedicated thread for this client */
        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_client, ci) != 0) {
            perror("pthread_create client");
            close(client_fd);
            free(ci);
        }
        /* Thread detaches itself inside handle_client */
    }

    close(server_fd);
    logger_close();
    return 0;
}
