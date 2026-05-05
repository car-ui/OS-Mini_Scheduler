#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "common.h"

#define DEFAULT_IP "127.0.0.1"

static int  g_sockfd    = -1;
static int  g_watching  = 0;   /* 1 while in watch mode */


/* ── recv_until_end ──────────────────────────────────────────────────────────
 * Accumulate socket data until END_MARKER appears, then print and return.
 * ─────────────────────────────────────────────────────────────────────────── */
static void recv_until_end(int sockfd) {
    char buf[BUF_SIZE * 8];
    int  total = 0;
    char tmp[BUF_SIZE];

    buf[0] = '\0';
    while (1) {
        int n = recv(sockfd, tmp, sizeof(tmp) - 1, 0);
        if (n <= 0) { printf("[server disconnected]\n"); exit(0); }
        tmp[n] = '\0';
        if (total + n < (int)sizeof(buf) - 1) {
            memcpy(buf + total, tmp, n);
            total += n;
            buf[total] = '\0';
        }
        if (strstr(buf, END_MARKER)) {
            char *mark = strstr(buf, END_MARKER);
            *mark = '\0';
            printf("%s", buf);
            fflush(stdout);
            return;
        }
    }
}


/* ── watch_reader_thread ─────────────────────────────────────────────────────
 * While g_watching==1, continuously read lines from the socket and print them.
 * The server pushes one line per context-switch event.
 * Exits when the socket closes or g_watching is cleared.
 * ─────────────────────────────────────────────────────────────────────────── */
static void *watch_reader(void *arg) {
    (void)arg;
    char line[512];
    int  pos = 0;
    char c;

    while (g_watching) {
        int n = recv(g_sockfd, &c, 1, 0);
        if (n <= 0) { g_watching = 0; break; }

        if (c == '\n') {
            line[pos] = '\0';
            pos = 0;

            /* Check for end-of-watch marker */
            if (strstr(line, END_MARKER) || strstr(line, "---END---")) {
                g_watching = 0;
                printf("\n");
                fflush(stdout);
                break;
            }

            /* Colour the event type for the client terminal */
            if      (strstr(line, "DISPATCH"))  printf("\033[32m%s\033[0m\n", line);
            else if (strstr(line, "PREEMPT"))   printf("\033[33m%s\033[0m\n", line);
            else if (strstr(line, "FINISHED"))  printf("\033[32;1m%s\033[0m\n", line);
            else if (strstr(line, "KILLED"))    printf("\033[31m%s\033[0m\n", line);
            else if (strstr(line, "SUBMIT"))    printf("\033[36m%s\033[0m\n", line);
            else if (strstr(line, "Algorithm")) printf("\033[35m%s\033[0m\n", line);
            else                                printf("%s\n", line);
            fflush(stdout);
        } else {
            if (pos < (int)sizeof(line) - 2) line[pos++] = c;
        }
    }
    return NULL;
}


/* ── enter_watch_mode ─────────────────────────────────────────────────────── */
static void enter_watch_mode(void) {
    g_watching = 1;

    /* Print the header sent by the server (no END_MARKER — server keeps pushing) */
    char tmp[BUF_SIZE * 2];
    int total = 0;
    char c;
    /* Read until the header separator "====...====\n" */
    while (total < (int)sizeof(tmp) - 2) {
        int n = recv(g_sockfd, &c, 1, 0);
        if (n <= 0) { g_watching = 0; return; }
        tmp[total++] = c;
        tmp[total]   = '\0';
        /* The header ends with "===...===\n" — detect by counting '=' runs */
        if (c == '\n' && total > 10) {
            char *last_nl = tmp + total - 1;
            /* Walk back to previous newline */
            char *prev = last_nl - 1;
            while (prev > tmp && *prev != '\n') prev--;
            if (prev != tmp) prev++;
            if (strncmp(prev, "====", 4) == 0) break;
        }
    }
    printf("%s", tmp);
    fflush(stdout);

    /* Spawn background reader thread */
    pthread_t tid;
    pthread_create(&tid, NULL, watch_reader, NULL);
    pthread_detach(tid);

    printf("\033[33m[watch mode]\033[0m Type 'unwatch' to stop.\n\n");
    fflush(stdout);

    /* Read "unwatch" from stdin — anything else is ignored in watch mode */
    char input[64];
    while (g_watching) {
        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\n")] = '\0';
        if (strcmp(input, "unwatch") == 0) {
            send(g_sockfd, "unwatch", 7, 0);
            /* wait for background thread to see END_MARKER and clear g_watching */
            int tries = 20;
            while (g_watching && tries-- > 0) usleep(100000);
            g_watching = 0;
            break;
        }
        printf("\033[33m[watch mode]\033[0m Type 'unwatch' to stop.\n");
        fflush(stdout);
    }
}


/* ═══════════════════════════════════════════════════════════════════════════
 * main
 * ═══════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[]) {
    const char *server_ip = (argc > 1) ? argv[1] : DEFAULT_IP;

    g_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_sockfd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    struct sockaddr_in srv = {0};
    srv.sin_family = AF_INET;
    srv.sin_port   = htons(PORT);
    if (inet_pton(AF_INET, server_ip, &srv.sin_addr) <= 0) {
        fprintf(stderr, "Invalid address: %s\n", server_ip); exit(EXIT_FAILURE);
    }
    if (connect(g_sockfd, (struct sockaddr *)&srv, sizeof(srv)) < 0) {
        perror("connect");
        fprintf(stderr, "Is the server running on %s:%d?\n", server_ip, PORT);
        exit(EXIT_FAILURE);
    }
    printf("Connected to scheduler server at %s:%d\n\n", server_ip, PORT);

    /* Welcome banner */
    recv_until_end(g_sockfd);

    /* ── Command loop ── */
    char input[BUF_SIZE];
    while (1) {
        printf("> ");
        fflush(stdout);

        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\n")] = '\0';
        if (input[0] == '\0') continue;

        /* Send command */
        if (send(g_sockfd, input, strlen(input), 0) < 0) { perror("send"); break; }

        if (strcmp(input, "watch") == 0) {
            enter_watch_mode();           /* special streaming mode */
        } else {
            recv_until_end(g_sockfd);     /* normal request/response */
            if (strcmp(input, "quit") == 0) break;
        }
    }

    close(g_sockfd);
    printf("Disconnected.\n");
    return 0;
}
