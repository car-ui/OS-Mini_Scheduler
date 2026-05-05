/* common.h — Shared definitions for scheduler project */
#ifndef COMMON_H
#define COMMON_H

#define PORT          8080
#define MAX_CLIENTS   10
#define BUF_SIZE      1024
#define LOG_FILE      "scheduler.log"
#define TIME_QUANTUM  2    /* Round Robin slice in seconds (keep small for demo) */
#define END_MARKER    "---END---\n"

/* ── Client roles ─────────────────────────────────────────────────────────── */
#define ROLE_GUEST  0   /* not yet logged in */
#define ROLE_USER   1   /* can submit / view status */
#define ROLE_ADMIN  2   /* can also kill / change algorithm */

/* ── Process states ───────────────────────────────────────────────────────── */
typedef enum { READY, RUNNING, FINISHED } ProcState;

/* ── Scheduling algorithm ─────────────────────────────────────────────────── */
typedef enum { RR, FCFS } Algorithm;

/* ── Process Control Block (singly-linked list node) ─────────────────────── */
typedef struct Process {
    int        pid;       /* scheduler-assigned PID                  */
    int        real_pid;  /* OS pid of forked worker child           */
    int        burst;     /* total CPU burst requested (seconds)     */
    int        remaining; /* time still needed                       */
    ProcState  state;
    int        owner;     /* client socket fd of submitting client   */
    struct Process *next;
} Process;

#endif /* COMMON_H */
