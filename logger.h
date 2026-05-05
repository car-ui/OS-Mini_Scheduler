/* logger.h — File-based event logger with fcntl locking */
#ifndef LOGGER_H
#define LOGGER_H

/* Open / create the log file. Call once at startup. */
void logger_init(void);

/* Write a timestamped line to the log file.
   Uses fcntl write-lock so concurrent threads/processes are safe. */
void log_event(const char *event);

/* Close the log file. */
void logger_close(void);

#endif /* LOGGER_H */
