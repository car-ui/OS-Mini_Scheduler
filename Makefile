# Makefile — Mini OS Scheduler
# Usage:
#   make          — build both server and client
#   make clean    — remove binaries and log

CC      = gcc
CFLAGS  = -Wall -Wextra -g -pthread
LDFLAGS = -pthread

SERVER_SRCS = server.c scheduler.c logger.c
CLIENT_SRCS = client.c
HEADERS     = common.h scheduler.h logger.h

.PHONY: all clean

all: server client

server: $(SERVER_SRCS) $(HEADERS)
	$(CC) $(CFLAGS) -o server $(SERVER_SRCS) $(LDFLAGS)
	@echo "Built: server"

client: $(CLIENT_SRCS) $(HEADERS)
	$(CC) $(CFLAGS) -o client $(CLIENT_SRCS) $(LDFLAGS)
	@echo "Built: client"

clean:
	rm -f server client scheduler.log
	@echo "Cleaned."
