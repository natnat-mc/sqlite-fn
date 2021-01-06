.PHONY: all clean re

CC = gcc
CFLAGS = -fPIC -shared --std=c99 -Wall -Wextra -pedantic -Werror
# CFLAGS := $(CFLAGS) -DDEBUG=1 -g

all: fn.so libfn.so fn
clean:
	rm -f fn.so libfn.so fn
re: clean all

fn.so: fn.c
	$(CC) $(CFLAGS) $^ -o $@

libfn.so:
	ln -s fn.so libfn.so

fn:
	ln -s fn.so fn
