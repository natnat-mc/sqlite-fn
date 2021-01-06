.PHONY: all

CC = gcc
CFLAGS = -fPIC -shared
# CFLAGS := $(CFLAGS) -DDEBUG=1 -g

all: fn.so libfn.so fn
clean:
	rm -f fn.so libfn.so fn

fn.so: fn.c
	$(CC) $(CFLAGS) $^ -o $@

libfn.so:
	ln -s fn.so libfn.so

fn:
	ln -s fn.so fn
