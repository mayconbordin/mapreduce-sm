CC=gcc
CFLAGS=-std=c99 -D_BSD_SOURCE -D_GNU_SOURCE -D_XOPEN_SOURCE=600 -DWALL_TIME
DEBUG_CFLAGS=-g -Wall -DPRINT_INFO
LDFLAGS=-I../../include -pthread `pkg-config --cflags --libs glib-2.0` -lgthread-2.0
SOURCES=../../src/queue.c ../../src/mapreduce.c wordcount.c
EXECUTABLE=wordcount

# glib
# `pkg-config --cflags --libs glib-2.0`


# Objects and libs to link
OBJS = 

all:	wordcount

wordcount:	$(OBJS) $(SOURCES)
	$(CC) $(CFLAGS) $^ -o $(EXECUTABLE) $(LDFLAGS)
	
debug:	$(OBJS) $(SOURCES)
	$(CC) $(CFLAGS) $(DEBUG_CFLAGS) $^ -o $(EXECUTABLE) $(LDFLAGS)
	
optimize: $(OBJS) $(SOURCES)
	$(CC) $(CFLAGS) $^ -o $(EXECUTABLE)_opt $(LDFLAGS) -O3

clean:
	rm -rf *.o $(EXECUTABLE) *.out
