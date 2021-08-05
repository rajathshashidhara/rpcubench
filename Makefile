
CFLAGS := -std=gnu99 -g -O3 -Wall -march=native -MD -MP
LDFLAGS := -m64 -g -O3 -march=native -no-pie -lrt -pthread

ICACHE_OBJS := 	icache_fill1.o 		\
				icache_fill8.o 		\
				icache_fill64.o 	\
				icache_fill256.o 	\
				icache_fill512.o

all: echoserver rpcclient unidir

echoserver: echoserver.o $(ICACHE_OBJS)

rpcclient: rpcclient.o

unidir: unidir.o

clean:
	rm -f echoserver echoserver.o echoserver.d \
		rpcclient rpcclient.o rpcclient.d \
		unidir unidir.o unidir.d \
		$(ICACHE_OBJS) $(ICACHE_OBJS:.o=.S)

-include Makefile.icache
-include echoserver.d
-include rpcclient.d
-include unidir.d

.PHONY: all clean
