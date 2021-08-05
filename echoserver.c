/*
 * Copyright 2019 University of Washington, Max Planck Institute for
 * Software Systems, and The University of Texas at Austin
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <getopt.h>

#include "util.h"
#include "icache.h"
#include "socketops.h"

/* Configuration */
static uint32_t listen_backlog = 512;
static uint32_t max_events = 128;
static uint32_t max_flows = 4096;
static uint32_t req_bytes = 1024;
static uint32_t resp_bytes = 1024;
static uint32_t op_delay = 0;
static uint32_t working_set = 0;
static uint32_t icache_set = 0;
static uint32_t icache_block = 0;
static uint16_t listen_port = 8080;
static uint32_t num_threads = 1;
static volatile uint32_t num_ready = 0;
static int reuse_port = 1;

/* Connection descriptor */
struct connection {
  struct connection *next;        /*> Next connection in list */
  int fd;                         /*> Socket file descriptor  */

  /* Buffer parameters */
  int len;
  int off;
  int ep_write;                   /*> WRITE Poll status       */
  char buf[];
};

/* Core descriptor */
struct core {
  int id;                         /*> Core ID                 */

  int listen_fd;                  /*> Listen socket fd        */
  int ep_fd;                      /*> epoll fd                */

  rng_t rng;                      /*> Random Number Generator */
  uint32_t opaque;                /*> Cycle spend opaque      */
  uint8_t* workingset;            /*> Working set buffer      */

  /* Connections */
  struct connection *conns;       /*> Connection list head    */
  uint32_t num_conns;             /*> Connections on list     */

  /* Counters */
  uint64_t rx_messages;           /*> RX messages on core     */
  uint64_t tx_messages;           /*> TX messages on core     */

#ifdef PRINT_STATS
  uint64_t rx_calls;
  uint64_t rx_fail;
  uint64_t rx_cycles;
  uint64_t rx_bytes;
  uint64_t tx_calls;
  uint64_t tx_fail;
  uint64_t tx_cycles;
  uint64_t tx_bytes;
  uint64_t ep_calls;
  uint64_t ep_cycles;
  uint64_t *epoll_hist;
#endif
} __attribute__((aligned (64)));

/* Benchmark */
struct benchmark {
  uint64_t t_prev;                  /*> Previous timestamp  */
  long double rx_tp;                /*> RX throughput       */
  long double tx_tp;                /*> TX throughput       */
};

static int open_listening(int coreid)
{
  int fd;
  struct sockaddr_in si;

  if ((fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Set SO_REUSEPORT */
  if (socket_set_reuseport(fd) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Set non-blocking */
  if (socket_set_nonblock(fd) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Bind socket */
  memset(&si, 0, sizeof(si));
  si.sin_family = AF_INET;
  si.sin_port = htons(listen_port + (reuse_port ? 0 : coreid));
  if (bind(fd, (struct sockaddr *) &si, sizeof(si)) < 0) {
    FATAL_ERROR(coreid);
  }

  /* listen on socket */
  if (listen(fd, listen_backlog) < 0) {
    FATAL_ERROR(coreid);
  }

  return fd;
}

static void init_connections(struct core *c)
{
  int i;
  struct connection *conn;
  size_t bufsize;

  bufsize = req_bytes > resp_bytes ? req_bytes : resp_bytes;
  c->conns = NULL;
  for (i = 0; i < max_flows; i++) {
    if ((conn = calloc(1, sizeof(*conn) + bufsize)) == NULL) {
      FATAL_ERROR(c->id);
    }

    conn->next = c->conns;
    c->conns = conn;
    c->num_conns++;
  }
}

static void prepare_core(struct core *c)
{
  int coreid = c->id;
  struct epoll_event ev;

  /* init working set */
  c->workingset = NULL;
  if (working_set > 0) {
    if ((c->workingset = malloc(working_set)) == NULL) {
      FATAL_ERROR(c->id);
    }
  }

  /* open listening socket */
  c->listen_fd = open_listening(coreid);

  /* open and configure epoll */
  if ((c->ep_fd = epoll_create(max_flows + 1)) < 0) {
    FATAL_ERROR(coreid);
  }
  ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
  ev.data.ptr = NULL;
  if (epoll_ctl(c->ep_fd, EPOLL_CTL_ADD, c->listen_fd, &ev) < 0) {
    FATAL_ERROR(coreid);
  }

  init_connections(c);

#ifdef PRINT_STATS
  if ((c->epoll_hist = calloc(max_flows + 2, sizeof(*c->epoll_hist))) == NULL) {
    FATAL_ERROR(coreid);
  }
#endif
}

static inline void accept_connections(struct core *c)
{
  int conn_fd;
  struct connection *conn;
  struct epoll_event ev;

  while (c->conns != NULL) {
    if ((conn_fd = accept(c->listen_fd, NULL, NULL)) < 0) {
      if (errno == EAGAIN) {
          break;
      }
      perror("accept failed");
      FATAL_ERROR(c->id);
    }

    if (socket_set_nonblock(conn_fd) < 0) {
      FATAL_ERROR(c->id);
    }

    if (socket_set_nonagle(conn_fd) < 0) {
      FATAL_ERROR(c->id);
    }

    conn = c->conns;
    c->conns = conn->next;
    c->num_conns--;

    /* add socket to epoll */
    ev.data.ptr = conn;
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    if (epoll_ctl(c->ep_fd, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
      FATAL_ERROR(c->id);
    }

    conn->fd = conn_fd;
    conn->len = 0;
    conn->off = 0;
    conn->ep_write = 0;
  }
}

static inline void conn_epupdate(struct core* c, struct connection* conn, int write)
{
  struct epoll_event ev;

  if (conn->ep_write == write)
    return;

  /* more to send but would block */
  ev.data.ptr = conn;
  ev.events = (write ? EPOLLOUT : EPOLLIN) | EPOLLERR | EPOLLHUP;
  if (epoll_ctl(c->ep_fd, EPOLL_CTL_MOD, conn->fd, &ev) < 0) {
    FATAL_ERROR(c->id);
  }

  conn->ep_write = write;
}

static inline int conn_send(struct core* c, struct connection* conn)
{
  int ret;

  while (conn->off < resp_bytes) {
    STATS_ADD(c, tx_calls, 1);
    STATS_TS(tsc);
    ret = write(conn->fd, conn->buf + conn->off, resp_bytes - conn->off);
    STATS_ADD(c, tx_cycles, time_ns() - tsc);
    if (ret < 0 && errno == EAGAIN) {
      STATS_ADD(c, tx_fail, 1);
      return 1;
    }
    else if (ret < 0) {
      FATAL_ERROR(c->id);
    }
    STATS_ADD(c, tx_bytes, ret);
    conn->off += ret;
  }

  conn->off = 0;
  conn->len = 0;
  return 0;
}

enum {
  RX_SUCCESS = 0,
  RX_FAIL,
  RX_RETRY,
  RX_EOS,
};

static inline int conn_recv(struct core* c, struct connection* conn)
{
  int ret;

  while (conn->len < req_bytes) {
    STATS_ADD(c, rx_calls, 1);
    STATS_TS(tsc);
    ret = read(conn->fd, conn->buf + conn->len, req_bytes - conn->len);
    STATS_ADD(c, rx_cycles, time_ns() - tsc);
    if (ret < 0 && errno == EAGAIN) {
      STATS_ADD(c, rx_fail, 1);
      return RX_RETRY;
    }
    else if (ret < 0) {
      FATAL_ERROR(c->id);
      return RX_FAIL;
    }
    else if (ret == 0) {
      return RX_EOS;
    }
    STATS_ADD(c, rx_bytes, ret);
    conn->len += ret;
  }

  __sync_fetch_and_add(&c->rx_messages, 1);
  conn->off = 0;
  return RX_SUCCESS;
}

static inline void conn_close(struct core* c, struct connection* conn)
{
  close(conn->fd);
  conn->next = c->conns;
  c->conns = conn;
  c->num_conns++;
}

static void process_request(struct core* c, struct connection* conn)
{
  uint32_t off;

  /* Simulate operation delay */
  if (op_delay > 0) {
    c->opaque = killcycles(op_delay, c->opaque);
  }

  /* Access working set */
  if (c->workingset != NULL) {
    off = (rng_gen32(&c->rng) % working_set) & ~63;
    conn->buf[0] = (*((volatile uint32_t*) (c->workingset + off)))++;
  }

  /* ICache footprint */
  if (icache_block != 0) {
    off = rng_gen32(&c->rng) % icache_set;

    switch (icache_block) {
    case 1:
      conn->buf[0] += icache_fill1(off);
      break;

    case 8:
      conn->buf[0] += icache_fill8(off);
      break;

    case 64:
      conn->buf[0] += icache_fill64(off);
      break;

    case 256:
      conn->buf[0] += icache_fill256(off);
      break;

    case 512:
      conn->buf[0] += icache_fill512(off);
      break;

    default:
      FATAL_ERROR(c->id);
      break;
    }
  }
}

static void process_event_connected(struct core* c, struct connection* conn, struct epoll_event* ev)
{
  int ret;

  /* Socket error? */
  if (ev->events & ~(EPOLLIN | EPOLLOUT)) {
    NONCRIT_ERROR(c->id);
    conn_close(c, conn);
    return;
  }

  /* Send out remaining response */
  if (conn->ep_write) {
    assert(conn->len == req_bytes);
    assert(conn->off < resp_bytes);

    ret = conn_send(c, conn);
    if (ret > 0) {
      /* response was not send completely */
      return;
    }
    else if (ret < 0) {
      /* error, close connection */
      NONCRIT_ERROR(c->id);
      conn_close(c, conn);
      return;
    }

    /* response was sent completely */
    __sync_fetch_and_add(&c->tx_messages, 1);
  }

  /* receive request */
  ret = conn_recv(c, conn);
  if (ret == RX_FAIL) {
    NONCRIT_ERROR(c->id);
    conn_close(c, conn);
    return;
  }
  else if (ret == RX_RETRY) {
    /* request was not received completely */
    conn_epupdate(c, conn, 0);
    return;
  }
  else if (ret == RX_EOS) {
    /* connection shut down */
    conn_close(c, conn);
    return;
  }

  process_request(c, conn);

  /* send response */
  assert(conn->len == req_bytes);
  assert(conn->off < resp_bytes);
  ret = conn_send(c, conn);
  if (ret > 0) {
    /* response was not sent completely */
    conn_epupdate(c, conn, 1);
    return;
  }
  else if (ret < 0) {
    /* error, close connection */
    NONCRIT_ERROR(c->id);
    conn_close(c, conn);
    return;
  }

  /* response was sent completely */
  __sync_fetch_and_add(&c->tx_messages, 1);
  conn_epupdate(c, conn, 0);
}

static void *thread_run(void *arg)
{
  struct core* c = arg;
  int n, i, coreid;
  struct epoll_event *evs;
  struct connection *conn;

  coreid = c->id;
  prepare_core(c);

  evs = calloc(max_events, sizeof(*evs));
  if (evs == NULL) {
    FATAL_ERROR(coreid);
  }

  __sync_fetch_and_add(&num_ready, 1);
  while (1) {
    STATS_TS(tsc);
    if ((n = epoll_wait(c->ep_fd, evs, max_events, -1)) < 0) {
      FATAL_ERROR(coreid);
    }
    STATS_ADD(c, ep_cycles, time_ns() - tsc);
    STATS_ADD(c, ep_calls, 1);
    STATS_ADD(c, epoll_hist[n], 1);

    /* Prefetch connection state */
    for (i = 0; i < n; i++) {
      conn = evs[i].data.ptr;
      if (conn != NULL)
        prefetch0(conn);
    }

    for (i = 0; i < n; i++) {
      conn = evs[i].data.ptr;
      if (conn == NULL) {
        /* the listening socket */
        if (evs[i].events != EPOLLIN) {
          FATAL_ERROR(coreid);
        }

        accept_connections(c);
      } else {
        process_event_connected(c, conn, &evs[i]);
      }
    }
  }

  return NULL;
}

static void print_usage(char* progname)
{
  printf("Usage: %s [OPTION]...\n"
      "\n"
      "   -t --threads=       Server threads [default: %d]\n"
      "   -p --base-port=     Server port [default: %d]\n"
      "   -n --max-flows=     Connections/threads [default: %d]\n"
      "   -b --max-bytes=     Request size  (B)  [default: %d]\n"
      "   -R --resp-bytes=    Response size (B)  [default: %d]\n"
      "   -D --op-delay=      Operation delay (cyc.) [default: %d]\n"
      "   -W --working-set=   Working set [default: %d]\n"
      "   -I --icache-set=    Instr. Cache Size [default: %d]\n"
      "   -B --backlog=       Listen backlog [default: %d]\n"
      "   -E --max-epoll-ev=  epoll() size [default: %d]\n"
      "   --reuse-port        Threads reuse the listen port (default)\n"
      "   --no-reuse-port     Threads use (port + tid)\n",
      progname, 1, 8080, 1, 1024, 1024, 0, 0, 0, 512, 128);
}

static void parse_args(int argc, char* argv[])
{
  int c;
  char* end;

  static struct option long_options[] =
  {
    /* These options set a flag */
    {"reuse-port",    no_argument, &reuse_port, 1},
    {"no-reuse-port", no_argument, &reuse_port, 0},

    {"threads",       required_argument, 0, 't'},
    {"base-port",     required_argument, 0, 'p'},
    {"max-flows",     required_argument, 0, 'n'},
    {"max-bytes",     required_argument, 0, 'b'},
    {"resp-bytes",    required_argument, 0, 'R'},
    {"op-delay",      required_argument, 0, 'D'},
    {"working-set",   required_argument, 0, 'W'},
    {"icache-set",    required_argument, 0, 'I'},
    {"backlog",       required_argument, 0, 'B'},
    {"max-epoll-ev",  required_argument, 0, 'E'},
  };

  while (1) {
    c = getopt_long(argc, argv, "t:p:n:b:R:D:W:I:B:E:", long_options, NULL);

    if (c == -1)
      break;

    switch (c) {
    case 0:
      break;

    case 't':
      num_threads = atoi(optarg);
      break;

    case 'p':
      listen_port = atoi(optarg);
      break;

    case 'n':
      max_flows = atoi(optarg);
      break;

    case 'b':
      req_bytes = atoi(optarg);
      break;

    case 'R':
      resp_bytes = atoi(optarg);
      break;

    case 'D':
      op_delay = atoi(optarg);
      break;

    case 'W':
      working_set = atoi(optarg);
      break;

    case 'I':
      if ((end = strchr(optarg, ',')) == NULL) {
        exit(EXIT_FAILURE);
      }
      *end = 0;
      icache_block = atoi(optarg);
      icache_set = atoi(end + 1);
      break;

    case 'B':
      listen_backlog = atoi(optarg);
      break;

    case 'E':
      max_events = atoi(optarg);
      break;

    case '?':
      print_usage(argv[0]);
      exit(EXIT_FAILURE);
      break;

    default:
      exit(EXIT_FAILURE);
    }
  }

  return;
}

static void collect_benchmark(struct core* cs, struct benchmark* b)
{
  uint64_t t_cur;
  long double tp, duration;
  int i;

  t_cur = time_ns();
  b->rx_tp = 0;
  b->tx_tp = 0;

  duration = (long double) (t_cur - b->t_prev) / 1000000000.;

  for (i = 0; i < num_threads; i++) {
    /* RX throughput */
    tp = cs[i].rx_messages / duration;
    cs[i].rx_messages = 0;
    b->rx_tp += tp;

    /* TX throughput */
    tp = cs[i].tx_messages / duration;
    cs[i].tx_messages = 0;
    b->tx_tp += tp;
  }

  b->t_prev = t_cur;

  /* Convert to mbps */
  b->rx_tp = b->rx_tp * req_bytes  * 8 / 1000000.;
  b->tx_tp = b->tx_tp * resp_bytes * 8 / 1000000.;
}

static void print_header()
{
  printf("time_ns,rxtp,txtp\n");
  fflush(stdout);
}

static void print_benchmark(struct benchmark* b)
{
  printf("%lu,%'.2Lf,%'.2Lf\n", b->t_prev, b->rx_tp, b->tx_tp);
  fflush(stdout);
}

int main(int argc, char* argv[])
{
  int i;
  pthread_t *pts;
  struct core* cs;
  char name[256];
  struct benchmark b;

  parse_args(argc, argv);

  signal(SIGPIPE, SIG_IGN);

  /* Setup worker threads */
  pts = calloc(num_threads, sizeof(*pts));
  cs = calloc(num_threads, sizeof(*cs));
  if (pts == NULL || cs == NULL) {
    FATAL_ERROR(num_threads + 1);
  }

  for (i = 0; i < num_threads; i++) {
    cs[i].id = i;

    if (pthread_create(&pts[i], NULL, thread_run, &cs[i]) != 0) {
      FATAL_ERROR(i);
    }

    snprintf(name, 256, "echo-w%u", i);
    pthread_setname_np(pts[i], name);
  }

  /* Wait for threads to launch */
  while (num_ready < num_threads);

  /* benchmark loop */
  print_header();
  b.t_prev = time_ns();
  while (1) {
    sleep(1);

    collect_benchmark(cs, &b);
    print_benchmark(&b);

#ifdef PRINT_STATS
    for (i = 0; i < num_threads; i++) {
        uint64_t rx_calls = read_cnt(&cs[i].rx_calls);
        uint64_t rx_cycles = read_cnt(&cs[i].rx_cycles);
        uint64_t tx_calls = read_cnt(&cs[i].tx_calls);
        uint64_t tx_cycles = read_cnt(&cs[i].tx_cycles);
        uint64_t ep_calls = read_cnt(&cs[i].ep_calls);
        uint64_t ep_cycles = read_cnt(&cs[i].ep_cycles);
        uint64_t rxc = (rx_calls == 0 ? 0 : rx_cycles / rx_calls);
        uint64_t txc = (tx_calls == 0 ? 0 : tx_cycles / tx_calls);
        uint64_t epc = (ep_calls == 0 ? 0 : ep_cycles / ep_calls);
        printf("    core %2d: (rt=%"PRIu64", rf=%"PRIu64 ", rxc=%"PRIu64
                ", rb=%"PRIu64", tt=%"PRIu64", tf=%"PRIu64", txc=%"PRIu64
                ", tb=%"PRIu64", et=%"PRIu64", epc=%"PRIu64",)", i,
            rx_calls, read_cnt(&cs[i].rx_fail), rxc,
            read_cnt(&cs[i].rx_bytes),
            tx_calls, read_cnt(&cs[i].tx_fail), txc,
            read_cnt(&cs[i].tx_bytes),
            ep_calls, epc);
        for (j = 0; j < max_flows + 2; j++) {
          if ((x = read_cnt(&cs[i].epoll_hist[j])) != 0) {
            printf(" epoll[%u]=%"PRIu64, j, x);
          }
        }
        printf("\n");
    }
#endif
  }

  return EXIT_SUCCESS;
}
