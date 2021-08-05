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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <getopt.h>

#include "util.h"
#include "socketops.h"

/* Configuration */
#define BATCH_SIZE  32
#define MAX_PENDING 32

static uint32_t num_conns = 1;
static uint32_t msg_bytes = 1;
static uint32_t num_threads = 1;
static uint32_t ip = 0;
static uint32_t port = 1234;
static uint32_t tx = 1;
static uint32_t op_delay = 0;
static uint32_t tx_delay = 0;
static int start_tx = 0;

/* Connection descriptor */
struct connection {
  int fd;
  size_t allocated;

  struct connection *next;
  struct connection *next_tx;

  short open;
  short id;
};

/* Core descriptor */
struct core {
  int id;
  int epfd;
  int listenfd;

  struct connection *conns_list;
  struct connection *conns;
  struct connection *conns_unused;

  uint32_t conns_open;
  uint32_t conns_closed;

  uint64_t bytes;
  uint64_t sum;
  uint64_t *conn_cnts;
  volatile uint32_t kop;

#ifdef PRINT_STATS
  uint64_t batches;
  uint64_t n_ops;
  uint64_t n_ops_short;
#endif
};

/* Benchmark */
struct benchmark {
  uint64_t t_prev;

  uint64_t bytes;
  long double tp;

  uint32_t conns_open;
  uint32_t conns_closed;
  uint64_t conn_bytes_min;
  uint64_t conn_bytes_max;
  uint64_t *conn_cnts;

  long double jfi;
};

static inline uint64_t touch(const void *buf, size_t len)
{
  uint64_t x = 0;
  size_t off = 0, avail;

  while (off < len) {
    avail = len - off;
    if (avail >= 32) {
      x += *(const uint64_t *) ((const uint8_t *) buf + off);
      x += *(const uint64_t *) ((const uint8_t *) buf + off + 8);
      x += *(const uint64_t *) ((const uint8_t *) buf + off + 16);
      x += *(const uint64_t *) ((const uint8_t *) buf + off + 24);
      off += 32;
    } else if (avail >= 16) {
      x += *(const uint64_t *) ((const uint8_t *) buf + off);
      x += *(const uint64_t *) ((const uint8_t *) buf + off + 8);
      off += 16;
    } else if (avail >= 8) {
      x += *(const uint64_t *) ((const uint8_t *) buf + off);
      off += 8;
    } else if (avail >= 4) {
      x += *(const uint32_t *) ((const uint8_t *) buf + off);
      off += 4;
    } else if (avail >= 2) {
      x += *(const uint16_t *) ((const uint8_t *) buf + off);
      off += 2;
    } else {
      x += *(const uint8_t *) buf + off;
      off++;
    }
  }
  return x;
}

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
  si.sin_addr.s_addr = ip;
  si.sin_port = htons(port);
  if (bind(fd, (struct sockaddr *) &si, sizeof(si)) < 0) {
    FATAL_ERROR(coreid);
  }

  /* listen on socket */
  if (listen(fd, num_conns) < 0) {
    FATAL_ERROR(coreid);
  }

  return fd;
}

static void init_connections(struct core *c)
{
  int i;
  struct connection *conn;

  /* allocate connection structs */
  c->conns_list = (struct connection*) calloc(num_conns, sizeof(struct connection));
  if (c->conns_list == NULL) {
    FATAL_ERROR(c->id);
  }

  for (i = 0; i < num_conns; i++) {
    conn = &c->conns_list[i];
    conn->open = 0;
    conn->id = i;
    conn->next = c->conns_unused;
    c->conns_unused = conn;
  }
}

static void prepare_core(struct core *c)
{
  struct epoll_event ev;

  c->conns_closed = num_conns;
  init_connections(c);

  if ((c->conn_cnts = calloc(num_conns, sizeof(*c->conn_cnts))) == NULL) {
    FATAL_ERROR(c->id);
  }

  if ((c->epfd = epoll_create(num_conns + 1)) < 0) {
    FATAL_ERROR(c->id);
  }

  if (ip == 0) {
    if ((c->listenfd = open_listening(c->id)) < 0) {
      FATAL_ERROR(c->id);
    }

    /* open and configure epoll */
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    ev.data.ptr = NULL;
    if (epoll_ctl(c->epfd, EPOLL_CTL_ADD, c->listenfd, &ev) < 0) {
      FATAL_ERROR(c->id);
    }
  }
}

static inline void conn_connected(struct core *c, struct connection *conn)
{
  struct epoll_event ev;

  conn->next = c->conns;
  c->conns = conn;
  c->conns_open++;
  conn->open = 1;

  if (!tx && ip != 0) {
    ev.data.ptr = conn;
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    if (epoll_ctl(c->epfd, EPOLL_CTL_MOD, conn->fd, &ev) < 0) {
      FATAL_ERROR(c->id);
    }
  }
}

static void conn_connect(struct core *c)
{
  int fd, ret;
  struct epoll_event ev;
  struct connection *conn;
  struct sockaddr_in addr;

  conn = c->conns_unused;
  c->conns_unused = conn->next;
  c->conns_closed--;

  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = ip;
  addr.sin_port = htons(port);

  if ((fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    FATAL_ERROR(c->id);
  }

  /* Set non-blocking */
  if (socket_set_nonblock(fd) < 0) {
    FATAL_ERROR(c->id);
  }

  /* Set non-blocking */
  if (socket_set_nonagle(fd) < 0) {
    FATAL_ERROR(c->id);
  }

  /* Add to epoll */
  ev.data.ptr = conn;
  ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR;
  if (epoll_ctl(c->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
    FATAL_ERROR(c->id);
  }

  /* initiate non-blocking connect */
  ret = connect(fd, (struct sockaddr*) &addr, sizeof(addr));
  if (ret == 0) {
    conn_connected(c, conn);
  }
  else if (ret < 0 && errno == EINPROGRESS) {
    /* still going on */
  }
  else {
    /* opening connection failed */
    FATAL_ERROR(c->id);
  }

  conn->fd = fd;
}

static inline void accept_connections(struct core *c)
{
  int fd;
  struct connection *conn;
  struct epoll_event ev;

  while (c->conns_unused != NULL) {
    if ((fd = accept(c->listenfd, NULL, NULL)) < 0) {
      if (errno == EAGAIN) {
          break;
      }
      FATAL_ERROR(c->id);
    }

    if (socket_set_nonblock(fd) < 0) {
      FATAL_ERROR(c->id);
    }

    if (socket_set_nonagle(fd) < 0) {
      FATAL_ERROR(c->id);
    }

    conn = c->conns_unused;
    c->conns_unused = conn->next;
    c->conns_closed--;

    ev.data.ptr = conn;
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | (tx ? EPOLLOUT : 0);

    if (epoll_ctl(c->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
      FATAL_ERROR(c->id);
    }

    conn->fd = fd;
    conn_connected(c, conn);
  }

  /* remove listenfd from epoll if we can't accept more connections */
  if (c->conns_unused == NULL) {
    if (epoll_ctl(c->epfd, EPOLL_CTL_DEL, c->listenfd, &ev) < 0) {
      FATAL_ERROR(c->id);
    }
  }
}

static void listener_event(struct core *c, uint32_t event)
{
  if ((event & (EPOLLERR | EPOLLHUP)) != 0) {
    FATAL_ERROR(c->id);
  }

  accept_connections(c);
}

static void* loop_receive(void *data)
{
  int i, ret;
  struct core *c = data;
  struct connection *conn;
  struct epoll_event evs[BATCH_SIZE];
  struct epoll_event *ev;
  ssize_t bytes;
  uint64_t bytes_rx_bump;
#ifdef PRINT_STATS
  uint32_t n_ops, n_short;
#endif

  void* buf;

  if ((buf = malloc(msg_bytes)) == NULL) {
    FATAL_ERROR(c->id);
  }

  prepare_core(c);

  while (1) {
    /* open connections if necessary:
     *  - not in listening mode
     *  - haven't reached the connection limit
     *  - maximum pending connect limit not reached
     */
    while (ip != 0 && c->conns_open < num_conns && c->conns_closed > 0 &&
        (num_conns - c->conns_open - c->conns_closed) < MAX_PENDING)
    {
      conn_connect(c);
    }

    if ((ret = epoll_wait(c->epfd, evs, BATCH_SIZE, -1)) < 0) {
      FATAL_ERROR(c->id);
    }

    bytes_rx_bump = 0;
#ifdef PRINT_STATS
    n_ops = n_short = 0;
#endif

    for (i = 0; i < ret; i++) {
      ev = &evs[i];
      conn = ev->data.ptr;

      /* catch events on listening socket */
      if (conn == NULL) {
        listener_event(c, ev->events);
        continue;
      }

      if ((ev->events & (EPOLLERR | EPOLLHUP)) != 0) {
        FATAL_ERROR(c->id);
      }

      if ((ev->events & EPOLLOUT) != 0) {
        if (socket_get_socketerror(conn->fd) != 0) {
          FATAL_ERROR(c->id);
        }

        conn_connected(c, conn);
      }

      if ((ev->events & EPOLLIN) != 0) {
        bytes = read(conn->fd, buf, msg_bytes);

        if (bytes > 0) {
          c->conn_cnts[conn->id] += bytes;
          c->sum += touch(buf, bytes);
          bytes_rx_bump += bytes;

#ifdef PRINT_STATS
          n_ops++;
          if (bytes < msg_bytes) {
            n_short++;
          }
#endif
        }
        else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          /* do nothing */
        }
        else {
          FATAL_ERROR(c->id);
        }
      }
    }

    if (op_delay > 0) {
      c->kop = killcycles(op_delay * bytes_rx_bump / msg_bytes, c->kop);
    }

    __sync_fetch_and_add(&c->bytes, bytes_rx_bump);
#ifdef PRINT_STATS
    __sync_fetch_and_add(&c->batches, 1);
    __sync_fetch_and_add(&c->n_ops, n_ops);
    __sync_fetch_and_add(&c->n_ops_short, n_short);
#endif
  }

  return NULL;
}

static void *loop_send(void *data)
{
  int i, ret;
  struct core *c = data;
  struct connection *conn;
  struct epoll_event evs[BATCH_SIZE];
  struct epoll_event *ev;

  ssize_t bytes;
  void* buf;
  uint64_t bytes_rx_bump;
  uint64_t bytes_tx_bump;
#ifdef PRINT_STATS
  uint32_t n_ops, n_short;
#endif

  if ((buf = malloc(msg_bytes)) == NULL) {
    FATAL_ERROR(c->id);
  }

  prepare_core(c);

  while (1) {
    /* open connections if necessary:
     *  - not in listening mode
     *  - haven't reached the connection limit
     *  - maximum pending connect limit not reached
     */
    while (ip != 0 && c->conns_open < num_conns && c->conns_closed > 0 &&
        (num_conns - c->conns_open - c->conns_closed) < MAX_PENDING)
    {
      conn_connect(c);
    }

    if ((ret = epoll_wait(c->epfd, evs, BATCH_SIZE, -1)) < 0) {
      FATAL_ERROR(c->id);
    }

    bytes_rx_bump = 0;
    bytes_tx_bump = 0;
#ifdef PRINT_STATS
    n_ops = n_short = 0;
#endif

    for (i = 0; i < ret; i++) {
      ev = &evs[i];
      conn = ev->data.ptr;

      /* catch events on listening socket */
      if (conn == NULL) {
        listener_event(c, ev->events);
        continue;
      }

      if ((ev->events & (EPOLLERR | EPOLLHUP)) != 0) {
        FATAL_ERROR(c->id);
      }

      if ((ev->events & EPOLLOUT) != 0) {
        if (conn->open == 0) {
          if (socket_get_socketerror(conn->fd) != 0) {
            FATAL_ERROR(c->id);
          }

          conn_connected(c, conn);
        }

        if (start_tx) {
          memset(buf, 1, msg_bytes);
          bytes = write(conn->fd, buf, msg_bytes);
          if (bytes > 0) {
            c->conn_cnts[conn->id] += bytes;
            bytes_tx_bump += bytes;
#ifdef PRINT_STATS
            n_ops++;
            if (bytes < msg_bytes)
              n_short++;
#endif
          }
          else if (bytes < 0  && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            /* do nothing */
          }
          else {
            FATAL_ERROR(c->id);
          }
        }
      }

      if ((ev->events & EPOLLIN) != 0) {
        bytes = read(conn->fd, buf, msg_bytes);

        if (bytes > 0) {
          c->sum += touch(buf, bytes);
          bytes_rx_bump += bytes;
        }
        else if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          /* do nothing */
        }
        else {
          FATAL_ERROR(c->id);
        }
      }
    }

    if (op_delay > 0) {
      c->kop = killcycles(bytes_tx_bump * op_delay / msg_bytes, c->kop);
    }

    __sync_fetch_and_add(&c->bytes, bytes_tx_bump);
#ifdef PRINT_STATS
    __sync_fetch_and_add(&c->batches, 1);
    __sync_fetch_and_add(&c->n_ops, n_ops);
    __sync_fetch_and_add(&c->n_ops_short, n_short);
#endif
  }

  return NULL;
}

static void print_usage(char* progname)
{
  fprintf(stderr,
      "Usage: %s OPTIONS\n"
      "Options:\n"
      "  -i IP       IP address for client mode       [default server mode]\n"
      "  -p PORT     Port to listen on/connect to     [default 1234]\n"
      "  -t THREADS  Number of threads to use         [default 1]\n"
      "  -c CONNS    Number of connections per thread [default 1]\n"
      "  -b BYTES    Message size in bytes            [default 1]\n"
      "  -r          Receive mode                     [default transmit]\n"
      "  -d DELAY    Seconds before sending starts    [default 0]\n"
      "  -o DELAY    Cycles to artificially delay/op  [default 0]\n",
      progname);
}

static void parse_args(int argc, char* argv[])
{
  int c, done;

  static struct option long_options[] =
  {
    /* These options set a flag */
    {"threads",       required_argument, 0, 't'},
    {"ip",            required_argument, 0, 'i'},
    {"port",          required_argument, 0, 'p'},
    {"conns",         required_argument, 0, 'c'},
    {"msg-bytes",     required_argument, 0, 'b'},
    {"op-delay",      required_argument, 0, 'o'},
    {"tx-delay",      required_argument, 0, 'd'},

    /* These options set a flag */
    {"receive",       no_argument, 0, 'r'},
  };

  done = 0;
  while (!done) {
    c = getopt_long(argc, argv, "t:i:p:c:b:o:d:r", long_options, NULL);
    switch (c) {
    case 'i':
      if ((ip = inet_addr(optarg)) == INADDR_NONE) {
        FATAL_ERROR(0);
      }
      break;

    case 'p':
      port = atoi(optarg);
      break;

    case 't':
      num_threads = atoi(optarg);
      break;

    case 'c':
      num_conns = atoi(optarg);
      break;

    case 'b':
      msg_bytes = atoi(optarg);
      break;

    case 'd':
      tx_delay = atoi(optarg);
      break;

    case 'o':
      op_delay = atoi(optarg);
      break;

    case 'r':
      tx = 0;
      break;

    case -1:
      done = 1;
      break;

    case '?':
      print_usage(argv[0]);
      FATAL_ERROR(0);
      break;

    default:
      FATAL_ERROR(0);
      break;
    }
  }
}

static inline void print_header()
{
  printf("bytes,tp,conns_open,conns_closed,conn_bytes_min,conn_bytes_max,jfi\n");
  fflush(stdout);
}

static void collect_benchmark(struct core *cs, struct benchmark *b)
{
  int i, j;
  long double jfi_sosq;
  struct core* c;
  uint64_t conn_diff, t_cur;

  b->bytes = 0;
  b->tp = 0.0;
  b->conns_open = b->conns_closed = 0;
  b->conn_bytes_min = UINT64_MAX;
  b->conn_bytes_max = 0;
  b->jfi = 0.0;

  jfi_sosq = 0.0;

  for (i = 0; i < num_threads; i++) {
    c = &cs[i];
    b->bytes += __sync_fetch_and_add(&c->bytes, 0);
    b->conns_open += c->conns_open;
    b->conns_closed += c->conns_closed;

    for (j = 0; j < num_conns; j++) {
      conn_diff = c->conn_cnts[j] - b->conn_cnts[num_conns * i + j];
      b->conn_cnts[num_conns * i + j] = c->conn_cnts[j];

      if (conn_diff < b->conn_bytes_min)
        b->conn_bytes_min = conn_diff;

      if (conn_diff > b->conn_bytes_max) {
        b->conn_bytes_max = conn_diff;
      }
      b->jfi += conn_diff;
      jfi_sosq += (double) conn_diff * conn_diff;
    }
  }

  if (b->jfi != 0) {
    b->jfi = (b->jfi * b->jfi) / (num_conns * num_threads * jfi_sosq);
  }

  t_cur = time_ns();
  b->tp = ((b->bytes * 8) / 1000000.) / ((double) (t_cur - b->t_prev) / 1000000000.);
  b->t_prev = t_cur;
}

static void print_benchmark(struct benchmark *b)
{
  printf("%lu,%'.2Lf,%u,%u,%lu,%lu,%Lf\n",
      b->bytes, b->tp, b->conns_open, b->conns_closed,
      b->conn_bytes_min, b->conn_bytes_max, b->jfi);
  fflush(stdout);
}

int main(int argc, char* argv[])
{
  struct core *cs;
  pthread_t *pts;
  struct benchmark b;
  int i;

#ifdef PRINT_STATS
  uint64_t stat_batches, stat_ops, stat_short;
#endif

  parse_args(argc, argv);

  if ((cs = calloc(num_threads, sizeof(*cs))) == NULL) {
    FATAL_ERROR(0);
  }

  if ((pts = calloc(num_threads, sizeof(*pts))) == NULL) {
    FATAL_ERROR(0);
  }

  for (i = 0; i < num_threads; i++) {
    cs[i].id = i;
    prepare_core(&cs[i]);

    if (tx) {
      if (pthread_create(&pts[i], NULL, loop_send, &cs[i]) != 0) {
        FATAL_ERROR(0);
      }
    }
    else {
      if (pthread_create(&pts[i], NULL, loop_receive, &cs[i]) != 0) {
        FATAL_ERROR(0);
      }
    }
  }

  if ((b.conn_cnts = calloc(num_conns * num_threads, sizeof(*b.conn_cnts))) == NULL) {
    FATAL_ERROR(0);
  }

  if (tx_delay > 0 && tx) {
    sleep(tx_delay);
  }
  start_tx = 1;
  b.t_prev = time_ns();

  print_header();

  while (1) {
    sleep(1);

    collect_benchmark(cs, &b);
    print_benchmark(&b);

#ifdef PRINT_STATS
    stat_batches = stat_ops = stat_short = 0;
    for (i = 0; i < num_threads; i++) {
      stat_batches += __sync_fetch_and_and(&cs[i].batches, 0);
      stat_ops += __sync_fetch_and_and(&cs[i].n_ops, 0);
      stat_short += __sync_fetch_and_and(&cs[i].n_ops_short, 0);
    }
    printf("  stats: batches=%"PRIu64" ops=%"PRIu64" short=%"PRIu64"\n",
        stat_batches, stat_ops, stat_short);
    fflush(stdout);
#endif
  }

  return EXIT_SUCCESS;
}
