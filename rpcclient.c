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
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>
#include <assert.h>
#include <locale.h>
#include <getopt.h>

#include "util.h"
#include "socketops.h"

#define HIST_START_US   0
#define HIST_BUCKET_US  1
#define HIST_BUCKETS    (256 * 1024)

/* Configuration */
static uint32_t max_req_pending = 64;
static uint32_t max_conn_pending = 16;
static uint32_t tx_message_size = 64;
static uint32_t rx_message_size = 64;
static uint32_t num_conns = 8;
static uint32_t num_msgs = 0;
static uint32_t openall_delay = 0;
static uint32_t num_threads = 1;
static struct sockaddr_in *addrs;
static size_t addrs_num;
static volatile int start_running = 0;

/* Connection descriptor */
enum conn_state {
  CONN_CLOSED = 0,
  CONN_CONNECTING = 1,
  CONN_OPEN = 2,
  CONN_CLOSING = 3,
};

struct connection {
  struct connection *next_closed;
  struct sockaddr_in *addr;
  int fd;

  enum conn_state state;
  int ep_write;

  void *rx_buf;
  void *tx_buf;

  uint32_t msg_pending;
  uint32_t tx_remain;
  uint32_t rx_remain;
  uint32_t tx_cnt;

  uint64_t messages;
};

/* Worker core descriptor */
struct core {
  int id;
  int ep_fd;
  pthread_t pthread;

  struct connection* conns;
  struct connection* closed_conns;
  uint32_t conn_pending;
  uint32_t conn_open;

  uint32_t* latency_hist;
  uint64_t messages;

#ifdef PRINT_STATS
  uint64_t rx_calls;
  uint64_t rx_short;
  uint64_t rx_fail;
  uint64_t rx_bytes;
  uint64_t tx_calls;
  uint64_t tx_short;
  uint64_t tx_fail;
  uint64_t tx_bytes;
  uint64_t rx_cycles;
  uint64_t tx_cycles;
  uint64_t ep_calls;
  uint64_t ep_cycles;
#endif
} __attribute__((aligned(64)));

/* Benchmark */
struct benchmark {
  uint64_t t_prev;
  uint64_t msg_total;
  long double tp;
  uint32_t *latency_hist;
  uint32_t open_total;

#ifdef PRINT_PERCORE
  long double* per_core_tp;
#endif

#ifdef PRINT_JFI
  long double jfi_inst;
  long double jfi_ewma;
#endif
};

static inline void record_latency(struct core *c, uint64_t nanos)
{
  size_t bucket = ((nanos / 1000) - HIST_START_US) / HIST_BUCKET_US;
  if (bucket >= HIST_BUCKETS) {
      bucket = HIST_BUCKETS - 1;
  }
  __sync_fetch_and_add(&c->latency_hist[bucket], 1);
}

static void conn_connect(struct core *c, struct connection *conn)
{
  int fd, coreid, ret;
  struct epoll_event ev;

  coreid = c->id;

  if ((fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Set non-blocking */
  if (socket_set_nonblock(fd) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Set non-blocking */
  if (socket_set_nonagle(fd) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Add to epoll */
  ev.data.ptr = conn;
  ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR;
  if (epoll_ctl(c->ep_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
    FATAL_ERROR(coreid);
  }

  /* initiate non-blocking connect */
  ret = connect(fd, (struct sockaddr*) conn->addr, sizeof(*conn->addr));
  if (ret == 0) {
    /* success */
    conn->state = CONN_OPEN;
  }
  else if (ret < 0 && errno == EINPROGRESS) {
    /* ongoing */
    conn->state = CONN_CONNECTING;
  }
  else {
    FATAL_ERROR(coreid);
  }

  conn->fd = fd;
  conn->ep_write = 1;
  conn->msg_pending = 0;
  conn->tx_cnt = 0;
  conn->tx_remain = tx_message_size;
  conn->rx_remain = rx_message_size;
#ifdef PRINT_STATS
  conn->cnt = 0;
#endif
}

static inline void conn_close(struct core *c, struct connection *conn)
{
  if (conn->state == CONN_OPEN || conn->state == CONN_CLOSING)
    c->conn_open--;

  close(conn->fd);
  conn->state = CONN_CLOSED;
  conn->fd = -1;

  conn->next_closed = c->closed_conns;
  c->closed_conns = conn;
}

static int conn_recv(struct core *c, struct connection *conn)
{
  int ret;
  uint64_t *rx_timestamp;

  do {
    STATS_ADD(c, rx_calls, 1);
    STATS_TS(tsc);
    ret = read(conn->fd, conn->rx_buf + rx_message_size - conn->rx_remain, conn->rx_remain);
    STATS_ADD(c, rx_cycles, time_ns() - tsc);

    if (ret > 0) {
      STATS_ADD(c, rx_bytes, ret);

      if (ret < rx_message_size) {
        STATS_ADD(c, rx_short, 1);
      }

      conn->rx_remain -= ret;
      if (conn->rx_remain == 0) {
        /* received whole message */
        __sync_fetch_and_add(&c->messages, 1);
        __sync_fetch_and_add(&conn->messages, 1);

        rx_timestamp = conn->rx_buf;
        record_latency(c, time_ns() - *rx_timestamp);

        conn->rx_remain = rx_message_size;
        conn->msg_pending--;
      }
    }
    else if (ret == 0) {
      /* EOS */
      conn_close(c, conn);
      return -1;
    }
    else if (ret < 0 && errno != EAGAIN) {
      NONCRIT_ERROR(c->id);
      conn_close(c, conn);
      return -1;
    }
    else if (ret < 0 && errno == EAGAIN) {
      /* nothing to receive */
      STATS_ADD(c, rx_fail, 1);
    }
  } while(conn->msg_pending > 0 && ret > 0);

  if (conn->state == CONN_CLOSING && conn->msg_pending == 0) {
    conn_close(c, conn);
    return 1;
  }

  return 0;
}

static int conn_send(struct core *c, struct connection *conn)
{
  int ret;
  int wait_write;
  uint64_t *tx_timestamp;
  struct epoll_event ev;

  ret = 1;
  wait_write = 0;

  while ((conn->msg_pending < max_req_pending || max_conn_pending == 0) &&
      (conn->tx_cnt < num_msgs || num_msgs == 0) && ret > 0)
  {
    /* timestamp if starting a new message */
    if (conn->tx_remain == tx_message_size) {
      tx_timestamp = conn->tx_buf;
      *tx_timestamp = time_ns();
    }

    STATS_ADD(c, tx_calls, 1);
    STATS_TS(tsc);
    ret = write(conn->fd, conn->tx_buf + tx_message_size - conn->tx_remain, conn->tx_remain);
    STATS_ADD(c, tx_cycles, time_ns() - tsc);

    if (ret > 0) {
      STATS_ADD(c, tx_bytes, ret);

      if (ret < tx_message_size) {
        STATS_ADD(c, tx_short, 1);
      }

      conn->tx_remain -= ret;
      if (conn->tx_remain == 0) {
        /* send whole message */
        conn->msg_pending++;
        conn->tx_cnt++;
        conn->tx_remain = tx_message_size;

        if ((conn->msg_pending < max_req_pending || max_req_pending == 0) &&
          (conn->tx_cnt < num_msgs || num_msgs == 0))
        {
          /* send next message when epoll tells us to */
          wait_write = 1;
          break;
        }
      }
    }
    else if (ret < 0 && errno != EAGAIN) {
      NONCRIT_ERROR(c->id);
      conn_close(c, conn);
      return -1;
    }
    else if (ret < 0 && errno == EAGAIN) {
      /* send would block */
      wait_write = 1;
      STATS_ADD(c, tx_fail, 1);
    }
  }

  /* Epoll on write if necessary */
  if (wait_write != conn->ep_write) {
    ev.data.ptr = conn;
    ev.events = EPOLLIN | EPOLLHUP | EPOLLERR | (wait_write ? EPOLLOUT : 0);

    if (epoll_ctl(c->ep_fd, EPOLL_CTL_MOD, conn->fd, &ev) != 0) {
      FATAL_ERROR(c->id);
    }
    conn->ep_write = wait_write;
  }

  /* shutdown if connection tx limit reached */
  if (num_msgs > 0 && conn->tx_cnt >= num_msgs && conn->state == CONN_OPEN) {
    if (shutdown(conn->fd, SHUT_WR) != 0) {
      conn_close(c, conn);
      return -1;
    }
    conn->state = CONN_CLOSING;
  }

  return 0;
}

static void process_event_connecting(struct core *c, struct connection *conn, uint32_t events)
{
  int ret;

  assert(conn->state == CONN_CONNECTING);
  c->conn_pending--;

  if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
    NONCRIT_ERROR(c->id);

    conn_close(c, conn);
    return;
  }

  if ((ret = socket_get_socketerror(conn->fd)) < 0) {
    FATAL_ERROR(c->id);
  }
  if (ret != 0) {
    conn_close(c, conn);
    return;
  }

  conn->state = CONN_OPEN;
  c->conn_open++;
}

static void process_event_connected(struct core *c, struct connection *conn, uint32_t events)
{
  if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
    NONCRIT_ERROR(c->id);

    conn_close(c, conn);
    return;
  }

  if ((events & EPOLLIN) == EPOLLIN && conn_recv(c, conn) != 0)
    return;

  if (conn_send(c, conn) != 0) {
    return;
  }
}

static void conn_events(struct core *c, struct connection *conn, uint32_t events)
{
  if (conn->state == CONN_CONNECTING) {
    process_event_connecting(c, conn, events);
  }

  process_event_connected(c, conn, events);
}

static inline void connect_more(struct core *c)
{
  struct connection *conn;

  while ((conn = c->closed_conns) != NULL &&
      (c->conn_pending < max_conn_pending || max_conn_pending == 0))
  {
    c->closed_conns = conn->next_closed;

    conn_connect(c, conn);
    c->conn_pending++;
  }
}

static void open_all(struct core *c)
{
  int i, ret;
  struct epoll_event evs[max_conn_pending];
  struct epoll_event ev;
  struct connection* conn;

  while (c->conn_open != num_conns) {
    connect_more(c);

    /* wait for events */
    if ((ret = epoll_wait(c->ep_fd, evs, max_conn_pending, -1)) < 0) {
      FATAL_ERROR(c->id);
    }

    for (i = 0; i < ret; i++) {
      conn = evs[i].data.ptr;

      process_event_connecting(c, conn, evs[i].events);

      ev.data.ptr = conn;
      ev.events = EPOLLIN | EPOLLHUP | EPOLLERR;
      if (epoll_ctl(c->ep_fd, EPOLL_CTL_MOD, conn->fd, &ev) != 0) {
        FATAL_ERROR(c->id);
      }
      conn->ep_write = 0;
    }
  }

  for (i = 0; i < num_conns; i++) {
    conn = &c->conns[i];
    ev.data.ptr = conn;
    ev.events = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLOUT;
    if (epoll_ctl(c->ep_fd, EPOLL_CTL_MOD, conn->fd, &ev) != 0) {
      FATAL_ERROR(c->id);
    }
    conn->ep_write = 1;
  }
}

static void init_connections(struct core *c)
{
  int coreid, i;
  size_t next_addr;
  uint8_t* buf;

  coreid = c->id;

  if ((c->conns = calloc(num_conns, sizeof(*c->conns))) == NULL) {
    FATAL_ERROR(coreid);
  }

  c->closed_conns = NULL;
  next_addr = (num_conns * c->id) % addrs_num;
  for (i = 0; i < num_conns; i++) {
    if ((buf = malloc(rx_message_size + tx_message_size)) == NULL) {
      FATAL_ERROR(coreid);
    }

    c->conns[i].rx_buf = buf;
    c->conns[i].tx_buf = buf + rx_message_size;
    c->conns[i].state = CONN_CLOSED;
    c->conns[i].fd = -1;
    c->conns[i].addr = &addrs[next_addr];

    c->conns[i].next_closed = c->closed_conns;
    c->closed_conns = &c->conns[i];

    next_addr = (next_addr + 1) % addrs_num;
  }
}

static void prepare_core(struct core *c)
{
  int coreid = c->id;

  /* Create epoll_fd */
  if ((c->ep_fd = epoll_create(4 * num_conns)) < 0) {
    FATAL_ERROR(coreid);
  }

  /* Allocate histogram */
  if ((c->latency_hist = calloc(HIST_BUCKETS, sizeof(*c->latency_hist))) == NULL) {
    FATAL_ERROR(coreid);
  }

  init_connections(c);
}

static void* thread_run(void *arg)
{
  struct core *c = arg;
  int i, num_evs, ret;
  struct epoll_event *evs;

  prepare_core(c);

  if (openall_delay != 0) {
    open_all(c);
    while (!start_running);
  }

  num_evs = 4 * num_conns;
  if ((evs = calloc(num_evs, sizeof(*evs))) == NULL) {
    FATAL_ERROR(c->id);
  }

  while (1) {
    if (c->closed_conns != NULL)
      connect_more(c);

    STATS_ADD(c, ep_calls, 1);
    STATS_TS(tsc);
    if ((ret = epoll_wait(c->ep_fd, evs, num_evs, -1)) < 0) {
      FATAL_ERROR(c->id);
    }
    STATS_ADD(c, ep_cycles, time_ns() - tsc);

    for (i = 0; i < ret; i++) {
      conn_events(c, evs[i].data.ptr, evs[i].events);
    }
  }
}

static void print_usage(char* progname)
{
  printf("Usage: %s [OPTION] -h <server_ip> -p <port>\n"
      "\n"
      "   -t --threads=           Client threads [default: %d]\n"
      "   -b --request-bytes=     Request size   [default: %d]\n"
      "   -R --response-bytes=    Response size  [default: %d]\n"
      "   -n --max-flows          Connections/thread [default: %d]\n"
      "   -P --pipeline           In-flight msgs/connection [default: %d]\n"
      "   -D --openall-delay      Delay to open all connections on startup [default: %d]\n"
      "   -M --max-msgs-conn      Close connection after M messages [default: %d]\n"
      "   -C --max-pend-conns     Max number of 3-way handshakes in progress per thread [default: %d]\n"
      "   -c --config-file        MTCP configuration file\n",
      progname, 1, 64, 64, 8, 64, 0, 0, 16);
}

static int parse_addrs(const char *ip, const char *ports)
{
  size_t i;
  char *end;

  if (ip == NULL || ports == NULL) {
    NONCRIT_ERROR(0);
    return -1;
  }

  addrs_num = 1;
  for (i = 0; ports[i] != 0; i++)
    if (ports[i] == ',')
      addrs_num++;

  if ((addrs = calloc(addrs_num, sizeof(*addrs))) == NULL) {
    NONCRIT_ERROR(0);
    return -1;
  }

  for (i = 0; i < addrs_num; i++) {
    addrs[i].sin_family = AF_INET;
    addrs[i].sin_addr.s_addr = inet_addr(ip);
    if (addrs[i].sin_addr.s_addr == ((in_addr_t)-1)) {
      NONCRIT_ERROR(0);
      return -1;
    }
    addrs[i].sin_port = htons(strtoul(ports, &end, 10));
    if (addrs[i].sin_port == 0) {
      NONCRIT_ERROR(0);
      return -1;
    }

    if (*end == ',') {
      ports = end + 1;
    } else if (*end == 0) {
      assert(i == addrs_num - 1);
    } else {
      FATAL_ERROR(0);
    }
  }

  return 0;
}

static void parse_args(int argc, char* argv[])
{
  int c;
  const char* ip = NULL;
  const char* ports = NULL;

  static struct option long_options[] = {
      {"host",            required_argument, 0, 'h'},
      {"port",            required_argument, 0, 'p'},
      {"threads",         required_argument, 0, 't'},
      {"request-bytes",   required_argument, 0, 'b'},
      {"response-bytes",  required_argument, 0, 'R'},
      {"max-flows",       required_argument, 0, 'n'},
      {"pipeline",        required_argument, 0, 'P'},
      {"openall-delay",   required_argument, 0, 'D'},
      {"max-msgs-conn",   required_argument, 0, 'M'},
      {"max-pend-conns",  required_argument, 0, 'C'},
      {"config-file",     required_argument, 0, 'c'},
  };

  while (1) {
    c = getopt_long(argc, argv, "h:p:t:b:R:n:P:D:M:C:", long_options, NULL);

    if (c == -1)
      break;

    switch (c) {
    case 0:
      break;

    case 'h':
      ip = optarg;
      break;

    case 'p':
      ports = optarg;
      break;

    case 't':
      num_threads = atoi(optarg);
      break;

    case 'b':
      tx_message_size = atoi(optarg);
      break;

    case 'R':
      rx_message_size = atoi(optarg);
      break;

    case 'n':
      num_conns = atoi(optarg);
      break;

    case 'P':
      max_req_pending = atoi(optarg);
      break;

    case 'D':
      openall_delay = atoi(optarg);
      break;

    case 'M':
      num_msgs = atoi(optarg);
      break;

    case 'C':
      max_conn_pending = atoi(optarg);
      break;

    case '?':
      print_usage(argv[0]);
      exit(EXIT_FAILURE);

    default:
      exit(EXIT_FAILURE);
    }
  }

  if (parse_addrs(ip, ports) != 0) {
    FATAL_ERROR(0);
  }

  return;
}

static void init_benchmark(struct benchmark* b)
{
  b->tp = 0.0;
  b->latency_hist = calloc(HIST_BUCKETS, sizeof(*b->latency_hist));
  if (b->latency_hist == NULL) {
    FATAL_ERROR(0);
  }

#ifdef PRINT_PERCORE
  b->per_core_tp = calloc(num_threads, sizeof(*b->per_core_tp));
  if (b->per_core_tp == NULL) {
    FATAL_ERROR(0);
  }
#endif

#ifdef PRINT_JFI
  b->jfi = 0.0;
  b->jfi_ewma = 0.0;
#endif
}

static void print_header()
{
  printf("time_ns,flows,tp,lat50p,lat90p,lat95p,lat99p,lat99.9p,lat99.99p");
#ifdef PRINT_JFI
  printf(",jfi,jfi-ewma");
#endif
#ifdef PRINT_PERCORE
  int i;
  for (i = 0; i < num_threads; i++)
    printf(",core[%d]", i);
#endif
  printf("\n");
  fflush(stdout);
}

static inline void hist_fract_buckets(uint32_t *hist, uint64_t total,
        double *fracs, size_t *idxs, size_t num)
{
  size_t i, j;
  uint64_t sum = 0, goals[num];
  for (j = 0; j < num; j++) {
    goals[j] = total * fracs[j];
  }
  for (i = 0, j = 0; i < HIST_BUCKETS && j < num; i++) {
    sum += hist[i];
    for (; j < num && sum >= goals[j]; j++) {
      idxs[j] = i;
    }
  }
}

static inline int hist_value(size_t i)
{
  if (i == HIST_BUCKETS - 1) {
    return -1;
  }

  return i * HIST_BUCKET_US + HIST_START_US;
}

static void collect_benchmark(struct core *cs, struct benchmark *b)
{
  int i, j;
  uint64_t t_cur;
  long double tp, duration;
  uint32_t hx;

  b->tp = 0.0;
  b->open_total = 0;
  memset(b->latency_hist, 0, sizeof(*b->latency_hist) * HIST_BUCKETS);

  t_cur = time_ns();
  duration = (double) (t_cur - b->t_prev) / 1000000000.;

  b->msg_total = 0;
  for (i = 0; i < num_threads; i++) {
    tp = (long double) cs[i].messages / duration;
    cs[i].messages = 0;

    b->open_total += cs[i].conn_open;
    b->tp += tp;
#ifdef PRINT_PERCORE
    b->per_core_tp[i] = tp;
#endif

    for (j = 0; j < HIST_BUCKETS; j++) {
      hx = cs[i].latency_hist[j];
      b->msg_total += hx;
      b->latency_hist[j] += hx;
    }
  }

#ifdef PRINT_JFI
  long double jfi, jfi_sosq;

  jfi = jfi_sosq = 0;
  for (i = 0; i < num_threads; i++) {
    for (j = 0; j < num_conns; j++) {
      hx = cs[i].conns[j].messages;
      cs[i].conns[j].messages = 0;

      jfi += m;
      jfi_sosq += (m * m);
    }
  }
  if (jfi != 0 && b->open_total != 0) {
    jfi = (jfi * jfi) / (b->open_total * jfi_sosq);
  }
  b->jfi = jfi;
  b->jfi_ewma = (7.0 * b->jfi_ewma + jfi) / 8.0;
#endif

  b->t_prev = t_cur;
  /* Convert to mbps */
  b->tp = b->tp * tx_message_size * 8 / 1000000.;
#ifdef PRINT_PERCORE
  for (i = 0; i < num_threads; i++)
    b->per_core_tp[i] = b->per_core_tp[i] * tx_message_size * 8 / 1000000.;
#endif
}

static void print_benchmark(struct benchmark *b)
{
  double fracs[6] = { 0.5, 0.9, 0.95, 0.99, 0.999, 0.9999 };
  size_t fracs_pos[sizeof(fracs) / sizeof(fracs[0])];

  hist_fract_buckets(b->latency_hist, b->msg_total,
    fracs, fracs_pos, sizeof(fracs) / sizeof(fracs[0]));

  printf("%lu,%u,%'.2Lf,%d,%d,%d,%d,%d,%d",
        b->t_prev, b->open_total, b->tp,
        hist_value(fracs_pos[0]), hist_value(fracs_pos[1]), hist_value(fracs_pos[2]),
        hist_value(fracs_pos[3]), hist_value(fracs_pos[4]), hist_value(fracs_pos[5]));
#ifdef PRINT_JFI
  printf(",%'.2Lf,%'.2Lf", b->jfi, b->jfi_ewma);
#endif
#ifdef PRINT_PERCORE
  int i;
  for (i = 0; i < num_threads; i++)
    printf(",%'.2Lf", b->per_core_ttp[i]);
#endif
  printf("\n");
  fflush(stdout);
}

int main(int argc, char* argv[])
{
  int i;
  struct core *cs;
  struct benchmark b;

  setlocale(LC_NUMERIC, "");

  parse_args(argc, argv);

  init_benchmark(&b);

  /* Init worker threads */
  cs = calloc(num_threads, sizeof(*cs));
  if (cs == NULL) {
    FATAL_ERROR(0);
  }

  for (i = 0; i < num_threads; i++) {
    cs[i].id = i;
    if (pthread_create(&cs[i].pthread, NULL, thread_run, cs + i)) {
      FATAL_ERROR(0);
    }
  }

  if (openall_delay != 0) {
    sleep(openall_delay);
    start_running = 1;
  }

  print_header();

  b.t_prev = time_ns();
  while (1) {
    sleep(1);

    collect_benchmark(cs, &b);
    print_benchmark(&b);

#ifdef PRINT_STATS
    printf("stats:\n");
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

      printf("    core %2d: (rt=%"PRIu64", rs=%"PRIu64", rf=%"PRIu64
          ", rxc=%"PRIu64", rb=%"PRIu64", tt=%"PRIu64", ts=%"PRIu64", tf=%"PRIu64
          ", txc=%"PRIu64", tb=%"PRIu64", et=%"PRIu64", epc=%"PRIu64")\n", i,
          rx_calls, read_cnt(&cs[i].rx_short), read_cnt(&cs[i].rx_fail),
          rxc, read_cnt(&cs[i].rx_bytes),
          tx_calls, read_cnt(&cs[i].tx_short), read_cnt(&cs[i].tx_fail),
          txc, read_cnt(&cs[i].tx_bytes), ep_calls, epc);
    }
    for (i = 0; i < num_threads; i++) {
      for (j = 0; j < num_conns; j++) {
        printf("      t[%d].conns[%d]:  pend=%u  rx_r=%u  tx_r=%u  cnt=%"
                PRIu64" fd=%d\n",
                i, j, cs[i].conns[j].pending, cs[i].conns[j].rx_remain,
                cs[i].conns[j].tx_remain, cs[i].conns[j].cnt,
                cs[i].conns[j].fd);
      }
    }
    fflush(stdout);
#endif
  }

  return EXIT_SUCCESS;
}
