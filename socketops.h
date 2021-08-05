#ifndef RPCUBENCH_SOCKETOPS_H_
#define RPCUBENCH_SOCKETOPS_H_

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>

static inline int socket_set_nonblock(int fd)
{
  int flag;

  if ((flag = fcntl(fd, F_GETFL, 0)) == -1) {
      return -1;
  }
  flag |= O_NONBLOCK;
  return fcntl(fd, F_SETFL, flag);
}

static inline int socket_set_reuseport(int fd)
{
  int flag = 1;
  return setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flag, sizeof(flag));
}

static inline int socket_set_nonagle(int fd)
{
  int flag = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
}

static inline int socket_get_socketerror(int fd)
{
  int status;
  socklen_t slen;

  slen = sizeof(status);
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &status, &slen) < 0) {
    return -1;
  }

  return status;
}

#endif /* RPCUBENCH_SOCKETOPS_H_ */
