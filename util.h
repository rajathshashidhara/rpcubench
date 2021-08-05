#ifndef RPCUBENCH_UTIL_H_
#define RPCUBENCH_UTIL_H_

#define _GNU_SOURCE
#include <stdlib.h>
#include <time.h>

#define NONCRIT_ERROR(core)    \
  do {  \
    fprintf(stderr, "[%d] %s:%d\n", core, __FILE__, __LINE__);      \
  } while(0)
#define FATAL_ERROR(core)  \
  do {  \
    NONCRIT_ERROR(core);  \
    abort();          \
  } while(0)

#ifdef PRINT_STATS
  #define STATS_ADD(c, f, n) __sync_fetch_and_add(&c->f, n)
  #define STATS_TS(n)        uint64_t n = time_ns()
#else
  #define STATS_ADD(c, f, n) do {} while(0)
  #define STATS_TS(n)        do {} while(0)
#endif

static inline uint64_t read_cnt(uint64_t *p)
{
  uint64_t v = *p;
  __sync_fetch_and_sub(p, v);
  return v;
}

static inline void prefetch0(const volatile void *p)
{
  asm volatile ("prefetcht0 %[p]" : : [p] "m" (*(const volatile char *)p));
}

static inline uint64_t rdtsc(void)
{
  uint32_t eax, edx;

  asm volatile ("rdtsc" : "=a" (eax), "=d" (edx));
  return ((uint64_t) edx << 32) | eax;
}

static inline uint32_t killcycles(uint32_t cyc, uint32_t opaque)
{
  uint64_t start, end;

  start = rdtsc();
  end = start + cyc;

  if (end >= start) {
    while (rdtsc() < end) {
      opaque = opaque * 42 + 37;
      opaque ^= 0x12345678;
      opaque = opaque * 42 + 37;
      opaque ^= 0x87654321;
    }
  }
  else {
    while (rdtsc() >= start || rdtsc() < end) {
      opaque = opaque * 42 + 37;
      opaque ^= 0x12345678;
      opaque = opaque * 42 + 37;
      opaque ^= 0x87654321;
    }
  }

  return opaque;
}

/** Time **/
static inline uint64_t time_ns(void)
{
  struct timespec ts;
  uint64_t ns;

  clock_gettime(CLOCK_MONOTONIC, &ts); // Use CLOCK_MONOTONIC_RAW instead?
  ns = (uint64_t) ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;

  return ns;
}

/** RNG **/
#define RNG_CONST_A       0x5deece66dULL
#define RNG_CONST_C       0xbULL
#define RNG_CONST_M       (1ULL << 48)
typedef uint64_t rng_t;

static inline void rng_init(rng_t *rng, uint64_t seed)
{
  *rng = (seed ^ RNG_CONST_A) % RNG_CONST_M;
}

static inline uint32_t rng_gen32(rng_t *rng)
{
  uint64_t curr, next;
  curr = *rng;

  next = (RNG_CONST_A * curr + RNG_CONST_C) % RNG_CONST_M;
  *rng = next;

  return next >> 16;
}

static inline double rng_gend(rng_t *rng)
{
  uint64_t x =
          (((uint64_t) rng_gen32(rng) >> 6) << 27) +
          (rng_gen32(rng) >> 5);
  return x / ((double) (1ULL << 53));
}

static inline void rng_gen(rng_t *rng, void *buf, size_t size)
{
  uint32_t* u32buf = (uint32_t*) buf;
  for (; size >= 4; size -= 4) {
    *u32buf = rng_gen32(rng);
    u32buf++;
  }

  uint8_t* u8buf = (uint8_t*) u32buf;
  uint32_t x = rng_gen32(rng);
  for (; size > 0; size--) {
    *u8buf = (uint8_t) (x >> 24);
    x <<= 8;
    u8buf++;
  }
}

#endif /* RPCUBENCH_UTIL_H_ */
