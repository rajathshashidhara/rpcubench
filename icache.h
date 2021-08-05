#ifndef RPCUBENCH_ICACHE_H_
#define RPCUBENCH_ICACHE_H_

#include <stdint.h>

uint64_t icache_fill1(uint64_t x);
uint64_t icache_fill8(uint64_t x);
uint64_t icache_fill64(uint64_t x);
uint64_t icache_fill256(uint64_t x);
uint64_t icache_fill512(uint64_t x);

#endif /* RPCUBENCH_ICACHE_H_ */
