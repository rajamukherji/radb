#ifndef LINEAR_INDEX_H
#define LINEAR_INDEX_H

#include "config.h"
#include "common.h"

#include <stddef.h>
#include <stdint.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct linear_index_t linear_index_t;
typedef int (*linear_compare_t)(void *Keys, void *Full, uint32_t Index);
typedef size_t (*linear_insert_t)(void *Keys, void *Full);

linear_index_t *linear_index_open(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS);
void linear_index_close(linear_index_t *Store);

typedef struct {
	linear_index_t *Index;
	radb_error_t Error;
} linear_index_open_t;

typedef uint8_t linear_key_t[16];

linear_index_t *linear_index_create(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS);
linear_index_open_t linear_index_open_v2(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS);

size_t linear_index_insert(linear_index_t *Store, uint32_t Hash, linear_key_t Key, void *Full);
size_t linear_index_search(linear_index_t *Store, uint32_t Hash, linear_key_t Key, void *Full);

typedef struct {
	size_t Index;
	int Created;
} linear_index_result_t;

linear_index_result_t linear_index_insert2(linear_index_t *Store, uint32_t Hash, linear_key_t Key, void *Full);

#endif
