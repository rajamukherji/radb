#ifndef LINEAR_INDEX_H
#define LINEAR_INDEX_H

#include "config.h"
#include "common.h"

#define INVALID_INDEX 0xFFFFFFFF

typedef struct linear_index_t linear_index_t;
typedef int (*linear_compare_t)(void *Keys, const void *Full, uint32_t Index);
typedef size_t (*linear_insert_t)(void *Keys, const void *Full);

linear_index_t *linear_index_open(const char *Prefix, void *Keys RADB_MEM_PARAMS);
linear_index_t *linear_index_create(const char *Prefix, void *Keys RADB_MEM_PARAMS);
void linear_index_set_compare(linear_index_t *Store, linear_compare_t Compare);
void linear_index_set_insert(linear_index_t *Store, linear_insert_t Insert);
void *linear_index_keys(linear_index_t *Store);
size_t linear_index_count(linear_index_t *Store);
void linear_index_close(linear_index_t *Store);


void linear_index_set_extra(linear_index_t *Store, uint32_t Value);
uint32_t linear_index_get_extra(linear_index_t *Store);

typedef struct {
	linear_index_t *Index;
	radb_error_t Error;
} linear_index_open_t;

#define LINEAR_KEY_SIZE 16
typedef uint8_t linear_key_t[LINEAR_KEY_SIZE];

linear_index_open_t linear_index_open2(const char *Prefix, void *Keys RADB_MEM_PARAMS);

size_t linear_index_search(linear_index_t *Store, uint32_t Hash, const linear_key_t Key, const void *Full);
size_t linear_index_insert(linear_index_t *Store, uint32_t Hash, const linear_key_t Key, const void *Full);
size_t linear_index_delete(linear_index_t *Store, uint32_t Hash, const linear_key_t Key, const void *Full);

index_result_t linear_index_insert2(linear_index_t *Store, uint32_t Hash, const linear_key_t Key, const void *Full);
index_result_t linear_index_delete2(linear_index_t *Store, uint32_t Hash, const linear_key_t Key, const void *Full);

#endif
