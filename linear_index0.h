#ifndef LINEAR_INDEX0_H
#define LINEAR_INDEX0_H

#include "config.h"
#include "common.h"

#define INVALID_INDEX 0xFFFFFFFF

typedef struct linear_index0_t linear_index0_t;
typedef int (*linear_compare_t)(void *Keys, const void *Full, uint32_t Index);
typedef size_t (*linear_insert_t)(void *Keys, const void *Full);

linear_index0_t *linear_index0_open(const char *Prefix, void *Keys RADB_MEM_PARAMS);
linear_index0_t *linear_index0_create(const char *Prefix, void *Keys RADB_MEM_PARAMS);
void linear_index0_set_compare(linear_index0_t *Store, linear_compare_t Compare);
void linear_index0_set_insert(linear_index0_t *Store, linear_insert_t Insert);
void *linear_index0_keys(linear_index0_t *Store);
size_t linear_index0_count(linear_index0_t *Store);
void linear_index0_close(linear_index0_t *Store);


void linear_index0_set_extra(linear_index0_t *Store, uint32_t Value);
uint32_t linear_index0_get_extra(linear_index0_t *Store);

typedef struct {
	linear_index0_t *Index;
	radb_error_t Error;
} linear_index0_open_t;

linear_index0_open_t linear_index0_open2(const char *Prefix, void *Keys RADB_MEM_PARAMS);

size_t linear_index0_search(linear_index0_t *Store, uint32_t Hash, const void *Full);
size_t linear_index0_insert(linear_index0_t *Store, uint32_t Hash, const void *Full);
size_t linear_index0_delete(linear_index0_t *Store, uint32_t Hash, const void *Full);

index_result_t linear_index0_insert2(linear_index0_t *Store, uint32_t Hash, const void *Full);
index_result_t linear_index0_insert2(linear_index0_t *Store, uint32_t Hash, const void *Full);

#endif
