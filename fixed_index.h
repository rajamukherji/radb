#ifndef FIXED_INDEX_H
#define FIXED_INDEX_H

#include "config.h"

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF
#define DELETED_INDEX 0xFFFFFFFE

typedef struct fixed_index_t fixed_index_t;

fixed_index_t *fixed_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
fixed_index_t *fixed_index_open(const char *Prefix RADB_MEM_PARAMS);
size_t fixed_index_num_entries(fixed_index_t *Store);
#define fixed_index_count fixed_index_num_entries
size_t fixed_index_num_deleted(fixed_index_t *Store);
void fixed_index_close(fixed_index_t *Store);

size_t fixed_index_insert(fixed_index_t *Store, const char *Key);
size_t fixed_index_search(fixed_index_t *Store, const char *Key);

typedef struct {
	size_t Index;
	int Created;
} fixed_index_result_t;

fixed_index_result_t fixed_index_insert2(fixed_index_t *Store, const char *Key);

const void *fixed_index_get(fixed_index_t *Store, size_t Index);
size_t fixed_index_delete(fixed_index_t *Store, const char *Key);

#endif
