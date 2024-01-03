#ifndef FIXED_INDEX_H
#define FIXED_INDEX_H

#include "config.h"
#include "common.h"

#define INVALID_INDEX 0xFFFFFFFF
#define DELETED_INDEX 0xFFFFFFFE

typedef struct fixed_index_t fixed_index_t;

fixed_index_t *fixed_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
fixed_index_t *fixed_index_open(const char *Prefix RADB_MEM_PARAMS);
size_t fixed_index_num_entries(fixed_index_t *Store);
#define fixed_index_count fixed_index_num_entries
size_t fixed_index_num_deleted(fixed_index_t *Store);
void fixed_index_close(fixed_index_t *Store);

typedef struct {
	fixed_index_t *Index;
	radb_error_t Error;
} fixed_index_open_t;

fixed_index_open_t fixed_index_open2(const char *Prefix RADB_MEM_PARAMS);

size_t fixed_index_insert(fixed_index_t *Store, const char *Key);
size_t fixed_index_search(fixed_index_t *Store, const char *Key);

index_result_t fixed_index_insert2(fixed_index_t *Store, const char *Key);

const void *fixed_index_get(fixed_index_t *Store, size_t Index);
size_t fixed_index_delete(fixed_index_t *Store, const char *Key);

uint32_t fixed_index_key_size(fixed_index_t *Store);

typedef int (*fixed_index_foreach_fn)(size_t Index, void *Data);
int fixed_index_foreach(fixed_index_t *Store, void *Data, fixed_index_foreach_fn Callback);

#endif
