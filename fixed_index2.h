#ifndef FIXED_INDEX2_H
#define FIXED_INDEX2_H

#include "linear_index.h"

typedef struct linear_index_t fixed_index2_t;

fixed_index2_t *fixed_index2_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
fixed_index2_t *fixed_index2_open(const char *Prefix RADB_MEM_PARAMS);
size_t fixed_index2_num_entries(fixed_index2_t *Store);
#define fixed_index2_count fixed_index2_num_entries
size_t fixed_index2_num_deleted(fixed_index2_t *Store);
void fixed_index2_close(fixed_index2_t *Store);

linear_index_open_t fixed_index2_open2(const char *Prefix RADB_MEM_PARAMS);

size_t fixed_index2_insert(fixed_index2_t *Store, const void *Key);
size_t fixed_index2_search(fixed_index2_t *Store, const void *Key);

index_result_t fixed_index2_insert2(fixed_index2_t *Store, const void *Key);

const void *fixed_index2_get(fixed_index2_t *Store, size_t Index);
size_t fixed_index2_delete(fixed_index2_t *Store, const void *Key);

#endif
