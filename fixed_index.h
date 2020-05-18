#ifndef FIXED_INDEX_H
#define FIXED_INDEX_H

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct fixed_index_t fixed_index_t;

fixed_index_t *fixed_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize);
fixed_index_t *fixed_index_open(const char *Prefix);
size_t fixed_index_count(fixed_index_t *Store);
void fixed_index_close(fixed_index_t *Store);

size_t fixed_index_insert(fixed_index_t *Store, const char *Key);
size_t fixed_index_search(fixed_index_t *Store, const char *Key);
const void *fixed_index_get(fixed_index_t *Store, size_t Index);
size_t fixed_index_delete(fixed_index_t *Store, const char *Key);

#endif
