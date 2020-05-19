#ifndef STRING_INDEX_H
#define STRING_INDEX_H

#include "common.h"

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct string_index_t string_index_t;

string_index_t *string_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
string_index_t *string_index_open(const char *Prefix RADB_MEM_PARAMS);
size_t string_index_count(string_index_t *Store);
void string_index_close(string_index_t *Store);

size_t string_index_insert(string_index_t *Store, const char *Key);
size_t string_index_search(string_index_t *Store, const char *Key);
const char *string_index_get(string_index_t *Store, size_t Index);
size_t string_index_delete(string_index_t *Store, const char *Key);

#endif
