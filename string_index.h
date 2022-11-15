#ifndef STRING_INDEX_H
#define STRING_INDEX_H

#include "config.h"
#include "common.h"

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF
#define DELETED_INDEX 0xFFFFFFFE

typedef struct string_index_t string_index_t;

string_index_t *string_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
string_index_t *string_index_open(const char *Prefix RADB_MEM_PARAMS);
size_t string_index_num_entries(string_index_t *Store);
#define string_index_count string_index_num_entries
size_t string_index_num_deleted(string_index_t *Store);
void string_index_close(string_index_t *Store);

typedef struct {
	string_index_t *Index;
	radb_error_t Error;
} string_index_open_t;

string_index_open_t string_index_open_v2(const char *Prefix RADB_MEM_PARAMS);

size_t string_index_insert(string_index_t *Store, const char *Key, size_t Length);
size_t string_index_search(string_index_t *Store, const char *Key, size_t Length);

typedef struct {
	size_t Index;
	int Created;
} string_index_result_t;

string_index_result_t string_index_insert2(string_index_t *Store, const char *Key, size_t Length);

size_t string_index_size(string_index_t *Store, size_t Index);
size_t string_index_get(string_index_t *Store, size_t Index, void *Buffer, size_t Space);
size_t string_index_delete(string_index_t *Store, const char *Key, size_t Length);

#endif
