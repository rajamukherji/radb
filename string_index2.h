#ifndef STRING_INDEX2_H
#define STRING_INDEX2_H

#include "linear_index.h"

typedef struct linear_index_t string_index2_t;

string_index2_t *string_index2_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
string_index2_t *string_index2_open(const char *Prefix RADB_MEM_PARAMS);
size_t string_index2_num_entries(string_index2_t *Store);
#define string_index2_count string_index2_num_entries
size_t string_index2_num_deleted(string_index2_t *Store);
void string_index2_close(string_index2_t *Store);

linear_index_open_t string_index2_open2(const char *Prefix RADB_MEM_PARAMS);

size_t string_index2_insert(string_index2_t *Store, const char *Key, size_t Length);
size_t string_index2_search(string_index2_t *Store, const char *Key, size_t Length);

index_result_t string_index2_insert2(string_index2_t *Store, const char *Key, size_t Length);

size_t string_index2_size(string_index2_t *Store, size_t Index);
size_t string_index2_get(string_index2_t *Store, size_t Index, void *Buffer, size_t Space);
size_t string_index2_delete(string_index2_t *Store, const char *Key, size_t Length);

#endif
