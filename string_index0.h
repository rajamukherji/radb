#ifndef STRING_INDEX0_H
#define STRING_INDEX0_H

#include "linear_index0.h"

typedef struct linear_index0_t string_index0_t;

string_index0_t *string_index0_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS);
string_index0_t *string_index0_open(const char *Prefix RADB_MEM_PARAMS);
size_t string_index0_num_entries(string_index0_t *Store);
#define string_index0_count string_index0_num_entries
size_t string_index0_num_deleted(string_index0_t *Store);
void string_index0_close(string_index0_t *Store);

linear_index0_open_t string_index0_open2(const char *Prefix RADB_MEM_PARAMS);

size_t string_index0_insert(string_index0_t *Store, const char *Key, size_t Length);
size_t string_index0_search(string_index0_t *Store, const char *Key, size_t Length);

index_result_t string_index0_insert2(string_index0_t *Store, const char *Key, size_t Length);

size_t string_index0_size(string_index0_t *Store, size_t Index);
size_t string_index0_get(string_index0_t *Store, size_t Index, void *Buffer, size_t Space);
size_t string_index0_delete(string_index0_t *Store, const char *Key, size_t Length);

#endif
