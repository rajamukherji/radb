#ifndef STRING_INDEX_H
#define STRING_INDEX_H

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct string_index_t string_index_t;

string_index_t *string_index_create(const char *Prefix);
string_index_t *string_index_open(const char *Prefix);
void string_index_close(string_index_t *Index);

size_t string_index_insert(string_index_t *Index, const void *Key);
size_t string_index_search(string_index_t *Store, const void *Key);

#endif
