#ifndef STRING_STORE_H
#define STRING_STORE_H

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct string_store_t string_store_t;

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, int Index);
string_store_t *string_store_open(const char *Prefix);
void string_store_close(string_store_t *Store);

void string_store_reserve(string_store_t *Store, size_t Index);

size_t string_store_alloc(string_store_t *Store);
void string_store_free(string_store_t *Store, size_t Index);

size_t string_store_get_size(string_store_t *Store, size_t Index);
void string_store_get_value(string_store_t *Store, size_t Index, void *Buffer);
void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length);

size_t string_store_insert(string_store_t *Store, const void *Buffer, size_t Length);
size_t string_store_lookup(string_store_t *Store, const void *Buffer, size_t Length);

#endif
