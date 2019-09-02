#ifndef RADB_H
#define RADB_H

#include <stddef.h>

typedef struct radb_t radb_t;

typedef struct radb_object_store_t radb_object_store_t;
typedef struct radb_string_store_t radb_string_store_t;

radb_t *radb_open(const char *Path);

void radb_string_store_reserve(radb_string_store_t *Store, size_t Index);

int radb_string_store_alloc(radb_string_store_t *Store);
void radb_string_store_free(radb_string_store_t *Store, size_t Index);

size_t radb_string_store_get_size(radb_string_store_t *Store, size_t Index);
void radb_string_store_get(radb_string_store_t *Store, size_t Index, void *Buffer);
void radb_string_store_set(radb_string_store_t *Store, size_t Index, void *Buffer, size_t Size);



#endif
