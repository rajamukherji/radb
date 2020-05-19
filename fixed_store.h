#ifndef FIXED_STORE_H
#define FIXED_STORE_H

#include "common.h"

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct fixed_store_t fixed_store_t;

fixed_store_t *fixed_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS);
fixed_store_t *fixed_store_open(const char *Prefix RADB_MEM_PARAMS);
void fixed_store_close(fixed_store_t *Store);

void *fixed_store_get(fixed_store_t *Store, size_t Index);

size_t fixed_store_alloc(fixed_store_t *Store);
void fixed_store_free(fixed_store_t *Store, size_t Index);

#endif
