#ifndef FIXED_STORE_H
#define FIXED_STORE_H

#include "config.h"
#include "common.h"

#define INVALID_INDEX 0xFFFFFFFF

typedef struct fixed_store_t fixed_store_t;

fixed_store_t *fixed_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS);
fixed_store_t *fixed_store_open(const char *Prefix RADB_MEM_PARAMS);
void fixed_store_close(fixed_store_t *Store);

typedef struct {
	fixed_store_t *Store;
	radb_error_t Error;
} fixed_store_open_t;

fixed_store_open_t fixed_store_open2(const char *Prefix RADB_MEM_PARAMS);

size_t fixed_store_num_entries(fixed_store_t *Store);

void *fixed_store_get(fixed_store_t *Store, size_t Index);

void fixed_store_shift(fixed_store_t *Store, size_t Source, size_t Count, size_t Destination);

size_t fixed_store_alloc(fixed_store_t *Store);
void fixed_store_free(fixed_store_t *Store, size_t Index);

#endif
