#ifndef RADB_COMMON_H
#define RADB_COMMON_H

#include <stddef.h>
#include <stdint.h>

#define INVALID_INDEX 0xFFFFFFFF
#define DELETED_INDEX 0xFFFFFFFE

typedef enum {
	RADB_SUCCESS,
	RADB_FILE_NOT_FOUND,
	RADB_HEADER_MISMATCH,
	RADB_HEADER_CORRUPTED,
	RADB_KEYS_FILE_NOT_FOUND,
	RADB_KEYS_HEADER_MISMATCH,
	RADB_KEYS_HEADER_CORRUPTED,
	RADB_FILE_LOCKED
} radb_error_t;

const char *radb_error_string(radb_error_t Error);

typedef struct {
	size_t Index;
	int Created;
} index_result_t;

#endif
