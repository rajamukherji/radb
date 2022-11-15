#include "common.h"

const char *radb_error_string(radb_error_t Error) {
	switch (Error) {
	case RADB_SUCCESS: return "success";
	case RADB_FILE_NOT_FOUND: return "file not found";
	case RADB_HEADER_MISMATCH: return "header mismatch";
	case RADB_HEADER_CORRUPTED: return "header corrupted";
	case RADB_KEYS_FILE_NOT_FOUND: return "keys file not found";
	case RADB_KEYS_HEADER_MISMATCH: return "keys header mismatch";
	case RADB_KEYS_HEADER_CORRUPTED: return "keys header corrupted";
	default: return "invalid error";
	}
}
