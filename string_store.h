#ifndef STRING_STORE_H
#define STRING_STORE_H

#include "config.h"

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct string_store_t string_store_t;
typedef struct string_store_writer_t string_store_writer_t;
typedef struct string_store_reader_t string_store_reader_t;

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS);
string_store_t *string_store_open(const char *Prefix RADB_MEM_PARAMS);
void string_store_close(string_store_t *Store);

size_t string_store_num_entries(string_store_t *Store);

size_t string_store_size(string_store_t *Store, size_t Index);
void string_store_get(string_store_t *Store, size_t Index, void *Buffer);
void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length);

int string_store_compare(string_store_t *Store, const void *Other, size_t Length, size_t Index);
int string_store_compare2(string_store_t *Store, size_t Index1, size_t Index2);

size_t string_store_alloc(string_store_t *Store);
void string_store_free(string_store_t *Store, size_t Index);

struct string_store_writer_t {
	string_store_t *Store;
	void *Node;
	size_t Index, Remain;
};

void string_store_writer_open(string_store_writer_t *Writer, string_store_t *Store, size_t Index);
size_t string_store_writer_write(string_store_writer_t *Writer, const void *Buffer, size_t Length);

struct string_store_reader_t {
	string_store_t *Store;
	void *Node;
	size_t Offset, Remain;
};

void string_store_reader_open(string_store_reader_t *Reader, string_store_t *Store, size_t Index);
size_t string_store_reader_read(string_store_reader_t *Reader, void *Buffer, size_t Length);

#endif
