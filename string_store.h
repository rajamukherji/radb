#ifndef STRING_STORE_H
#define STRING_STORE_H

#include <stddef.h>

#define INVALID_INDEX 0xFFFFFFFF

typedef struct string_store_t string_store_t;
typedef struct string_store_writer_t string_store_writer_t;
typedef struct string_store_reader_t string_store_reader_t;

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize);
string_store_t *string_store_open(const char *Prefix);
void string_store_close(string_store_t *Store);

size_t string_store_get_size(string_store_t *Store, size_t Index);
void string_store_get_value(string_store_t *Store, size_t Index, void *Buffer);
void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length);

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
