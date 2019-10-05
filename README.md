# Radb
Raja's Attempt at a Database

## About

`radb` provides a simple but fast data storage library that can be embedded into a 
C application. It intentionally only provides a limited number of functions.

## Usage

```c
string_index_t *string_index_create(const char *Prefix, size_t ChunkSize);
string_index_t *string_index_open(const char *Prefix);
void string_index_close(string_index_t *Index);

size_t string_index_insert(string_index_t *Index, const void *Key);
size_t string_index_search(string_index_t *Store, const void *Key);
```

```c
string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize);
string_store_t *string_store_open(const char *Prefix);
void string_store_close(string_store_t *Store);

size_t string_store_get_size(string_store_t *Store, size_t Index);
void string_store_get_value(string_store_t *Store, size_t Index, void *Buffer);
void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length);
```

```c
fixed_store_t *fixed_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize);
fixed_store_t *fixed_store_open(const char *Prefix);
void fixed_store_close(fixed_store_t *Store);

void *fixed_store_get(fixed_store_t *Store, size_t Index);
```
