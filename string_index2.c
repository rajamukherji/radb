#include "string_index2.h"
#include "string_index.h"
#include "string_store.h"
#include <string.h>

typedef struct {
	const char *String;
	size_t Length;
} string_key_t;

static int linear_compare_string(string_store_t *Store, string_key_t *Full, uint32_t Index) {
	if (Full->Length < sizeof(linear_key_t)) return 0;
	return string_store_compare(Store, Full->String, Full->Length, Index);
}

static size_t linear_insert_string(string_store_t *Store, string_key_t *Full) {
	size_t Index = string_store_alloc(Store);
	string_store_set(Store, Index, Full->String, Full->Length);
	return Index;
}

string_index2_t *string_index2_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS) {
	string_store_t *Keys = string_store_create(Prefix, KeySize, ChunkSize RADB_MEM_ARGS);
	linear_index_t *Index = linear_index_create(Prefix, Keys RADB_MEM_ARGS);
	linear_index_set_compare(Index, (linear_compare_t)linear_compare_string);
	linear_index_set_insert(Index, (linear_insert_t)linear_insert_string);
	return Index;
}

static int migrate_compare_string(string_store_t *Store, size_t *Original, uint32_t Index) {
	return 1;
}

static size_t migrate_insert_string(string_store_t *Store, size_t *Original) {
	return *Original;
}

typedef struct {
	linear_index_t *Index;
	string_store_t *Store;
} migration_t;

static int migrate(size_t Index, migration_t *Migration) {
	linear_key_t Key = {0,};
	string_store_reader_t Reader;
	string_store_reader_open(&Reader, Migration->Store, Index);
	size_t Read = string_store_reader_read(&Reader, Key, sizeof(linear_key_t));
	uint32_t Hash = 5381;
	for (int I = 0; I < Read; ++I) Hash = ((Hash << 5) + Hash) + Key[I];
	if (Read == sizeof(linear_key_t)) {
		Key[sizeof(linear_key_t) - 1] = 1;
		unsigned char Buffer[16];
		do {
			Read = string_store_reader_read(&Reader, Buffer, 16);
			for (int I = 0; I < Read; ++I) Hash = ((Hash << 5) + Hash) + Buffer[I];
		} while (Read == 16);
	}
	linear_index_insert(Migration->Index, Hash, Key, &Index);
	return 0;
}

linear_index_open_t string_index2_open2(const char *Prefix RADB_MEM_PARAMS) {
	string_store_open_t KeysOpen = string_store_open2(Prefix RADB_MEM_ARGS);
	if (!KeysOpen.Store) return (linear_index_open_t){NULL, KeysOpen.Error + 3};
	linear_index_open_t IndexOpen = linear_index_open2(Prefix, KeysOpen.Store RADB_MEM_ARGS);
	if (IndexOpen.Error == RADB_FILE_NOT_FOUND) {
		string_index_open_t OldOpen = string_index_open2(Prefix RADB_MEM_ARGS);
		if (OldOpen.Error != RADB_SUCCESS) {
			string_store_close(KeysOpen.Store);
			return IndexOpen;
		}
		linear_index_t *NewIndex = linear_index_create(Prefix, KeysOpen.Store RADB_MEM_ARGS);
		linear_index_set_compare(NewIndex, (linear_compare_t)migrate_compare_string);
		linear_index_set_insert(NewIndex, (linear_insert_t)migrate_insert_string);
		migration_t Migration = {NewIndex, KeysOpen.Store};
		string_index_foreach(OldOpen.Index, &Migration, (void *)migrate);
		string_index_close(OldOpen.Index);
		IndexOpen.Index = NewIndex;
		IndexOpen.Error = RADB_SUCCESS;
	}
	if (IndexOpen.Error != RADB_SUCCESS) string_store_close(KeysOpen.Store);
	linear_index_set_compare(IndexOpen.Index, (linear_compare_t)linear_compare_string);
	linear_index_set_insert(IndexOpen.Index, (linear_insert_t)linear_insert_string);
	return IndexOpen;
}

string_index2_t *string_index2_open(const char *Prefix RADB_MEM_PARAMS) {
	return string_index2_open2(Prefix RADB_MEM_ARGS).Index;
}

size_t string_index2_count(string_index2_t *Store) {
	return linear_index_count(Store);
}

void string_index2_close(string_index2_t *Store) {
	string_store_close(linear_index_keys(Store));
	linear_index_close(Store);
}

static uint32_t string_hash(const char *String, size_t Length) {
	const unsigned char *Bytes = (const unsigned char *)String;
	uint32_t Hash = 5381;
	for (int I = 0; I < Length; ++I) Hash = ((Hash << 5) + Hash) + Bytes[I];
	return Hash;
}

size_t string_index2_insert(string_index2_t *Store, const char *String, size_t Length) {
	if (!Length) Length = strlen(String);
	uint32_t Hash = string_hash(String, Length);
	string_key_t Full = {String, Length};
	linear_key_t Key = {0,};
	if (Length >= sizeof(linear_key_t)) {
		memcpy(Key, String, sizeof(linear_key_t) - 1);
		Key[sizeof(linear_key_t) - 1] = 1;
	} else {
		memcpy(Key, String, Length);
	}
	return linear_index_insert(Store, Hash, Key, &Full);
}

size_t string_index2_search(string_index2_t *Store, const char *String, size_t Length) {
	if (!Length) Length = strlen(String);
	uint32_t Hash = string_hash(String, Length);
	string_key_t Full = {String, Length};
	linear_key_t Key = {0,};
	if (Length >= sizeof(linear_key_t)) {
		memcpy(Key, String, sizeof(linear_key_t) - 1);
		Key[sizeof(linear_key_t) - 1] = 1;
	} else {
		memcpy(Key, String, Length);
	}
	return linear_index_search(Store, Hash, Key, &Full);
}

index_result_t string_index2_insert2(string_index2_t *Store, const char *String, size_t Length) {
	if (!Length) Length = strlen(String);
	uint32_t Hash = string_hash(String, Length);
	string_key_t Full = {String, Length};
	linear_key_t Key = {0,};
	if (Length >= sizeof(linear_key_t)) {
		memcpy(Key, String, sizeof(linear_key_t) - 1);
		Key[sizeof(linear_key_t) - 1] = 1;
	} else {
		memcpy(Key, String, Length);
	}
	return linear_index_insert2(Store, Hash, Key, &Full);
}

size_t string_index2_size(string_index2_t *Store, size_t Index) {
	return string_store_size(linear_index_keys(Store), Index);
}

size_t string_index2_get(string_index2_t *Store, size_t Index, void *Buffer, size_t Space) {
	return string_store_get(linear_index_keys(Store), Index, Buffer, Space);
}

size_t string_index2_delete(string_index2_t *Store, const char *String, size_t Length) {
	return INVALID_INDEX;
}
