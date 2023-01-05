#include "fixed_index2.h"
#include "fixed_index.h"
#include "fixed_store.h"
#include <string.h>

typedef struct {
	const void *Value;
	size_t Size;
} fixed_key_t;

static int linear_compare_fixed(fixed_store_t *Store, fixed_key_t *Full, uint32_t Index) {
	return memcmp(Full->Value, fixed_store_get(Store, Index), Full->Size);
}

static int linear_compare_nop(fixed_store_t *Store, void *Full, uint32_t Index) {
	return 0;
}

static size_t linear_insert_exact(fixed_store_t *Store, void *Value) {
	fixed_store_alloc_t Alloc = fixed_store_alloc2(Store);
	memcpy(Alloc.Value, Value, sizeof(linear_key_t));
	return Alloc.Index;
}

static size_t linear_insert_fixed(fixed_store_t *Store, fixed_key_t *Full) {
	fixed_store_alloc_t Alloc = fixed_store_alloc2(Store);
	memcpy(Alloc.Value, Full->Value, Full->Size);
	return Alloc.Index;
}

fixed_index2_t *fixed_index2_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS) {
	fixed_store_t *Keys = fixed_store_create(Prefix, KeySize, ChunkSize RADB_MEM_ARGS);
	linear_index_t *Index = linear_index_create(Prefix, Keys RADB_MEM_ARGS);
	if (KeySize == sizeof(linear_key_t)) {
		linear_index_set_compare(Index, (linear_compare_t)linear_compare_nop);
		linear_index_set_insert(Index, (linear_insert_t)linear_insert_exact);
	} else if (KeySize > sizeof(linear_key_t)) {
		linear_index_set_compare(Index, (linear_compare_t)linear_compare_fixed);
		linear_index_set_insert(Index, (linear_insert_t)linear_insert_fixed);
	} else {
		linear_index_set_compare(Index, (linear_compare_t)linear_compare_nop);
		linear_index_set_insert(Index, (linear_insert_t)linear_insert_fixed);
	}
	linear_index_set_extra(Index, KeySize);
	return Index;
}

static int migrate_compare_fixed(fixed_store_t *Store, size_t *Original, uint32_t Index) {
	return 1;
}

static size_t migrate_insert_fixed(fixed_store_t *Store, size_t *Original) {
	return *Original;
}

typedef struct {
	linear_index_t *Index;
	fixed_store_t *Store;
	size_t Size;
} migration_t;

static uint32_t fixed_hash(const void *Value, size_t Length) {
	const unsigned char *Bytes = (const unsigned char *)Value;
	uint32_t Hash = 5381;
	for (int I = 0; I < Length; ++I) Hash = ((Hash << 5) + Hash) + Bytes[I];
	return Hash;
}

static int migrate(size_t Index, migration_t *Migration) {
	unsigned char *Value = fixed_store_get(Migration->Store, Index);
	uint32_t Hash = fixed_hash(Value, Migration->Size);
	if (Migration->Size >= sizeof(linear_key_t)) {
		linear_index_insert(Migration->Index, Hash, Value, &Index);
	} else {
		linear_key_t Key = {0,};
		memcpy(Key, Value, Migration->Size);
		linear_index_insert(Migration->Index, Hash, Key, &Index);
	}
	return 0;
}

linear_index_open_t fixed_index2_open2(const char *Prefix RADB_MEM_PARAMS) {
	fixed_store_open_t KeysOpen = fixed_store_open2(Prefix RADB_MEM_ARGS);
	if (!KeysOpen.Store) return (linear_index_open_t){NULL, KeysOpen.Error + 3};
	linear_index_open_t IndexOpen = linear_index_open2(Prefix, KeysOpen.Store RADB_MEM_ARGS);
	if (IndexOpen.Error == RADB_FILE_NOT_FOUND) {
		fixed_index_open_t OldOpen = fixed_index_open2(Prefix RADB_MEM_ARGS);
		if (OldOpen.Error != RADB_SUCCESS) {
			fixed_store_close(KeysOpen.Store);
			return IndexOpen;
		}
		linear_index_t *NewIndex = linear_index_create(Prefix, KeysOpen.Store RADB_MEM_ARGS);
		linear_index_set_compare(NewIndex, (linear_compare_t)migrate_compare_fixed);
		linear_index_set_insert(NewIndex, (linear_insert_t)migrate_insert_fixed);
		size_t Size = fixed_index_key_size(OldOpen.Index);
		linear_index_set_extra(NewIndex, Size);
		migration_t Migration = {NewIndex, KeysOpen.Store, Size};
		fixed_index_foreach(OldOpen.Index, &Migration, (void *)migrate);
		fixed_index_close(OldOpen.Index);
		IndexOpen.Index = NewIndex;
		IndexOpen.Error = RADB_SUCCESS;
	}
	if (IndexOpen.Error != RADB_SUCCESS) fixed_store_close(KeysOpen.Store);
	size_t KeySize = linear_index_get_extra(IndexOpen.Index);
	if (KeySize == sizeof(linear_key_t)) {
		linear_index_set_compare(IndexOpen.Index, (linear_compare_t)linear_compare_nop);
		linear_index_set_insert(IndexOpen.Index, (linear_insert_t)linear_insert_exact);
	} else if (KeySize > sizeof(linear_key_t)) {
		linear_index_set_compare(IndexOpen.Index, (linear_compare_t)linear_compare_fixed);
		linear_index_set_insert(IndexOpen.Index, (linear_insert_t)linear_insert_fixed);
	} else {
		linear_index_set_compare(IndexOpen.Index, (linear_compare_t)linear_compare_nop);
		linear_index_set_insert(IndexOpen.Index, (linear_insert_t)linear_insert_fixed);
	}
	return IndexOpen;
}

fixed_index2_t *fixed_index2_open(const char *Prefix RADB_MEM_PARAMS) {
	return fixed_index2_open2(Prefix RADB_MEM_ARGS).Index;
}

size_t fixed_index2_count(fixed_index2_t *Store) {
	return linear_index_count(Store);
}

void fixed_index2_close(fixed_index2_t *Store) {
	fixed_store_close(linear_index_keys(Store));
	linear_index_close(Store);
}

size_t fixed_index2_insert(fixed_index2_t *Store, const void *Value) {
	size_t Length = linear_index_get_extra(Store);
	uint32_t Hash = fixed_hash(Value, Length);
	fixed_key_t Full = {Value, Length};
	if (Length == sizeof(linear_key_t)) {
		return linear_index_insert(Store, Hash, Value, Value);
	} else if (Length > sizeof(linear_key_t)) {
		return linear_index_insert(Store, Hash, Value, &Full);
	} else {
		linear_key_t Key = {0,};
		memcpy(Key, Value, Length);
		return linear_index_insert(Store, Hash, Key, &Full);
	}
}

size_t fixed_index2_search(fixed_index2_t *Store, const void *Value) {
	size_t Length = linear_index_get_extra(Store);
	uint32_t Hash = fixed_hash(Value, Length);
	fixed_key_t Full = {Value, Length};
	if (Length == sizeof(linear_key_t)) {
		return linear_index_search(Store, Hash, Value, Value);
	} else if (Length > sizeof(linear_key_t)) {
		return linear_index_search(Store, Hash, Value, &Full);
	} else {
		linear_key_t Key = {0,};
		memcpy(Key, Value, Length);
		return linear_index_search(Store, Hash, Key, &Full);
	}
}

index_result_t fixed_index2_insert2(fixed_index2_t *Store, const void *Value) {
	size_t Length = linear_index_get_extra(Store);
	uint32_t Hash = fixed_hash(Value, Length);
	fixed_key_t Full = {Value, Length};
	if (Length == sizeof(linear_key_t)) {
		return linear_index_insert2(Store, Hash, Value, Value);
	} else if (Length > sizeof(linear_key_t)) {
		return linear_index_insert2(Store, Hash, Value, &Full);
	} else {
		linear_key_t Key = {0,};
		memcpy(Key, Value, Length);
		return linear_index_insert2(Store, Hash, Key, &Full);
	}
}

const void *fixed_index2_get(fixed_index2_t *Store, size_t Index) {
	return fixed_store_get(linear_index_keys(Store), Index);
}

size_t fixed_index2_delete(fixed_index2_t *Store, const void *Value) {
	return INVALID_INDEX;
}
