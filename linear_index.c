#include "linear_index.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

typedef struct {
	linear_key_t Key;
	uint32_t Index;
	uint32_t Value;
} linear_entry_t;

typedef struct {
	uint32_t Signature, Version;
	uint32_t NumOffset, NumEntry;
	uint32_t MaxOffset, MaxEntry;
	uint32_t Reserved1, Reserved2;
	uint32_t Offsets[];
} linear_header_t;

struct linear_index_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	linear_header_t *Header;
	linear_entry_t *Table;
	void *Keys;
	int (*Compare)(void *Keys, void *Full, uint32_t Index);
	uint32_t (*Insert)(void *Keys, void *Full);
	size_t HeaderSize, TableSize;
	int HeaderFd, TableFd;
};

#ifdef RADB_MEM_GC
#include <gc/gc.h>
#endif

#ifdef RADB_MEM_PER_STORE
static inline const char *radb_strdup(const char *String, void *Allocator, void *(*alloc_atomic)(void *, size_t)) {
	size_t Length = strlen(String);
	char *Copy = alloc_atomic(Allocator, Length + 1);
	strcpy(Copy, String);
	return Copy;
}
#endif

#define MAKE_VERSION(MAJOR, MINOR) (0xFF000000 + (MAJOR << 16) + (MINOR << 8))

#define LINEAR_INDEX_SIGNATURE 0x494C4152
#define LINEAR_INDEX_VERSION MAKE_VERSION(1, 0)

linear_index_t *linear_index_create(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS) {
#if defined(RADB_MEM_MALLOC)
	linear_index_t *Store = malloc(sizeof(linear_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	linear_index_t *Store = GC_malloc(sizeof(linear_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	linear_index_t *Store = alloc(Allocator, sizeof(linear_index_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.offsets", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Store->HeaderSize = 512;
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = LINEAR_INDEX_SIGNATURE;
	Store->Header->Version = LINEAR_INDEX_VERSION;
	Store->Header->MaxOffset = (512 - sizeof(linear_header_t)) / sizeof(uint32_t);
	Store->Header->NumOffset = 1;
	Store->Header->MaxEntry = 512 / sizeof(linear_entry_t);
	Store->Header->NumEntry = 0;
	Store->Header->Offsets[0] = INVALID_INDEX;
	sprintf(FileName, "%s.table", Prefix);
	Store->TableFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Store->TableSize = 512;
	ftruncate(Store->TableFd, Store->TableSize);
	Store->Table = mmap(NULL, Store->TableSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->TableFd, 0);
	Store->Keys = Keys;
	Store->Compare = Compare;
	Store->Insert = Insert;
	return Store;
}

linear_index_open_t linear_index_open_v2(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.offsets", Prefix);
	if (stat(FileName, Stat)) return (linear_index_open_t){NULL, RADB_FILE_NOT_FOUND};
#if defined(RADB_MEM_MALLOC)
	linear_index_t *Store = malloc(sizeof(linear_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	linear_index_t *Store = GC_malloc(sizeof(linear_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	linear_index_t *Store = alloc(Allocator, sizeof(linear_index_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	if (Store->Header->Signature != LINEAR_INDEX_SIGNATURE) {
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		return (linear_index_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	sprintf(FileName, "%s.table", Prefix);
	if (stat(FileName, Stat)) {
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		return (linear_index_open_t){NULL, RADB_FILE_NOT_FOUND};
	}
	Store->TableFd = open(FileName, O_RDWR, 0777);
	Store->TableSize = Stat->st_size;
	Store->Table = mmap(NULL, Store->TableSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->TableFd, 0);
	return (linear_index_open_t){Store, RADB_SUCCESS};
}

linear_index_t *linear_index_open(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS) {
	return linear_index_open_v2(Prefix, Keys, Compare, Insert RADB_MEM_ARGS).Index;
}

void linear_index_close(linear_index_t *Store) {
	msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->HeaderFd);
	msync(Store->Table, Store->TableSize, MS_SYNC);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->TableFd);
#if defined(RADB_MEM_MALLOC)
	free((void *)Store->Prefix);
	free(Store);
#elif defined(RADB_MEM_GC)
#else
	Store->free(Store->Allocator, (void *)Store->Prefix);
	Store->free(Store->Allocator, Store);
#endif
}

size_t linear_index_search(linear_index_t *Store, linear_key_t Key, void *Full) {
	size_t NumOffset = Store->Header->NumOffset;
	size_t Scale = NumOffset > 1 ? 1 << (64 - __builtin_clzl(NumOffset - 1)) : 1;
	size_t Index = *(uint32_t *)Key & (Scale - 1);
	if (Index >= NumOffset) Index -= (Scale >> 1);
	size_t Offset = Store->Header->Offsets[Index];
	if (Offset == INVALID_INDEX) return INVALID_INDEX;
	linear_entry_t *Last = Store->Table + Store->Header->NumEntry;
	for (linear_entry_t *Entry = Store->Table + Offset; Entry < Last; ++Entry) {
		if (Entry->Index == INVALID_INDEX) {
			return INVALID_INDEX;
		} else if (Entry->Index != Index) {
			return INVALID_INDEX;
		} else if (!memcmp(Entry->Key, Key, sizeof(linear_key_t)) && !Store->Compare(Store->Keys, Full, Entry->Index)) {
			return Entry->Value;
		}
	}
	return INVALID_INDEX;
}

static void linear_index_add_offset(linear_index_t *Store) {
	size_t NumOffset = Store->Header->NumOffset;
	if (NumOffset == Store->Header->MaxOffset) {
		size_t HeaderSize = Store->HeaderSize + 512;
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		Store->Header->MaxOffset = (HeaderSize - sizeof(linear_header_t)) / sizeof(uint32_t);
		Store->HeaderSize = HeaderSize;
	}
	size_t Scale = 1 << (64 - __builtin_clzl(NumOffset));
	size_t Shift = Scale >> 1;
	size_t Index = Scale > NumOffset ? NumOffset - Shift : NumOffset & (Scale - 1);
	size_t Offset = Store->Header->Offsets[Index];
	if (Offset == INVALID_INDEX) {
		Store->Header->Offsets[Store->Header->NumOffset++] = INVALID_INDEX;
		return;
	}
	linear_entry_t *Table = Store->Table;
	linear_entry_t *First = Table + Offset, *Last = First, *A = First;
	linear_entry_t *Limit = Table + Store->Header->NumEntry;
	while (Last < Limit) {
		if (Last->Index != Index) break;
		++Last;
	}
	linear_entry_t *B = Last;
	Store->Header->NumOffset= ++NumOffset;
	while (A < B) {
		size_t NewIndex = *(uint32_t *)A->Key & (Scale - 1);
		if (NewIndex >= NumOffset) NewIndex -= Shift;
		if (NewIndex == Index) {
			++A;
		} else {
			--B;
			linear_entry_t Temp;
			memcpy(&Temp, A, sizeof(linear_entry_t));
			memcpy(A, B, sizeof(linear_entry_t));
			memcpy(B, &Temp, sizeof(linear_entry_t));
			B->Index = NewIndex;
		}
	}
	if (B == Last) {
		Store->Header->Offsets[NumOffset - 1] = INVALID_INDEX;
	} else {
		if (B == First) Store->Header->Offsets[Index] = INVALID_INDEX;
		Store->Header->Offsets[NumOffset - 1] = B - Table;
	}
}

static void linear_index_alloc_entries(linear_index_t *Store, size_t Count) {
	size_t NumEntry = Store->Header->NumEntry;
	if (NumEntry + Count >= Store->Header->MaxEntry) {
		size_t Allocation = ((Count * sizeof(linear_entry_t) + 511) / 512) * 512;
		size_t TableSize = Store->TableSize + Allocation;
		ftruncate(Store->TableFd, TableSize);
#ifdef Linux
		Store->Table = mremap(Store->Table, Store->TableSize, TableSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Table, Store->TableSize);
		Store->Table = mmap(NULL, TableSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->TableFd, 0);
#endif
		Store->Header->MaxEntry = TableSize / sizeof(linear_entry_t);
		Store->TableSize = TableSize;
	}
}

static linear_index_result_t linear_index_put_entry(linear_index_t *Store, uint32_t Index, linear_key_t Key, void *Full) {
	linear_index_alloc_entries(Store, 1);
	size_t Offset = Store->Header->NumEntry++;
	linear_entry_t *Entry = Store->Table + Offset;
	Entry->Index = Index;
	memcpy(Entry->Key, Key, sizeof(linear_key_t));
	uint32_t Insert = Entry->Value = Store->Insert(Store->Keys, Full);
	linear_index_add_offset(Store);
	return (linear_index_result_t){Insert, 1};
}

linear_index_result_t linear_index_insert2(linear_index_t *Store, linear_key_t Key, void *Full) {
	size_t NumOffset = Store->Header->NumOffset;
	size_t Scale = NumOffset > 1 ? 1 << (64 - __builtin_clzl(NumOffset - 1)) : 1;
	size_t Index = *(uint32_t *)Key & (Scale - 1);
	if (Index >= NumOffset) Index -= (Scale >> 1);
	size_t Offset = Store->Header->Offsets[Index];
	if (Offset == INVALID_INDEX) {
		Store->Header->Offsets[Index] = Store->Header->NumEntry;
		return linear_index_put_entry(Store, Index, Key, Full);
	}
	linear_entry_t *Last = Store->Table + Store->Header->NumEntry;
	for (linear_entry_t *Entry = Store->Table + Offset; Entry < Last; ++Entry) {
		if (Entry->Index == INVALID_INDEX) {
			Entry->Index = Index;
			memcpy(Entry->Key, Key, sizeof(linear_key_t));
			uint32_t Insert = Entry->Value = Store->Insert(Store->Keys, Full);
			linear_index_add_offset(Store);
			return (linear_index_result_t){Insert, 1};
		} else if (Entry->Index != Index) {
			if (Offset > 0 && Store->Table[Offset - 1].Index == INVALID_INDEX) {
				Store->Header->Offsets[Index] = Offset - 1;
				linear_entry_t *Entry2 = Store->Table + (Offset - 1);
				Entry2->Index = Index;
				memcpy(Entry2->Key, Key, sizeof(linear_key_t));
				uint32_t Insert = Entry2->Value = Store->Insert(Store->Keys, Full);
				linear_index_add_offset(Store);
				return (linear_index_result_t){Insert, 1};
			} else {
				size_t Count = (Entry - Store->Table) - Offset;
				linear_index_alloc_entries(Store, Count + 1);
				Store->Header->Offsets[Index] = Store->Header->NumEntry;
				linear_entry_t *Entry2 = mempcpy(Last, Store->Table + Offset, Count * sizeof(linear_entry_t));
				Entry2->Index = Index;
				memcpy(Entry2->Key, Key, sizeof(linear_key_t));
				Store->Header->NumEntry += (Count + 1);
				uint32_t Insert = Entry2->Value = Store->Insert(Store->Keys, Full);
				linear_index_add_offset(Store);
				return (linear_index_result_t){Insert, 1};
			}
		} else if (!memcmp(Entry->Key, Key, sizeof(linear_key_t)) && !Store->Compare(Store->Keys, Full, Entry->Index)) {
			return (linear_index_result_t){Entry->Value, 0};
		}
	}
	return linear_index_put_entry(Store, Index, Key, Full);
}

size_t linear_index_insert(linear_index_t *Store, linear_key_t Key, void *Full) {
	return linear_index_insert2(Store, Key, Full).Index;
}
