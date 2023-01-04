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
	uint32_t Offset;
	uint32_t Index;
	uint32_t Hash;
	uint32_t Value;
	linear_key_t Key;
} linear_node_t;

typedef struct {
	uint32_t Signature, Version;
	uint32_t NumOffsets, NumEntries;
	uint32_t NumNodes, NextFree;
	uint32_t Count, Extra;
	linear_node_t Nodes[];
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
	void *Keys;
	linear_compare_t Compare;
	linear_insert_t Insert;
	size_t HeaderSize;
	int HeaderFd;
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
	sprintf(FileName, "%s.index", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Store->HeaderSize = 512;
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = LINEAR_INDEX_SIGNATURE;
	Store->Header->Version = LINEAR_INDEX_VERSION;
	Store->Header->NumNodes = (512 - sizeof(linear_header_t)) / sizeof(linear_node_t);
	Store->Header->NumOffsets = 1;
	Store->Header->NumEntries = 0;
	Store->Header->NextFree = INVALID_INDEX;
	Store->Header->Count = 0;
	Store->Header->Nodes[0].Index = INVALID_INDEX;
	Store->Keys = Keys;
	Store->Compare = Compare;
	Store->Insert = Insert;
	return Store;
}

linear_index_open_t linear_index_open2(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.index", Prefix);
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
	Store->Keys = Keys;
	Store->Compare = Compare;
	Store->Insert = Insert;
	return (linear_index_open_t){Store, RADB_SUCCESS};
}

linear_index_t *linear_index_open(const char *Prefix, void *Keys, linear_compare_t Compare, linear_insert_t Insert RADB_MEM_PARAMS) {
	return linear_index_open2(Prefix, Keys, Compare, Insert RADB_MEM_ARGS).Index;
}

void linear_index_close(linear_index_t *Store) {
	msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->HeaderFd);
#if defined(RADB_MEM_MALLOC)
	free((void *)Store->Prefix);
	free(Store);
#elif defined(RADB_MEM_GC)
#else
	Store->free(Store->Allocator, (void *)Store->Prefix);
	Store->free(Store->Allocator, Store);
#endif
}

void linear_index_set_extra(linear_index_t *Store, uint32_t Value) {
	Store->Header->Extra = Value;
}

uint32_t linear_index_get_extra(linear_index_t *Store) {
	return Store->Header->Extra;
}

void *linear_index_keys(linear_index_t *Store) {
	return Store->Keys;
}

size_t linear_index_count(linear_index_t *Store) {
	return Store->Header->Count;
}

size_t linear_index_search(linear_index_t *Store, uint32_t Hash, linear_key_t Key, void *Full) {
	size_t NumOffset = Store->Header->NumOffsets;
	size_t Scale = NumOffset > 1 ? 1 << (64 - __builtin_clzl(NumOffset - 1)) : 1;
	size_t Index = Hash & (Scale - 1);
	if (Index >= NumOffset) Index -= (Scale >> 1);
	linear_node_t *Nodes = Store->Header->Nodes;
	size_t Offset = Nodes[Index].Offset;
	if (Offset == INVALID_INDEX) return INVALID_INDEX;
	linear_node_t *Last = Nodes + Store->Header->NumEntries;
	for (linear_node_t *Entry = Nodes + Offset; Entry < Last; ++Entry) {
		if (Entry->Index == INVALID_INDEX) {
			return INVALID_INDEX;
		} else if (Entry->Index != Index) {
			return INVALID_INDEX;
		} else if (Entry->Hash == Hash && !memcmp(Entry->Key, Key, sizeof(linear_key_t)) && !Store->Compare(Store->Keys, Full, Entry->Value)) {
			return Entry->Value;
		}
	}
	return INVALID_INDEX;
}

static linear_node_t *linear_index_grow_nodes(linear_index_t *Store, size_t Target) {
	if (Target > Store->Header->NumNodes) {
		size_t Required = Target - Store->Header->NumNodes;
		size_t Allocation = ((Required * sizeof(linear_node_t) + 511) / 512) * 512;
		size_t HeaderSize = Store->HeaderSize + Allocation;
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		Store->Header->NumNodes = (HeaderSize - sizeof(linear_header_t)) / sizeof(linear_node_t);
		Store->HeaderSize = HeaderSize;
	}
	return Store->Header->Nodes;
}

static void linear_index_add_offset(linear_index_t *Store) {
	size_t NumOffsets = Store->Header->NumOffsets;
	size_t Scale = 1 << (64 - __builtin_clzl(NumOffsets));
	size_t Shift = Scale >> 1;
	size_t Index = Scale > NumOffsets ? NumOffsets - Shift : NumOffsets & (Scale - 1);
	linear_node_t *Nodes = linear_index_grow_nodes(Store, NumOffsets + 1);
	size_t Offset = Nodes[Index].Offset;
	if (Offset == INVALID_INDEX) {
		Nodes[Store->Header->NumOffsets++].Offset = INVALID_INDEX;
		return;
	}
	linear_node_t *First = Nodes + Offset, *Last = First, *A = First;
	linear_node_t *Limit = Nodes + Store->Header->NumEntries;
	while (Last < Limit) {
		if (Last->Index != Index) break;
		++Last;
	}
	linear_node_t *B = Last;
	Store->Header->NumOffsets = ++NumOffsets;
	while (A < B) {
		size_t NewIndex = A->Hash & (Scale - 1);
		if (NewIndex >= NumOffsets) NewIndex -= Shift;
		if (NewIndex == Index) {
			++A;
		} else {
			--B;
			uint32_t TempHash = A->Hash;
			A->Hash = B->Hash;
			B->Hash = TempHash;
			uint32_t TempValue = A->Value;
			A->Value = B->Value;
			B->Value = TempValue;
			linear_key_t TempKey;
			memcpy(TempKey, A->Key, sizeof(linear_key_t));
			memcpy(A->Key, B->Key, sizeof(linear_key_t));
			memcpy(B->Key, TempKey, sizeof(linear_key_t));
			B->Index = NewIndex;
		}
	}
	if (B == Last) {
		Nodes[NumOffsets - 1].Offset = INVALID_INDEX;
	} else {
		if (B == First) Nodes[Index].Offset = INVALID_INDEX;
		Nodes[NumOffsets - 1].Offset = B - Nodes;
	}
}

static index_result_t linear_index_add_entry(linear_index_t *Store, uint32_t Index, uint32_t Hash, linear_key_t Key, void *Full) {
	linear_node_t *Nodes = linear_index_grow_nodes(Store, Store->Header->NumEntries + 1);
	size_t Offset = Store->Header->NumEntries++;
	linear_node_t *Entry = Nodes + Offset;
	Entry->Index = Index;
	Entry->Hash = Hash;
	memcpy(Entry->Key, Key, sizeof(linear_key_t));
	uint32_t Insert = Entry->Value = Store->Insert(Store->Keys, Full);
	linear_index_add_offset(Store);
	return (index_result_t){Insert, 1};
}

index_result_t linear_index_insert2(linear_index_t *Store, uint32_t Hash, linear_key_t Key, void *Full) {
	size_t NumOffsets = Store->Header->NumOffsets;
	size_t Scale = NumOffsets > 1 ? 1 << (64 - __builtin_clzl(NumOffsets - 1)) : 1;
	size_t Index = Hash & (Scale - 1);
	if (Index >= NumOffsets) Index -= (Scale >> 1);
	linear_node_t *Nodes = Store->Header->Nodes;
	size_t Offset = Nodes[Index].Offset;
	if (Offset == INVALID_INDEX) {
		++Store->Header->Count;
		size_t Free = Store->Header->NextFree;
		if (Free != INVALID_INDEX && Nodes[Free].Index == INVALID_INDEX) {
			Store->Header->NextFree = Nodes[Free].Value;
			Nodes[Index].Offset = Free;
			Nodes[Free].Index = Index;
			Nodes[Free].Hash = Hash;
			memcpy(Nodes[Free].Key, Key, sizeof(linear_key_t));
			uint32_t Insert = Nodes[Free].Value = Store->Insert(Store->Keys, Full);
			linear_index_add_offset(Store);
			return (index_result_t){Insert, 1};
		} else {
			Nodes[Index].Offset = Store->Header->NumEntries;
			return linear_index_add_entry(Store, Index, Hash, Key, Full);
		}
	}
	linear_node_t *Last = Nodes + Store->Header->NumEntries;
	for (linear_node_t *Entry = Nodes + Offset; Entry < Last; ++Entry) {
		if (Entry->Index == INVALID_INDEX) {
			++Store->Header->Count;
			Entry->Index = Index;
			Entry->Hash = Hash;
			memcpy(Entry->Key, Key, sizeof(linear_key_t));
			uint32_t Insert = Entry->Value = Store->Insert(Store->Keys, Full);
			linear_index_add_offset(Store);
			return (index_result_t){Insert, 1};
		} else if (Entry->Index != Index) {
			++Store->Header->Count;
			if (Offset > 0 && Nodes[Offset - 1].Index == INVALID_INDEX) {
				Nodes[Index].Offset = Offset - 1;
				linear_node_t *Entry2 = Nodes + (Offset - 1);
				Entry2->Index = Index;
				Entry2->Hash = Hash;
				memcpy(Entry2->Key, Key, sizeof(linear_key_t));
				uint32_t Insert = Entry2->Value = Store->Insert(Store->Keys, Full);
				linear_index_add_offset(Store);
				return (index_result_t){Insert, 1};
			} else {
				size_t Count = (Entry - Nodes) - Offset;
				Nodes = linear_index_grow_nodes(Store, Store->Header->NumEntries + Count + 1);
				Nodes[Index].Offset = Store->Header->NumEntries;
				linear_node_t *Source = Nodes + Offset;
				linear_node_t *Target = Nodes + Store->Header->NumEntries;
				Store->Header->NumEntries += (Count + 1);
				for (int I = 0; I < Count; ++I, ++Source, ++Target) {
					Target->Index = Index;
					Target->Hash = Source->Hash;
					memcpy(Target->Key, Source->Key, sizeof(linear_key_t));
					Target->Value = Source->Value;
					Source->Index = INVALID_INDEX;
				}
				Nodes[Offset].Value = Store->Header->NextFree;
				Store->Header->NextFree = Offset;
				Target->Index = Index;
				Target->Hash = Hash;
				memcpy(Target->Key, Key, sizeof(linear_key_t));
				uint32_t Insert = Target->Value = Store->Insert(Store->Keys, Full);
				linear_index_add_offset(Store);
				return (index_result_t){Insert, 1};
			}
		} else if (Entry->Hash == Hash && !memcmp(Entry->Key, Key, sizeof(linear_key_t)) && !Store->Compare(Store->Keys, Full, Entry->Value)) {
			return (index_result_t){Entry->Value, 0};
		}
	}
	++Store->Header->Count;
	return linear_index_add_entry(Store, Index, Hash, Key, Full);
}

size_t linear_index_insert(linear_index_t *Store, uint32_t Hash, linear_key_t Key, void *Full) {
	return linear_index_insert2(Store, Hash, Key, Full).Index;
}
