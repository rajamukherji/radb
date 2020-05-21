#include "fixed_store.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef RADB_MEM_GC
#include <gc/gc.h>
#endif

typedef struct header_t {
	uint32_t NodeSize, ChunkSize;
	uint32_t NumEntries, FreeEntry;
	char Nodes[];
} header_t;

struct fixed_store_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	header_t *Header;
	size_t HeaderSize;
	int HeaderFd;
};

#ifdef RADB_MEM_PER_STORE
static inline const char *radb_strdup(const char *String, void *Allocator, void *(*alloc_atomic)(void *, size_t)) {
	size_t Length = strlen(String);
	char *Copy = alloc_atomic(Allocator, Length + 1);
	strcpy(Copy, String);
	return Copy;
}
#endif

fixed_store_t *fixed_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS) {
#if defined(RADB_MEM_MALLOC)
	fixed_store_t *Store = malloc(sizeof(fixed_store_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	fixed_store_t *Store = GC_malloc(sizeof(fixed_store_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	fixed_store_t *Store = alloc(Allocator, sizeof(fixed_store_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	uint32_t NodeSize = ((RequestedSize + 7) / 8) * 8;
	if (!ChunkSize) ChunkSize = 512;
	int NumEntries = (ChunkSize - sizeof(header_t) + NodeSize - 1) / NodeSize;
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Store->HeaderSize = sizeof(header_t) + NumEntries * NodeSize;
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->NodeSize = NodeSize;
	Store->Header->ChunkSize = (ChunkSize + NodeSize - 1) / NodeSize;
	Store->Header->NumEntries = NumEntries;
	Store->Header->FreeEntry = 0;
	*(uint32_t *)Store->Header->Nodes = INVALID_INDEX;
	//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	return Store;
}

fixed_store_t *fixed_store_open(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return NULL;
#if defined(RADB_MEM_MALLOC)
	fixed_store_t *Store = malloc(sizeof(fixed_store_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	fixed_store_t *Store = GC_malloc(sizeof(fixed_store_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	fixed_store_t *Store = alloc(Allocator, sizeof(fixed_store_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	return Store;
}

void fixed_store_close(fixed_store_t *Store) {
	//msync(Store->Header, Store->HeaderSize, MS_SYNC);
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

size_t fixed_store_num_entries(fixed_store_t *Store) {
	return Store->Header->NumEntries;
}

void *fixed_store_get(fixed_store_t *Store, size_t Index) {
	if (Index >= Store->Header->NumEntries) {
		size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
		NumEntries += Store->Header->ChunkSize - 1;
		NumEntries /= Store->Header->ChunkSize;
		NumEntries *= Store->Header->ChunkSize;
		size_t HeaderSize = Store->HeaderSize + NumEntries * Store->Header->NodeSize;
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		Store->Header->NumEntries += NumEntries;
		Store->HeaderSize = HeaderSize;
	}
	return Store->Header->Nodes + Index * Store->Header->NodeSize;
}

size_t fixed_store_alloc(fixed_store_t *Store) {
	size_t FreeEntry = Store->Header->FreeEntry;
	size_t Index = *(uint32_t *)(Store->Header->Nodes + FreeEntry * Store->Header->NodeSize);
	if (Index == INVALID_INDEX) {
		Index = FreeEntry + 1;
		if (Index >= Store->Header->NumEntries) {
			size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
			NumEntries += Store->Header->ChunkSize - 1;
			NumEntries /= Store->Header->ChunkSize;
			NumEntries *= Store->Header->ChunkSize;
			size_t HeaderSize = Store->HeaderSize + NumEntries * Store->Header->NodeSize;
			ftruncate(Store->HeaderFd, HeaderSize);
	#ifdef Linux
			Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
	#else
			munmap(Store->Header, Store->HeaderSize);
			Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	#endif
			Store->Header->NumEntries += NumEntries;
			Store->HeaderSize = HeaderSize;
		}
		*(uint32_t *)(Store->Header->Nodes + Index * Store->Header->NodeSize) = INVALID_INDEX;
	}
	Store->Header->FreeEntry = Index;
	return FreeEntry;
}

void fixed_store_free(fixed_store_t *Store, size_t Index) {
	*(uint32_t *)(Store->Header->Nodes + Index * Store->Header->NodeSize) =  Store->Header->FreeEntry;
	Store->Header->FreeEntry = Index;
}
