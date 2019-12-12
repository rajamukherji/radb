#include "fixed_store.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#ifndef NO_GC
#include <gc.h>
#define new(T) ((T *)GC_malloc(sizeof(T)))
#else
#define new(T) ((T *)malloc(sizeof(T)))
#endif

typedef struct header_t {
	uint32_t NodeSize, ChunkSize;
	uint32_t NumEntries, FreeEntry;
	char Nodes[];
} header_t;

struct fixed_store_t {
	const char *Prefix;
	header_t *Header;
	size_t HeaderSize;
	int HeaderFd;
};

fixed_store_t *fixed_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize) {
	uint32_t NodeSize = ((RequestedSize + 15) / 16) * 16;
	if (!ChunkSize) ChunkSize = 512;
	int NumEntries = (ChunkSize - sizeof(header_t) + NodeSize - 1) / NodeSize;
	fixed_store_t *Store = new(fixed_store_t);
#ifndef NO_GC
	Store->Prefix = strdup(Prefix);
#else
	Store->Prefix = GC_strdup(Prefix);
#endif
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

fixed_store_t *fixed_store_open(const char *Prefix) {
	fixed_store_t *Store = new(fixed_store_t);
#ifndef NO_GC
	Store->Prefix = strdup(Prefix);
#else
	Store->Prefix = GC_strdup(Prefix);
#endif
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return NULL;
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	return Store;
}

void fixed_store_close(fixed_store_t *Store) {
	//msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->HeaderFd);
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
