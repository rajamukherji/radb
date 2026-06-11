#include "fixed_store.h"
#include "fixed_index.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

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

#define FIXED_STORE_SIGNATURE 0x53464152
#define FIXED_STORE_VERSION MAKE_VERSION(1, 0)

typedef struct {
	uint32_t Signature, Version;
	uint32_t NodeSize, ChunkSize;
	uint32_t NumEntries, FreeEntry;
	char Nodes[];
} fixed_store_header_t;

struct fixed_store_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	fixed_store_header_t *Header;
	size_t HeaderSize;
	int HeaderFd;
};

static int lock_file(int Fd, short Type) {
	struct flock Lock = {0,};
	Lock.l_type = Type;
	if (fcntl(Fd, F_SETLK, &Lock) < 0) {
		fprintf(stderr, "Error locking file: %s", strerror(errno));
		return -1;
	} else {
		return 0;
	}
}

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
	uint32_t NodeSize;
	if (RequestedSize <= 4) {
		NodeSize = 4;
	} else {
		NodeSize = ((RequestedSize + 7) / 8) * 8;
	}
	if (!ChunkSize) ChunkSize = 512;
	int NumEntries = (ChunkSize - sizeof(fixed_store_header_t) + NodeSize - 1) / NodeSize;
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT | O_TRUNC, 0777);
	lock_file(Store->HeaderFd, F_WRLCK);
	Store->HeaderSize = sizeof(fixed_store_header_t) + NumEntries * NodeSize;
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = FIXED_STORE_SIGNATURE;
	Store->Header->Version = FIXED_STORE_VERSION;
	Store->Header->NodeSize = NodeSize;
	Store->Header->ChunkSize = (ChunkSize + NodeSize - 1) / NodeSize;
	Store->Header->NumEntries = NumEntries;
	Store->Header->FreeEntry = 0;
	*(uint32_t *)Store->Header->Nodes = INVALID_INDEX;
	//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	return Store;
}

fixed_store_open_t fixed_store_open2_rw(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return (fixed_store_open_t){NULL, RADB_FILE_NOT_FOUND};
	int HeaderFd = open(FileName, O_RDWR, 0777);
	if (lock_file(HeaderFd, F_WRLCK)) {
		close(HeaderFd);
		return (fixed_store_open_t){NULL, RADB_FILE_LOCKED};
	}
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
	Store->HeaderFd = HeaderFd;
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	if (Store->Header->Signature != FIXED_STORE_SIGNATURE) {
		fixed_store_close(Store);
		return (fixed_store_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	uint32_t NodeSize = Store->Header->NodeSize;
	if (!NodeSize) return (fixed_store_open_t){NULL, RADB_HEADER_MISMATCH};
	size_t ExpectedSize = sizeof(fixed_store_header_t) + Store->Header->NumEntries * NodeSize;
	if (ExpectedSize != Store->HeaderSize) {
		// The header was not written after the store size was increased, adjust accordingly.
		uint32_t NumEntries = (Store->HeaderSize - sizeof(fixed_store_header_t)) / NodeSize;
		void *End = Store->Header->Nodes + sizeof(fixed_store_header_t) + NumEntries * NodeSize;
		uint32_t FreeEntry = NumEntries;
		while (End > (void *)Store->Header->Nodes) {
			--FreeEntry;
			End -= NodeSize;
			if (*(uint32_t *)End == INVALID_INDEX) {
				Store->Header->FreeEntry = FreeEntry;
				Store->Header->NumEntries = NumEntries;
				break;
			} else if (*(uint32_t *)End) {
				fixed_store_close(Store);
				return (fixed_store_open_t){NULL, RADB_HEADER_CORRUPTED};
			}
		}
	}
	if (Store->Header->FreeEntry >= Store->Header->NumEntries) {
		fixed_store_close(Store);
		return (fixed_store_open_t){NULL, RADB_HEADER_CORRUPTED};
	}
	return (fixed_store_open_t){Store, RADB_SUCCESS};
}

fixed_store_open_t fixed_store_open2_ro(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return (fixed_store_open_t){NULL, RADB_FILE_NOT_FOUND};
	int HeaderFd = open(FileName, O_RDONLY, 0777);
	if (lock_file(HeaderFd, F_RDLCK)) {
		close(HeaderFd);
		return (fixed_store_open_t){NULL, RADB_FILE_LOCKED};
	}
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
	Store->HeaderFd = HeaderFd;
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ, MAP_SHARED, Store->HeaderFd, 0);
	if (Store->Header->Signature != FIXED_STORE_SIGNATURE) {
		fixed_store_close(Store);
		return (fixed_store_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	uint32_t NodeSize = Store->Header->NodeSize;
	if (!NodeSize) return (fixed_store_open_t){NULL, RADB_HEADER_MISMATCH};
	size_t ExpectedSize = sizeof(fixed_store_header_t) + Store->Header->NumEntries * NodeSize;
	if (ExpectedSize != Store->HeaderSize) {
		// The header was not written after the store size was increased, adjust accordingly.
		uint32_t NumEntries = (Store->HeaderSize - sizeof(fixed_store_header_t)) / NodeSize;
		void *End = Store->Header->Nodes + sizeof(fixed_store_header_t) + NumEntries * NodeSize;
		uint32_t FreeEntry = NumEntries;
		while (End > (void *)Store->Header->Nodes) {
			--FreeEntry;
			End -= NodeSize;
			if (*(uint32_t *)End == INVALID_INDEX) {
				Store->Header->FreeEntry = FreeEntry;
				Store->Header->NumEntries = NumEntries;
				break;
			} else if (*(uint32_t *)End) {
				fixed_store_close(Store);
				return (fixed_store_open_t){NULL, RADB_HEADER_CORRUPTED};
			}
		}
	}
	if (Store->Header->FreeEntry >= Store->Header->NumEntries) {
		fixed_store_close(Store);
		return (fixed_store_open_t){NULL, RADB_HEADER_CORRUPTED};
	}
	return (fixed_store_open_t){Store, RADB_SUCCESS};
}

fixed_store_open_t fixed_store_open2(const char *Prefix, int Readonly RADB_MEM_PARAMS) {
	if (Readonly) {
		return fixed_store_open2_ro(Prefix RADB_MEM_ARGS);
	} else {
		return fixed_store_open2_rw(Prefix RADB_MEM_ARGS);
	}
}

fixed_store_t *fixed_store_open(const char *Prefix, int Readonly RADB_MEM_PARAMS) {
	return fixed_store_open2(Prefix, Readonly RADB_MEM_ARGS).Store;
}

void fixed_store_close(fixed_store_t *Store) {
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

size_t fixed_store_num_entries(fixed_store_t *Store) {
	return Store->Header->NumEntries;
}

size_t fixed_store_node_size(fixed_store_t *Store) {
	return Store->Header->NodeSize;
}

void *fixed_store_get_unchecked(fixed_store_t *Store, size_t Index) {
	return Store->Header->Nodes + Index * Store->Header->NodeSize;
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

void fixed_store_shift(fixed_store_t *Store, size_t Source, size_t Count, size_t Destination) {
	size_t Index = (Source > Destination ? Source : Destination) + Count;
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
	size_t LargeSource, LargeDest, LargeCount;
	size_t SmallSource, SmallDest, SmallCount;
	if (Source < Destination) {
		if (Source + Count > Destination) {
			LargeSource = Source;
			LargeDest = Destination;
			LargeCount = Count;
			SmallSource = Source + Count;
			SmallDest = Source;
			SmallCount = Destination - Source;
		} else {
			LargeSource = Source + Count;
			LargeDest = Source;
			LargeCount = Destination - Source;
			SmallSource = Source;
			SmallDest = Destination;
			SmallCount = Count;
		}
	} else if (Source > Destination) {
		if (Destination + Count > Source) {
			LargeSource = Source;
			LargeDest = Destination;
			LargeCount = Count;
			SmallSource = Destination;
			SmallDest = Destination + Count;
			SmallCount = Source - Destination;
		} else {
			LargeSource = Destination;
			LargeDest = Destination + Count;
			LargeCount = Source - Destination;
			SmallSource = Source;
			SmallDest = Destination;
			SmallCount = Count;
		}
	} else {
		return;
	}
	size_t NodeSize = Store->Header->NodeSize;
	LargeSource *= NodeSize;
	LargeDest *= NodeSize;
	LargeCount *= NodeSize;
	SmallSource *= NodeSize;
	SmallDest *= NodeSize;
	SmallCount *= NodeSize;
	char *Nodes = Store->Header->Nodes;
	if (SmallCount <= 256) {
		char *SmallSaved = alloca(SmallCount);
		memcpy(SmallSaved, Nodes + SmallSource, SmallCount);
		memmove(Nodes + LargeDest, Nodes + LargeSource, LargeCount);
		memcpy(Nodes + SmallDest, SmallSaved, SmallCount);
	} else {
		char *SmallSaved = malloc(SmallCount);
		memcpy(SmallSaved, Nodes + SmallSource, SmallCount);
		memmove(Nodes + LargeDest, Nodes + LargeSource, LargeCount);
		memcpy(Nodes + SmallDest, SmallSaved, SmallCount);
		free(SmallSaved);
	}
}

fixed_store_alloc_t fixed_store_alloc2(fixed_store_t *Store) {
	size_t FreeEntry = Store->Header->FreeEntry;
	void *Value = Store->Header->Nodes + FreeEntry * Store->Header->NodeSize;
	size_t Next = *(uint32_t *)Value;
	if (Next == INVALID_INDEX) {
		Next = FreeEntry + 1;
		if (Next >= Store->Header->NumEntries) {
			size_t NumEntries = (Next + 1) - Store->Header->NumEntries;
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
			Value = Store->Header->Nodes + FreeEntry * Store->Header->NodeSize;
		}
		*(uint32_t *)(Store->Header->Nodes + Next * Store->Header->NodeSize) = INVALID_INDEX;
	}
	Store->Header->FreeEntry = Next;
	return (fixed_store_alloc_t){Value, FreeEntry};
}

size_t fixed_store_alloc(fixed_store_t *Store) {
	return fixed_store_alloc2(Store).Index;
}

void fixed_store_free(fixed_store_t *Store, size_t Index) {
	*(uint32_t *)(Store->Header->Nodes + Index * Store->Header->NodeSize) =  Store->Header->FreeEntry;
	Store->Header->FreeEntry = Index;
}
