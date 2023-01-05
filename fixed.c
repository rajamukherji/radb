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
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
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

fixed_store_open_t fixed_store_open2(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return (fixed_store_open_t){NULL, RADB_FILE_NOT_FOUND};
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
	if (Store->Header->Signature != FIXED_STORE_SIGNATURE) {
		fixed_store_close(Store);
		return (fixed_store_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	uint32_t NodeSize = Store->Header->NodeSize;
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
	return (fixed_store_open_t){Store, RADB_SUCCESS};
}

fixed_store_t *fixed_store_open(const char *Prefix RADB_MEM_PARAMS) {
	return fixed_store_open2(Prefix RADB_MEM_ARGS).Store;
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

static void *fixed_store_get_unchecked(fixed_store_t *Store, size_t Index) {
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

#define FIXED_INDEX_SIGNATURE 0x49464152
#define FIXED_INDEX_VERSION MAKE_VERSION(1, 0)

typedef struct {
	uint32_t Hash;
	uint32_t Link;
} hash_t;

typedef struct {
	uint32_t Signature, Version;
	uint32_t Size, Space;
	uint32_t KeySize, Deleted;
	hash_t Hashes[];
} fixed_index_header_t;

struct fixed_index_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	fixed_index_header_t *Header;
	fixed_store_t *Keys;
	size_t HeaderSize;
	int HeaderFd;
	int SyncCounter;
};

fixed_index_t *fixed_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS) {
#if defined(RADB_MEM_MALLOC)
	fixed_index_t *Store = malloc(sizeof(fixed_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	fixed_index_t *Store = GC_malloc(sizeof(fixed_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	fixed_index_t *Store = alloc(Allocator, sizeof(fixed_index_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	if (!ChunkSize) ChunkSize = 512;
	char FileName[strlen(Prefix) + 10];
	Store->SyncCounter = 32;
	sprintf(FileName, "%s.index", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Store->HeaderSize = sizeof(fixed_index_header_t) + 64 * sizeof(hash_t);
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = FIXED_INDEX_SIGNATURE;
	Store->Header->Version = FIXED_INDEX_VERSION;
	Store->Header->Size = Store->Header->Space = 64;
	Store->Header->Deleted = 0;
	Store->Header->KeySize = KeySize;
	for (int I = 0; I < Store->Header->Size; ++I) Store->Header->Hashes[I].Link = INVALID_INDEX;
	Store->Keys = fixed_store_create(Prefix, KeySize, ChunkSize RADB_MEM_ARGS);
	//msync(Index->Header, Index->HeaderSize, MS_ASYNC);
	//msync(Index->Hashes, Index->Header->HashSize * sizeof(hash_t), MS_ASYNC);
	return Store;
}

fixed_index_open_t fixed_index_open2(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.index", Prefix);
	if (stat(FileName, Stat)) return (fixed_index_open_t){NULL, RADB_FILE_NOT_FOUND};
	fixed_store_open_t KeysOpen = fixed_store_open2(Prefix RADB_MEM_ARGS);
	if (!KeysOpen.Store) return (fixed_index_open_t){NULL, KeysOpen.Error + 3};
#if defined(RADB_MEM_MALLOC)
	fixed_index_t *Store = malloc(sizeof(fixed_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	fixed_index_t *Store = GC_malloc(sizeof(fixed_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	fixed_index_t *Store = alloc(Allocator, sizeof(fixed_index_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	if (Store->Header->Signature != FIXED_INDEX_SIGNATURE) {
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		fixed_store_close(KeysOpen.Store);
		return (fixed_index_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	Store->Keys = KeysOpen.Store;
	return (fixed_index_open_t){Store, RADB_SUCCESS};
}

fixed_index_t *fixed_index_open(const char *Prefix RADB_MEM_PARAMS) {
	return fixed_index_open2(Prefix RADB_MEM_ARGS).Index;
}

void fixed_index_close(fixed_index_t *Store) {
	fixed_store_close(Store->Keys);
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

static uint32_t hash(const char *Key, int Length) {
	uint32_t Hash = 5381;
	unsigned char *P = (unsigned char *)(Key);
	for (int I = Length; --I >= 0;) Hash = ((Hash << 5) + Hash) + P++[0];
	return Hash;
}

size_t fixed_index_num_entries(fixed_index_t *Store) {
	return Store->Header->Size - (Store->Header->Space + Store->Header->Deleted);
}

size_t fixed_index_num_deleted(fixed_index_t *Store) {
	return Store->Header->Deleted;
}

const void *fixed_index_get(fixed_index_t *Store, size_t Index) {
	return fixed_store_get(Store->Keys, Index);
}

static void sort_hashes(fixed_index_t *Store, hash_t *First, hash_t *Last) {
	hash_t *A = First;
	hash_t *B = Last;
	hash_t T = *A;
	hash_t P = *B;
	while (P.Link >= DELETED_INDEX) {
		--B;
		--Last;
		if (A == B) return;
		P = *B;
	}
	while (A != B) {
		int Cmp;
		if (T.Link < DELETED_INDEX) {
			if (T.Hash < P.Hash) {
				Cmp = -1;
			} else if (T.Hash > P.Hash) {
				Cmp = 1;
			} else {
				const void *TKey = fixed_store_get_unchecked(Store->Keys, T.Link);
				const void *PKey = fixed_store_get_unchecked(Store->Keys, P.Link);
				Cmp = memcmp(TKey, PKey, Store->Header->KeySize);
			}
		} else {
			Cmp = -1;
		}
		if (Cmp > 0) {
			*A = T;
			T = *++A;
		} else {
			*B = T;
			T = *--B;
		}
	}
	*A = P;
	if (First < A - 1) sort_hashes(Store, First, A - 1);
	if (A + 1 < Last) sort_hashes(Store, A + 1, Last);
}

index_result_t fixed_index_insert2(fixed_index_t *Store, const char *Key) {
	uint32_t Hash = hash(Key, Store->Header->KeySize);
	unsigned int Mask = Store->Header->Size - 1;
	for (;;) {
		unsigned int Incr = ((Hash >> 8) | 1) & Mask;
		unsigned int Index = Hash & Mask;
		hash_t *Hashes = Store->Header->Hashes;
		for (;;) {
			if (Hashes[Index].Link == INVALID_INDEX) break;
			if (Hashes[Index].Hash < Hash) break;
			if (Hashes[Index].Hash == Hash) {
				const void *HKey = fixed_store_get_unchecked(Store->Keys, Hashes[Index].Link);
				int Cmp = memcmp(Key, HKey, Store->Header->KeySize);
				if (Cmp < 0) break;
				if (Cmp == 0) return (index_result_t){Hashes[Index].Link, 0};
			}
			Index += Incr;
			Index &= Mask;
		}
		size_t Space = Store->Header->Space;
		if (--Space > Store->Header->Size >> 3) {
			Store->Header->Space = Space;
			uint32_t Link = fixed_store_alloc(Store->Keys);
			memcpy(fixed_store_get_unchecked(Store->Keys, Link), Key, Store->Header->KeySize);
			hash_t Old = Hashes[Index];
			Hashes[Index].Link = Link;
			Hashes[Index].Hash = Hash;
			while (Old.Link != INVALID_INDEX) {
				Incr = ((Old.Hash >> 8) | 1) & Mask;
				for (;;) {
					Index += Incr;
					Index &= Mask;
					if (Hashes[Index].Link == INVALID_INDEX) {
						Hashes[Index] = Old;
						//msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_ASYNC);
						return (index_result_t){Link, 1};
					} else if (Hashes[Index].Hash < Old.Hash) {
						hash_t New = Hashes[Index];
						Hashes[Index] = Old;
						Old = New;
						break;
					} else if (Hashes[Index].Hash == Old.Hash) {
						const void *HKey = fixed_store_get_unchecked(Store->Keys, Hashes[Index].Link);
						const void *OKey = fixed_store_get_unchecked(Store->Keys, Old.Link);
						int Cmp = memcmp(HKey, OKey, Store->Header->KeySize);
						if (Cmp < 0) {
							hash_t New = Hashes[Index];
							Hashes[Index] = Old;
							Old = New;
							break;
						}
					}
				}
			}
			//msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_ASYNC);
			return (index_result_t){Link, 1};
		}
		size_t HashSize = Store->Header->Size * 2;
		if (Space + Store->Header->Deleted > Store->Header->Size >> 3) HashSize = Store->Header->Size;
		Mask = HashSize - 1;

		char FileName2[strlen(Store->Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);

		size_t HeaderSize = sizeof(fixed_index_header_t) + HashSize * sizeof(hash_t);
		int HeaderFd = open(FileName2, O_RDWR | O_CREAT, 0777);
		ftruncate(HeaderFd, HeaderSize);
		fixed_index_header_t *Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, HeaderFd, 0);
		Header->Signature = FIXED_INDEX_SIGNATURE;
		Header->Version = FIXED_INDEX_VERSION;
		Header->Size = HashSize;
		Header->Space = Store->Header->Space + Store->Header->Deleted + (HashSize - Store->Header->Size);
		Header->Deleted = 0;
		Header->KeySize = Store->Header->KeySize;
		for (int I = 0; I < HashSize; ++I) Header->Hashes[I].Link = INVALID_INDEX;

		sort_hashes(Store, Hashes, Hashes + Store->Header->Size - 1);
		for (hash_t *Old = Hashes; Old->Link < DELETED_INDEX; ++Old) {
			unsigned long NewHash = Old->Hash;
			unsigned int NewIncr = ((NewHash >> 8) | 1) & Mask;
			unsigned int NewIndex = NewHash & Mask;
			while (Header->Hashes[NewIndex].Link != INVALID_INDEX) {
				NewIndex += NewIncr;
				NewIndex &= Mask;
			}
			Header->Hashes[NewIndex] = Old[0];
		}

		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);

		char FileName[strlen(Store->Prefix) + 10];
		sprintf(FileName, "%s.index", Store->Prefix);
		rename(FileName2, FileName);

		Store->HeaderSize = HeaderSize;
		Store->Header = Header;
		Store->HeaderFd = HeaderFd;

		//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	}

	return (index_result_t){INVALID_INDEX, 0};
}

size_t fixed_index_insert(fixed_index_t *Store, const char *Key) {
	return fixed_index_insert2(Store, Key).Index;
}

size_t fixed_index_search(fixed_index_t *Store, const char *Key) {
	uint32_t Hash = hash(Key, Store->Header->KeySize);
	unsigned int Mask = Store->Header->Size - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Header->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash && Hashes[Index].Link != DELETED_INDEX) {
			const void *HKey = fixed_store_get_unchecked(Store->Keys, Hashes[Index].Link);
			int Cmp = memcmp(Key, HKey, Store->Header->KeySize);
			if (Cmp < 0) break;
			if (Cmp == 0) return Hashes[Index].Link;
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}

size_t fixed_index_delete(fixed_index_t *Store, const char *Key) {
	uint32_t Hash = hash(Key, Store->Header->KeySize);
	unsigned int Mask = Store->Header->Size - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Header->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash && Hashes[Index].Link != DELETED_INDEX) {
			const void *HKey = fixed_store_get_unchecked(Store->Keys, Hashes[Index].Link);
			int Cmp = memcmp(Key, HKey, Store->Header->KeySize);
			if (Cmp < 0) break;
			if (Cmp == 0) {
				uint32_t Link = Hashes[Index].Link;
				fixed_store_free(Store->Keys, Link);
				Hashes[Index].Link = DELETED_INDEX;
				++Store->Header->Deleted;
				return Link;
			}
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}

uint32_t fixed_index_key_size(fixed_index_t *Store) {
	return Store->Header->KeySize;
}

int fixed_index_foreach(fixed_index_t *Store, void *Data, fixed_index_foreach_fn Callback) {
	hash_t *Hash = Store->Header->Hashes;
	hash_t *Limit = Hash + Store->Header->Size;
	while (Hash < Limit) {
		if (Hash->Link != INVALID_INDEX) if (Callback(Hash->Link, Data)) return 1;
		++Hash;
	}
	return 0;
}
