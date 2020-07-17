#include "fixed_index.h"
#include "fixed_store.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#ifdef RADB_MEM_GC
#include <gc/gc.h>
#endif

typedef struct hash_t {
	uint32_t Hash;
	uint32_t Link;
} hash_t;

#define MAKE_VERSION(MAJOR, MINOR) (0xFF000000 + (MAJOR << 16) + (MINOR << 8))

#define SIGNATURE 0x49464152
#define VERSION MAKE_VERSION(1, 0)

typedef struct header_t {
	uint32_t Signature, Version;
	uint32_t Size, Space;
	uint32_t KeySize, Reserved1;
	hash_t Hashes[];
} header_t;

struct fixed_index_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	header_t *Header;
	fixed_store_t *Keys;
	size_t HeaderSize;
	int HeaderFd;
	int SyncCounter;
};

#ifdef RADB_MEM_PER_STORE
static inline const char *radb_strdup(const char *String, void *Allocator, void *(*alloc_atomic)(void *, size_t)) {
	size_t Length = strlen(String);
	char *Copy = alloc_atomic(Allocator, Length + 1);
	strcpy(Copy, String);
	return Copy;
}
#endif

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
	Store->HeaderSize = sizeof(header_t) + 64 * sizeof(hash_t);
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = SIGNATURE;
	Store->Header->Version = VERSION;
	Store->Header->Size = Store->Header->Space = 64;
	Store->Header->KeySize = KeySize;
	for (int I = 0; I < Store->Header->Size; ++I) Store->Header->Hashes[I].Link = INVALID_INDEX;
	Store->Keys = fixed_store_create(Prefix, KeySize, ChunkSize RADB_MEM_ARGS);
	//msync(Index->Header, Index->HeaderSize, MS_ASYNC);
	//msync(Index->Hashes, Index->Header->HashSize * sizeof(hash_t), MS_ASYNC);
	return Store;
}

fixed_index_t *fixed_index_open(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.index", Prefix);
	if (stat(FileName, Stat)) return NULL;
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
	if (Store->Header->Signature != SIGNATURE) {
		puts("Header mismatch - aborting");
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		return NULL;
	}
	Store->Keys = fixed_store_open(Prefix RADB_MEM_ARGS);
	return Store;
}

void fixed_index_close(fixed_index_t *Store) {
	//msync(Store->Strings, Store->Header->StringsSize, MS_SYNC);
	//msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_SYNC);
	//msync(Store->Header, Store->HeaderSize, MS_SYNC);
	fixed_store_close(Store->Keys);
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

size_t fixed_index_count(fixed_index_t *Store) {
	return Store->Header->Size - Store->Header->Space;
}

const void *fixed_index_get(fixed_index_t *Store, size_t Index) {
	return fixed_store_get(Store->Keys, Index);
}

static void sort_hashes(fixed_index_t *Store, hash_t *First, hash_t *Last) {
	hash_t *A = First;
	hash_t *B = Last;
	hash_t T = *A;
	hash_t P = *B;
	while (P.Link == INVALID_INDEX) {
		--B;
		--Last;
		if (A == B) return;
		P = *B;
	}
	while (A != B) {
		int Cmp;
		if (T.Link != INVALID_INDEX) {
			if (T.Hash < P.Hash) {
				Cmp = -1;
			} else if (T.Hash > P.Hash) {
				Cmp = 1;
			} else {
				const void *TKey = fixed_store_get(Store->Keys, T.Link);
				const void *PKey = fixed_store_get(Store->Keys, P.Link);
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

typedef struct {
	size_t Index;
	int Created;
} fixed_index_result_t;

fixed_index_result_t fixed_index_insert2(fixed_index_t *Store, const char *Key) {
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
				const void *HKey = fixed_store_get(Store->Keys, Hashes[Index].Link);
				int Cmp = memcmp(Key, HKey, Store->Header->KeySize);
				if (Cmp < 0) break;
				if (Cmp == 0) return (fixed_index_result_t){Hashes[Index].Link, 0};
			}
			Index += Incr;
			Index &= Mask;
		}
		size_t Space = Store->Header->Space;
		if (--Space > Store->Header->Size >> 3) {
			Store->Header->Space = Space;
			uint32_t Result = fixed_store_alloc(Store->Keys);
			memcpy(fixed_store_get(Store->Keys, Result), Key, Store->Header->KeySize);
			hash_t Old = Hashes[Index];
			Hashes[Index].Link = Result;
			Hashes[Index].Hash = Hash;
			while (Old.Link != INVALID_INDEX) {
				Incr = ((Old.Hash >> 8) | 1) & Mask;
				for (;;) {
					Index += Incr;
					Index &= Mask;
					if (Hashes[Index].Link == INVALID_INDEX) {
						Hashes[Index] = Old;
						//msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_ASYNC);
						return (fixed_index_result_t){Result, 1};
					} else if (Hashes[Index].Hash < Old.Hash) {
						hash_t New = Hashes[Index];
						Hashes[Index] = Old;
						Old = New;
						break;
					} else if (Hashes[Index].Hash == Old.Hash) {
						const void *HKey = fixed_store_get(Store->Keys, Hashes[Index].Link);
						const void *OKey = fixed_store_get(Store->Keys, Old.Link);
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
			return (fixed_index_result_t){Result, 1};
		}
		size_t HashSize = Store->Header->Size * 2;
		Mask = HashSize - 1;

		char FileName2[strlen(Store->Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);

		size_t HeaderSize = sizeof(header_t) + HashSize * sizeof(hash_t);
		int HeaderFd = open(FileName2, O_RDWR | O_CREAT, 0777);
		ftruncate(HeaderFd, HeaderSize);
		header_t *Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, HeaderFd, 0);
		Header->Signature = SIGNATURE;
		Header->Version = VERSION;
		Header->Size = HashSize;
		Header->Space = Store->Header->Space + Store->Header->Size;
		Header->KeySize = Store->Header->KeySize;
		for (int I = 0; I < HashSize; ++I) Header->Hashes[I].Link = INVALID_INDEX;

		sort_hashes(Store, Hashes, Hashes + Store->Header->Size - 1);
		for (hash_t *Old = Hashes; Old->Link != INVALID_INDEX; ++Old) {
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

	return (fixed_index_result_t){INVALID_INDEX, 0};
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
		if (Hashes[Index].Hash == Hash) {
			const void *HKey = fixed_store_get(Store->Keys, Hashes[Index].Link);
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
	return 0;
}
