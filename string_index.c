#include "string_index.h"
#include "string_store.h"
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

typedef struct header_t {
	uint32_t Size;
	uint32_t Space;
	hash_t Hashes[];
} header_t;

struct string_index_t {
#ifdef RADB_MEM_PER_STORE
	void *(*alloc)(size_t);
	void *(*alloc_atomic)(size_t);
	void (*free)(void *);
#endif
	const char *Prefix;
	header_t *Header;
	string_store_t *Keys;
	size_t HeaderSize;
	int HeaderFd;
	int SyncCounter;
};

#ifdef RADB_MEM_PER_STORE
static inline const char *radb_strdup(const char *String, void *(*alloc_atomic)(size_t)) {
	size_t Length = strlen(String);
	char *Copy = alloc_atomic(Length + 1);
	strcpy(Copy, String);
	return Copy;
}
#endif

string_index_t *string_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS) {
#if defined(RADB_MEM_MALLOC)
	string_index_t *Store = malloc(sizeof(string_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	string_index_t *Store = GC_malloc(sizeof(string_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	string_index_t *Store = alloc(sizeof(string_index_t));
	Store->Prefix = radb_strdup(Prefix, alloc_atomic);
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
	Store->Header->Size = Store->Header->Space = 64;
	for (int I = 0; I < Store->Header->Size; ++I) Store->Header->Hashes[I].Link = INVALID_INDEX;
	Store->Keys = string_store_create(Prefix, KeySize, ChunkSize RADB_MEM_ARGS);
	//msync(Index->Header, Index->HeaderSize, MS_ASYNC);
	//msync(Index->Hashes, Index->Header->HashSize * sizeof(hash_t), MS_ASYNC);
	return Store;
}

string_index_t *string_index_open(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.index", Prefix);
	if (stat(FileName, Stat)) return NULL;
#if defined(RADB_MEM_MALLOC)
	string_index_t *Store = malloc(sizeof(string_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	string_index_t *Store = GC_malloc(sizeof(string_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	string_index_t *Store = alloc(sizeof(string_index_t));
	Store->Prefix = radb_strdup(Prefix, alloc_atomic);
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Keys = string_store_open(Prefix RADB_MEM_ARGS);
	return Store;
}

void string_index_close(string_index_t *Store) {
	//msync(Store->Strings, Store->Header->StringsSize, MS_SYNC);
	//msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_SYNC);
	//msync(Store->Header, Store->HeaderSize, MS_SYNC);
	string_store_close(Store->Keys);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->HeaderFd);
#if defined(RADB_MEM_MALLOC)
	free((void *)Store->Prefix);
	free(Store);
#elif defined(RADB_MEM_GC)
#else
	Store->free((void *)Store->Prefix);
	Store->free(Store);
#endif
}

inline uint32_t hash(const char *Key) {
	uint32_t Hash = 5381;
	unsigned char *P = (unsigned char *)(Key);
	while (P[0]) Hash = ((Hash << 5) + Hash) + P++[0];
	return Hash;
}

size_t string_index_count(string_index_t *Store) {
	return Store->Header->Size - Store->Header->Space;
}

size_t string_index_size(string_index_t *Store, size_t Index) {
	return string_store_size(Store->Keys, Index);
}

const char *string_index_get(string_index_t *Store, size_t Index) {
	size_t KeyLength = string_store_size(Store->Keys, Index);
#if defined(RADB_MEM_MALLOC)
	char *Key = malloc(KeyLength + 1);
#elif defined(RADB_MEM_GC)
	char *Key = GC_malloc_atomic(KeyLength + 1);
#else
	char *Key = Store->alloc_atomic(KeyLength + 1);
#endif
	string_store_get(Store->Keys, Index, Key);
	Key[KeyLength] = 0;
	return Key;
}

static void sort_hashes(string_index_t *Store, hash_t *First, hash_t *Last) {
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
				Cmp = string_store_compare2(Store->Keys, T.Link, P.Link);
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
} string_index_result_t;

string_index_result_t string_index_insert2(string_index_t *Store, const char *Key, size_t Length) {
	if (!Length) Length = strlen(Key);
	uint32_t Hash = hash(Key);
	unsigned int Mask = Store->Header->Size - 1;
	for (;;) {
		unsigned int Incr = ((Hash >> 8) | 1) & Mask;
		unsigned int Index = Hash & Mask;
		hash_t *Hashes = Store->Header->Hashes;
		for (;;) {
			if (Hashes[Index].Link == INVALID_INDEX) break;
			if (Hashes[Index].Hash < Hash) break;
			if (Hashes[Index].Hash == Hash) {
				int Cmp = string_store_compare(Store->Keys, Key, Length, Hashes[Index].Link);
				if (Cmp < 0) break;
				if (Cmp == 0) return (string_index_result_t){Hashes[Index].Link, 0};
			}
			Index += Incr;
			Index &= Mask;
		}
		size_t Space = Store->Header->Space;
		if (--Space > Store->Header->Size >> 3) {
			Store->Header->Space = Space;
			uint32_t Result = string_store_alloc(Store->Keys);
			string_store_set(Store->Keys, Result, Key, Length);
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
						return (string_index_result_t){Result, 1};
					} else if (Hashes[Index].Hash < Old.Hash) {
						hash_t New = Hashes[Index];
						Hashes[Index] = Old;
						Old = New;
						break;
					} else if (Hashes[Index].Hash == Old.Hash) {
						int Cmp = string_store_compare2(Store->Keys, Hashes[Index].Link, Old.Link);
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
			return (string_index_result_t){Result, 1};
		}
		size_t HashSize = Store->Header->Size * 2;
		Mask = HashSize - 1;

		char FileName2[strlen(Store->Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);

		size_t HeaderSize = sizeof(header_t) + HashSize * sizeof(hash_t);
		int HeaderFd = open(FileName2, O_RDWR | O_CREAT, 0777);
		ftruncate(HeaderFd, HeaderSize);
		header_t *Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, HeaderFd, 0);
		Header->Size = HashSize;
		Header->Space = Store->Header->Space + Store->Header->Size;
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

	return (string_index_result_t){INVALID_INDEX, 0};
}

size_t string_index_insert(string_index_t *Store, const char *Key, size_t Length) {
	return string_index_insert2(Store, Key, Length).Index;
}

size_t string_index_search(string_index_t *Store, const char *Key, size_t Length) {
	if (!Length) Length = strlen(Key);
	uint32_t Hash = hash(Key);
	unsigned int Mask = Store->Header->Size - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Header->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash) {
			int Cmp = string_store_compare(Store->Keys, Key, Length, Hashes[Index].Link);
			if (Cmp < 0) break;
			if (Cmp == 0) return Hashes[Index].Link;
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}

size_t string_index_delete(string_index_t *Store, const char *Key, size_t Length) {
	return 0;
}
