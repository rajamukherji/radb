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
#ifndef NO_GC
#include <gc.h>
#define new(T) ((T *)GC_malloc(sizeof(T)))
#else
#define new(T) ((T *)malloc(sizeof(T)))
#endif

typedef struct hash_t {
	uint32_t Hash;
	uint32_t Link;
} hash_t;

struct string_index_t {
	const char *Prefix;
	hash_t *Hashes;
	string_store_t *Keys;
	uint32_t HashSize;
	int HashesFd;
	int SyncCounter;
};

extern uint32_t string_store_get_extra(string_store_t *Store);
extern void string_store_set_extra(string_store_t *Store, uint32_t Value);

string_index_t *string_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize) {
	if (!ChunkSize) ChunkSize = 512;
	string_index_t *Store = new(string_index_t);
#ifndef NO_GC
	Store->Prefix = strdup(Prefix);
#else
	Store->Prefix = GC_strdup(Prefix);
#endif
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.keys", Prefix);
	Store->HashSize = 64;
	Store->SyncCounter = 32;
	sprintf(FileName, "%s.hashes", Prefix);
	Store->HashesFd = open(FileName, O_RDWR | O_CREAT, 0777);
	ftruncate(Store->HashesFd, Store->HashSize * sizeof(hash_t));
	Store->Hashes = mmap(NULL, Store->HashSize * sizeof(hash_t), PROT_READ | PROT_WRITE, MAP_SHARED, Store->HashesFd, 0);
	for (int I = 0; I < Store->HashSize; ++I) Store->Hashes[I].Link = INVALID_INDEX;
	Store->Keys = string_store_create(Prefix, KeySize, ChunkSize);
	string_store_set_extra(Store->Keys, Store->HashSize);
	//msync(Index->Header, Index->HeaderSize, MS_ASYNC);
	//msync(Index->Hashes, Index->Header->HashSize * sizeof(hash_t), MS_ASYNC);
	return Store;
}

string_index_t *string_index_open(const char *Prefix) {
	string_index_t *Store = new(string_index_t);
#ifndef NO_GC
	Store->Prefix = strdup(Prefix);
#else
	Store->Prefix = GC_strdup(Prefix);
#endif
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	Store->Keys = string_store_open(Prefix);
	Store->HashSize = string_store_get_extra(Store->Keys);
	sprintf(FileName, "%s.hashes", Prefix);
	Store->HashesFd = open(FileName, O_RDWR, 0777);
	Store->Hashes = mmap(NULL, Store->HashSize * sizeof(hash_t), PROT_READ | PROT_WRITE, MAP_SHARED, Store->HashesFd, 0);
	return Store;
}

void string_index_close(string_index_t *Store) {
	//msync(Store->Strings, Store->Header->StringsSize, MS_SYNC);
	//msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_SYNC);
	//msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Hashes, Store->HashSize * sizeof(hash_t));
	close(Store->HashesFd);
	string_store_close(Store->Keys);
}

#define hash(KEY) ({ \
	uint32_t Hash = 5381; \
	unsigned char *P = (unsigned char *)(KEY); \
	while (P[0]) Hash = ((Hash << 5) + Hash) + P++[0]; \
	Hash; \
})

size_t string_index_count(string_index_t *Store) {
	return string_store_num_entries(Store->Keys);
}

const char *string_index_get(string_index_t *Store, size_t Index) {
	size_t KeyLength = string_store_get_size(Store->Keys, Index);
#ifndef NO_GC
	char *Key = malloc(KeyLength + 1);
#else
	char *Key = GC_malloc_atomic(KeyLength + 1);
#endif
	string_store_get_value(Store->Keys, Index, Key);
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

size_t string_index_insert(string_index_t *Store, const char *Key) {
	size_t Length = strlen(Key);
	uint32_t Hash = hash(Key);
	unsigned int Mask = Store->HashSize - 1;
	for (;;) {
		unsigned int Incr = ((Hash >> 8) | 1) & Mask;
		unsigned int Index = Hash & Mask;
		hash_t *Hashes = Store->Hashes;
		for (;;) {
			if (Hashes[Index].Link == INVALID_INDEX) break;
			if (Hashes[Index].Hash < Hash) break;
			if (Hashes[Index].Hash == Hash) {
				int Cmp = string_store_compare(Store->Keys, Hashes[Index].Link, Key, Length);
				if (Cmp < 0) break;
				if (Cmp == 0) return Hashes[Index].Link;
			}
			Index += Incr;
			Index &= Mask;
		}
		size_t Space = Store->HashSize - string_store_num_entries(Store->Keys);
		if (--Space > Store->HashSize >> 3) {
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
						return Result;
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
			return Result;
		}
		size_t NewSize = Store->HashSize * 2;
		Mask = NewSize - 1;

		char FileName2[strlen(Store->Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);

		size_t HashMapSize = NewSize * sizeof(hash_t);
		int HashesFd = open(FileName2, O_RDWR | O_CREAT, 0777);
		ftruncate(HashesFd, HashMapSize);
		hash_t *NewHashes = mmap(NULL, HashMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, HashesFd, 0);
		for (int I = 0; I < NewSize; ++I) NewHashes[I].Link = INVALID_INDEX;

		sort_hashes(Store, Hashes, Hashes + Store->HashSize - 1);
		for (hash_t *Old = Hashes; Old->Link != INVALID_INDEX; ++Old) {
			unsigned long NewHash = Old->Hash;
			unsigned int NewIncr = ((NewHash >> 8) | 1) & Mask;
			unsigned int NewIndex = NewHash & Mask;
			while (NewHashes[NewIndex].Link != INVALID_INDEX) {
				NewIndex += NewIncr;
				NewIndex &= Mask;
			}
			NewHashes[NewIndex] = Old[0];
		}

		munmap(Store->Hashes, Store->HashSize * sizeof(hash_t));
		close(Store->HashesFd);

		char FileName[strlen(Store->Prefix) + 10];
		sprintf(FileName, "%s.hashes", Store->Prefix);
		rename(FileName2, FileName);

		Store->HashSize = NewSize;
		Store->Hashes = NewHashes;
		Store->HashesFd = HashesFd;
		string_store_set_extra(Store->Keys, Store->HashSize);

		//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	}
	return INVALID_INDEX;
}

size_t string_index_search(string_index_t *Store, const char *Key) {
	size_t Length = strlen(Key);
	uint32_t Hash = hash(Key);
	unsigned int Mask = Store->HashSize - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash) {
			int Cmp = string_store_compare(Store->Keys, Hashes[Index].Link, Key, Length);
			if (Cmp < 0) break;
			if (Cmp == 0) return Hashes[Index].Link;
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}
