#include "string_index.h"
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

typedef struct header_t {
	uint32_t NumKeys, KeysSize, HashSize;
	uint32_t StringsSize, StringsEnd;
	uint32_t ChunkSize, Reserved2, Reserved3;
	uint32_t Keys[];
} header_t;

struct string_index_t {
	const char *Prefix;
	header_t *Header;
	hash_t *Hashes;
	char *Strings;
	size_t HeaderSize;
	int HeaderFd, HashesFd, StringsFd;
};

string_index_t *string_index_create(const char *Prefix, size_t ChunkSize) {
	if (!ChunkSize) ChunkSize = 512;
	string_index_t *Index = new(string_index_t);
#ifndef NO_GC
	Index->Prefix = strdup(Prefix);
#else
	Index->Prefix = GC_strdup(Prefix);
#endif
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.keys", Prefix);
	Index->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Index->HeaderSize = sizeof(header_t) + 120 * sizeof(uint32_t);
	ftruncate(Index->HeaderFd, Index->HeaderSize);
	Index->Header = mmap(NULL, Index->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Index->HeaderFd, 0);
	Index->Header->ChunkSize = ChunkSize;
	Index->Header->NumKeys = 0;
	Index->Header->KeysSize = 120;
	Index->Header->HashSize = 64;
	Index->Header->StringsSize = ChunkSize;
	Index->Header->StringsEnd = 0;
	sprintf(FileName, "%s.hashes", Prefix);
	Index->HashesFd = open(FileName, O_RDWR | O_CREAT, 0777);
	ftruncate(Index->HashesFd, Index->Header->HashSize * sizeof(hash_t));
	Index->Hashes = mmap(NULL, Index->Header->HashSize * sizeof(hash_t), PROT_READ | PROT_WRITE, MAP_SHARED, Index->HashesFd, 0);
	for (int I = 0; I < Index->Header->HashSize; ++I) Index->Hashes[I].Link = INVALID_INDEX;
	sprintf(FileName, "%s.strings", Prefix);
	Index->StringsFd = open(FileName, O_RDWR | O_CREAT, 0777);
	ftruncate(Index->StringsFd, Index->Header->StringsSize);
	Index->Strings = mmap(NULL, Index->Header->StringsSize, PROT_READ | PROT_WRITE, MAP_SHARED, Index->StringsFd, 0);
	msync(Index->Header, Index->HeaderSize, MS_ASYNC);
	msync(Index->Hashes, Index->Header->HashSize * sizeof(hash_t), MS_ASYNC);
	return Index;
}

string_index_t *string_index_open(const char *Prefix) {
	string_index_t *Index = new(string_index_t);
#ifndef NO_GC
	Index->Prefix = strdup(Prefix);
#else
	Index->Prefix = GC_strdup(Prefix);
#endif
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.keys", Prefix);
	if (stat(FileName, Stat)) return NULL;
	Index->HeaderFd = open(FileName, O_RDWR, 0777);
	Index->HeaderSize = Stat->st_size;
	Index->Header = mmap(NULL, Index->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Index->HeaderFd, 0);
	sprintf(FileName, "%s.hashes", Prefix);
	Index->HashesFd = open(FileName, O_RDWR, 0777);
	Index->Hashes = mmap(NULL, Index->Header->HashSize * sizeof(hash_t), PROT_READ | PROT_WRITE, MAP_SHARED, Index->HashesFd, 0);
	sprintf(FileName, "%s.strings", Prefix);
	Index->StringsFd = open(FileName, O_RDWR, 0777);
	Index->Strings = mmap(NULL, Index->Header->StringsSize, PROT_READ | PROT_WRITE, MAP_SHARED, Index->StringsFd, 0);
	return Index;
}

void string_index_close(string_index_t *Store) {
	msync(Store->Strings, Store->Header->StringsSize, MS_SYNC);
	msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_SYNC);
	msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Strings, Store->Header->StringsSize);
	munmap(Store->Hashes, Store->Header->HashSize * sizeof(hash_t));
	munmap(Store->Header, Store->HeaderSize);
	close(Store->StringsFd);
	close(Store->HashesFd);
	close(Store->HeaderFd);
}

#define hash(KEY) ({ \
	uint32_t Hash = 5381; \
	unsigned char *P = (unsigned char *)(KEY); \
	while (P[0]) Hash = ((Hash << 5) + Hash) + P++[0]; \
	Hash; \
})

static void sort_hashes(string_index_t *Index, hash_t *First, hash_t *Last) {
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
				Cmp = strcmp(
					Index->Strings + Index->Header->Keys[T.Link],
					Index->Strings + Index->Header->Keys[P.Link]
				);
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
	if (First < A - 1) sort_hashes(Index, First, A - 1);
	if (A + 1 < Last) sort_hashes(Index, A + 1, Last);
}

size_t string_index_insert(string_index_t *Store, const void *Key) {
uint32_t Hash = hash(Key);
	unsigned int Mask = Store->Header->HashSize - 1;
	for (;;) {
		unsigned int Incr = ((Hash >> 8) | 1) & Mask;
		unsigned int Index = Hash & Mask;
		hash_t *Hashes = Store->Hashes;
		for (;;) {
			if (Hashes[Index].Link == INVALID_INDEX) break;
			if (Hashes[Index].Hash < Hash) break;
			if (Hashes[Index].Hash == Hash) {
				int Cmp = strcmp(Key, Store->Strings + Store->Header->Keys[Hashes[Index].Link]);
				if (Cmp < 0) break;
				if (Cmp == 0) return Hashes[Index].Link;
			}
			Index += Incr;
			Index &= Mask;
		}
		size_t Space = Store->Header->HashSize - (Store->Header->NumKeys + 1);
		if (Space > Store->Header->HashSize >> 3) {
			uint32_t Result = Store->Header->NumKeys++;
			uint32_t Offset = Store->Header->StringsEnd;

			if (Result >= Store->Header->KeysSize) {
				msync(Store->Header, Store->HeaderSize, MS_SYNC);
				uint32_t HeaderSize = Store->HeaderSize + 128 * sizeof(uint32_t);
				ftruncate(Store->HeaderFd, HeaderSize);
				Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
				Store->Header->KeysSize += 128;
				Store->HeaderSize = HeaderSize;
			}
			Store->Header->Keys[Result] = Offset;

			size_t KeySize = strlen(Key) + 1;
			if (Offset + KeySize > Store->Header->StringsSize) {
				msync(Store->Strings, Store->Header->StringsSize, MS_SYNC);
				uint32_t StringsSize = Store->Header->StringsSize + Store->Header->ChunkSize;
				ftruncate(Store->StringsFd, StringsSize);
				Store->Strings = mremap(Store->Strings, Store->Header->StringsSize, StringsSize, MREMAP_MAYMOVE);
				Store->Header->StringsSize = StringsSize;
			}
			strcpy(Store->Strings + Offset, Key);
			Store->Header->StringsEnd = Offset + KeySize;
			msync(Store->Strings, Store->Header->StringsSize, MS_ASYNC);

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
						msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_ASYNC);
						return Result;
					} else if (Hashes[Index].Hash < Old.Hash) {
						hash_t New = Hashes[Index];
						Hashes[Index] = Old;
						Old = New;
						break;
					} else if (Hashes[Index].Hash == Old.Hash) {
						int Cmp = strcmp(Store->Strings + Store->Header->Keys[Hashes[Index].Link], Store->Strings + Store->Header->Keys[Old.Link]);
						if (Cmp < 0) {
							hash_t New = Hashes[Index];
							Hashes[Index] = Old;
							Old = New;
							break;
						}
					}
				}
			}
			msync(Store->Hashes, Store->Header->HashSize * sizeof(hash_t), MS_ASYNC);
			return Result;
		}
		size_t NewSize = Store->Header->HashSize * 2;
		Mask = NewSize - 1;

		char FileName2[strlen(Store->Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);

		size_t HashMapSize = NewSize * sizeof(hash_t);
		int HashesFd = open(FileName2, O_RDWR | O_CREAT, 0777);
		ftruncate(HashesFd, HashMapSize);
		hash_t *NewHashes = mmap(NULL, HashMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, HashesFd, 0);
		for (int I = 0; I < NewSize; ++I) NewHashes[I].Link = INVALID_INDEX;

		sort_hashes(Store, Hashes, Hashes + Store->Header->HashSize - 1);
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

		munmap(Store->Hashes, Store->Header->HashSize * sizeof(hash_t));
		close(Store->HashesFd);

		char FileName[strlen(Store->Prefix) + 10];
		sprintf(FileName, "%s.hashes", Store->Prefix);
		rename(FileName2, FileName);

		Store->Hashes = NewHashes;
		Store->HashesFd = HashesFd;
		Store->Header->HashSize = NewSize;

		msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	}
	return INVALID_INDEX;
}

size_t string_index_search(string_index_t *Store, const void *Key) {
	uint32_t Hash = hash(Key);
	unsigned int Mask = Store->Header->HashSize - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash) {
			int Cmp = strcmp(Key, Store->Strings + Store->Header->Keys[Hashes[Index].Link]);
			if (Cmp < 0) break;
			if (Cmp == 0) return Hashes[Index].Link;
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}
