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

typedef struct entry_t {
	uint32_t Link, Length;
} entry_t;

typedef struct data_t {
	uint32_t Link;
	char Chars[];
} data_t;

typedef struct hash_t {
	uint32_t Hash;
	uint32_t Link;
} hash_t;

typedef struct header_t {
	uint32_t NodeSize, Reserved;
	uint32_t NumEntries, NumFreeEntries, FreeEntry;
	uint32_t NumNodes, NumFreeNodes, FreeNode;
	entry_t Entries[];
} header_t;

struct string_store_t {
	const char *Prefix;
	union {
		void *IndexMap;
		header_t *Index;
	};
	void *DataMap;
	union {
		void *HashMap;
		hash_t *Hashes;
	};
	size_t IndexMapSize, DataMapSize, HashMapSize, HashSize;
	int IndexFd, DataFd, HashFd;
};

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, int Index) {
	uint32_t NodeSize = 16;
	while (NodeSize < RequestedSize) NodeSize *= 2;
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	int Fd = open(FileName, O_RDWR | O_CREAT, 0777);
	header_t Header = {NodeSize, 0,};
	write(Fd, &Header, sizeof(Header));
	close(Fd);
	sprintf(FileName, "%s.data", Prefix);
	Fd = open(FileName, O_RDWR | O_CREAT, 0777);
	close(Fd);
	if (Index) {
		sprintf(FileName, "%s.hashes", Prefix);
		Fd = open(FileName, O_RDWR | O_CREAT, 0777);
		hash_t Hashes[8];
		for (int I = 0; I < 8; ++I) Hashes[I].Link = INVALID_INDEX;
		write(Fd, &Hashes, sizeof(Hashes));
		close(Fd);
	}
	return string_store_open(Prefix);
}

string_store_t *string_store_open(const char *Prefix) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return NULL;
	int IndexFd = open(FileName, O_RDWR, 0777);
	size_t IndexMapSize = Stat->st_size;
	void *IndexMap = mmap(NULL, IndexMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, IndexFd, 0);
	sprintf(FileName, "%s.data", Prefix);
	if (stat(FileName, Stat)) {
		munmap(IndexMap, IndexMapSize);
		return NULL;
	}
	int DataFd = open(FileName, O_RDWR, 0777);
	size_t DataMapSize = Stat->st_size;
	void *DataMap = NULL;
	if (DataMapSize) {
		DataMap = mmap(NULL, DataMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, DataFd, 0);
	}
	string_store_t *Store = new(string_store_t);
#ifndef NO_GC
	Store->Prefix = strdup(Prefix);
#else
	Store->Prefix = GC_strdup(Prefix);
#endif
	Store->IndexMap = IndexMap;
	Store->IndexMapSize = IndexMapSize;
	Store->IndexFd = IndexFd;
	Store->DataMap = DataMap;
	Store->DataMapSize = DataMapSize;
	Store->DataFd = DataFd;
	sprintf(FileName, "%s.hashes", Prefix);
	if (!stat(FileName, Stat)) {
		int HashFd = Store->HashFd = open(FileName, O_RDWR, 0777);
		size_t HashMapSize = Store->HashMapSize = Stat->st_size;
		Store->HashMap = mmap(NULL, HashMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, HashFd, 0);
		Store->HashSize = HashMapSize / sizeof(hash_t);
	}
	return Store;
}

void string_store_close(string_store_t *Store) {

}

void string_store_reserve(string_store_t *Store, size_t Index) {

}

size_t string_store_alloc(string_store_t *Store) {
	size_t Index;
	if (Store->Index->NumFreeEntries) {
		Index = Store->Index->FreeEntry;
		Store->Index->FreeEntry = Store->Index->Entries[Index].Link;
		--Store->Index->NumFreeEntries;
	} else {
		size_t MapSize = Store->IndexMapSize + sizeof(entry_t);
		msync(Store->IndexMap, Store->IndexMapSize, MS_SYNC);
		ftruncate(Store->IndexFd, MapSize);
		Store->IndexMap = mremap(Store->IndexMap, Store->IndexMapSize, MapSize, MREMAP_MAYMOVE);
		Index = Store->Index->NumEntries++;
		Store->IndexMapSize = MapSize;
	}
	Store->Index->Entries[Index].Link = 0xFFFFFFFF;
	Store->Index->Entries[Index].Length = 0;
	msync(Store->IndexMap, Store->IndexMapSize, MS_ASYNC);
	return Index;
}

void string_store_free(string_store_t *Store, size_t Index) {
	string_store_set(Store, Index, NULL, 0);
	Store->Index->Entries[Index].Link = Store->Index->FreeEntry;
	Store->Index->FreeEntry = Index;
	++Store->Index->NumFreeEntries;
}

size_t string_store_get_size(string_store_t *Store, size_t Index) {
	if (Index >= Store->Index->NumEntries) return 0;
	return Store->Index->Entries[Index].Length;
}

void string_store_get_value(string_store_t *Store, size_t Index, void *Buffer) {
	if (Index >= Store->Index->NumEntries) return;
	size_t Length = Store->Index->Entries[Index].Length;
	uint32_t Link = Store->Index->Entries[Index].Link;
	uint32_t NodeSize = Store->Index->NodeSize;
	data_t *Node = Store->DataMap + Link * NodeSize;
	while (Length > NodeSize) {
		memcpy(Buffer, Node->Chars, NodeSize - 4);
		Buffer += NodeSize - 4;
		Length -= NodeSize - 4;
		Node = Store->DataMap + Node->Link * NodeSize;
	}
	memcpy(Buffer, Node, Length);
}

void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length) {
	if (Index >= Store->Index->NumEntries) return;
	size_t OldLength = Store->Index->Entries[Index].Length;
	Store->Index->Entries[Index].Length = Length;
	uint32_t NodeSize = Store->Index->NodeSize;
	size_t OldNumBlocks = OldLength > NodeSize ? 1 + (OldLength - NodeSize) / (NodeSize - 4) : (OldLength != 0);
	size_t NewNumBlocks = Length > NodeSize ? 1 + (Length - NodeSize) / (NodeSize - 4) : (Length != 0);
	if (OldNumBlocks > NewNumBlocks) {
		uint32_t FreeStart = Store->Index->Entries[Index].Link;
		if (NewNumBlocks) {
			data_t *Node = Store->DataMap + FreeStart * NodeSize;
			while (Length > NodeSize) {
				memcpy(Node->Chars, Buffer, NodeSize - 4);
				Buffer += NodeSize - 4;
				Length -= NodeSize - 4;
				Node = Store->DataMap + Node->Link * NodeSize;
			}
			FreeStart = Node->Link;
			memcpy(Node, Buffer, Length);
		}
		data_t *FreeEnd = Store->DataMap + FreeStart * NodeSize;
		size_t NumFree = OldNumBlocks - NewNumBlocks;
		Store->Index->NumFreeNodes += NumFree;
		for (int I = NumFree; --I > 0;) FreeEnd = Store->DataMap + FreeEnd->Link * NodeSize;
		FreeEnd->Link = Store->Index->FreeNode;
		Store->Index->FreeNode = FreeStart;
	} else if (OldNumBlocks < NewNumBlocks) {
		size_t NumRequired = NewNumBlocks - OldNumBlocks;
		size_t NumFree = Store->Index->NumFreeNodes;
		if (NumRequired > NumFree) {
			int Shortfall = NumRequired - NumFree;
			size_t MapSize = Store->DataMapSize + Shortfall * NodeSize;
			if (Store->DataMapSize) {
				msync(Store->DataMap, Store->DataMapSize, MS_SYNC);
				ftruncate(Store->DataFd, MapSize);
				Store->DataMap = mremap(Store->DataMap, Store->DataMapSize, MapSize, MREMAP_MAYMOVE);
			} else {
				ftruncate(Store->DataFd, MapSize);
				Store->DataMap = mmap(NULL, MapSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
			}
			uint32_t FreeEnd;
			if (NumFree > 0) {
				FreeEnd = Store->Index->FreeNode;
				for (int I = NumFree; --I > 0;) FreeEnd = ((data_t *)(Store->DataMap + FreeEnd * NodeSize))->Link;
				FreeEnd = ((data_t *)(Store->DataMap + FreeEnd * NodeSize))->Link = Store->DataMapSize / NodeSize;
			} else {
				FreeEnd = Store->Index->FreeNode = Store->DataMapSize / NodeSize;
			}
			Store->Index->NumNodes += Shortfall;
			while (--Shortfall > 0) FreeEnd = ((data_t *)(Store->DataMap + FreeEnd * NodeSize))->Link = FreeEnd + 1;
			Store->DataMapSize = MapSize;
			Store->Index->NumFreeNodes = 0;
		} else {
			Store->Index->NumFreeNodes -= NumRequired;
		}
		if (OldNumBlocks) {
			data_t *Node = Store->DataMap + Store->Index->Entries[Index].Link * NodeSize;
			for (int I = OldNumBlocks; --I > 0;) {
				memcpy(Node->Chars, Buffer, NodeSize - 4);
				Buffer += NodeSize - 4;
				Length -= NodeSize - 4;
				Node = Store->DataMap + Node->Link * NodeSize;
			}
			Node->Link = Store->Index->FreeNode;
		} else {
			Store->Index->Entries[Index].Link = Store->Index->FreeNode;
		}
		data_t *Node = Store->DataMap + Store->Index->FreeNode * NodeSize;
		while (Length > NodeSize) {
			memcpy(Node->Chars, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			Node = Store->DataMap + Node->Link * NodeSize;
		}
		Store->Index->FreeNode = Node->Link;
		memcpy(Node, Buffer, Length);
	} else {
		data_t *Node = Store->DataMap + Store->Index->Entries[Index].Link * NodeSize;
		while (Length > NodeSize) {
			memcpy(Node->Chars, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			Node = Store->DataMap + Node->Link * NodeSize;
		}
		memcpy(Node, Buffer, Length);
	}
	msync(Store->IndexMap, Store->IndexMapSize, MS_ASYNC);
	msync(Store->DataMap, Store->DataMapSize, MS_ASYNC);
}

#define hash(KEY, LENGTH) ({ \
	uint32_t Hash = 5381; \
	unsigned char *P = (unsigned char *)(KEY); \
	for (int I = (LENGTH); --I >= 0;) { \
		Hash = ((Hash << 5) + Hash) + P++[0]; \
	} \
	Hash; \
})

static int string_store_compare(string_store_t *Store, const void *Buffer1, size_t Length1, size_t Index2) {
	size_t Length2 = Store->Index->Entries[Index2].Length;
	uint32_t Link2 = Store->Index->Entries[Index2].Link;
	uint32_t NodeSize = Store->Index->NodeSize;
	data_t *Node2 = Store->DataMap + Link2 * NodeSize;
	while (Length2 > NodeSize) {
		if (NodeSize - 4 > Length1) {
			return memcmp(Buffer1, Node2->Chars, Length1) | 1;
		}
		int Cmp = memcmp(Buffer1, Node2->Chars, NodeSize - 4);
		if (Cmp) return Cmp;
		Buffer1 += NodeSize - 4;
		Length1 -= NodeSize - 4;
		Length2 -= NodeSize - 4;
		Node2 = Store->DataMap + Node2->Link * NodeSize;
	}
	if (Length1 > Length2) {
		return -(memcmp(Node2, Buffer1, Length2) | 1);
	} else if (Length2 > Length1) {
		return memcmp(Buffer1, Node2, Length1) | 1;
	} else {
		return memcmp(Buffer1, Node2, Length2);
	}
}

static int string_store_compare2(string_store_t *Store, size_t Index1, size_t Index2) {
	uint32_t NodeSize = Store->Index->NodeSize;
	size_t Length1 = Store->Index->Entries[Index1].Length;
	uint32_t Link1 = Store->Index->Entries[Index1].Link;
	data_t *Node1 = Store->DataMap + Link1 * NodeSize;
	size_t Length2 = Store->Index->Entries[Index2].Length;
	uint32_t Link2 = Store->Index->Entries[Index2].Link;
	data_t *Node2 = Store->DataMap + Link2 * NodeSize;
	while (Length1 > NodeSize && Length2 > NodeSize) {
		int Cmp = memcmp(Node1->Chars, Node2->Chars, NodeSize - 4);
		if (Cmp) return Cmp;
		Length1 -= NodeSize - 4;
		Length2 -= NodeSize - 4;
		Node1 = Store->DataMap + Node1->Link * NodeSize;
		Node2 = Store->DataMap + Node2->Link * NodeSize;
	}
	if (Length1 > NodeSize) {
		return -(memcmp(Node2, Node1->Chars, Length2) | 1);
	} else if (Length2 > NodeSize) {
		return memcmp(Node1, Node2->Chars, Length1) | 1;
	} else if (Length1 > Length2) {
		return -(memcmp(Node2, Node1, Length2) | 1);
	} else if (Length2 > Length1) {
		return memcmp(Node1, Node2, Length1) | 1;
	} else {
		return memcmp(Node1, Node2, Length2);
	}
}

static void sort_hashes(string_store_t *Store, hash_t *First, hash_t *Last) {
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
				Cmp = string_store_compare2(Store, T.Link, P.Link);
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

size_t string_store_insert(string_store_t *Store, const void *Buffer, size_t Length) {
	uint32_t Hash = hash(Buffer, Length);
	unsigned int Mask = Store->HashSize - 1;
	for (;;) {
		unsigned int Incr = ((Hash >> 8) | 1) & Mask;
		unsigned int Index = Hash & Mask;
		hash_t *Hashes = Store->Hashes;
		for (;;) {
			if (Hashes[Index].Link == INVALID_INDEX) break;
			if (Hashes[Index].Hash < Hash) break;
			if (Hashes[Index].Hash == Hash) {
				int Cmp = string_store_compare(Store, Buffer, Length, Hashes[Index].Link);
				if (Cmp < 0) break;
				if (Cmp == 0) return Hashes[Index].Link;
			}
			Index += Incr;
			Index &= Mask;
		}
		uint32_t Space = Store->HashSize - Store->Index->NumEntries;
		if (Space > Store->HashSize >> 3) {
			uint32_t Result = string_store_alloc(Store);
			string_store_set(Store, Result, Buffer, Length);
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
						msync(Store->HashMap, Store->HashMapSize, MS_ASYNC);
						return Result;
					} else if (Hashes[Index].Hash < Old.Hash) {
						hash_t New = Hashes[Index];
						Hashes[Index] = Old;
						Old = New;
						break;
					} else if (Hashes[Index].Hash == Old.Hash) {
						int Cmp = string_store_compare2(Store, Hashes[Index].Link, Old.Link);
						if (Cmp < 0) {
							hash_t New = Hashes[Index];
							Hashes[Index] = Old;
							Old = New;
							break;
						}
					}
				}
			}
			return Result;
		}
		size_t NewSize = Store->HashSize * 2;
		Mask = NewSize - 1;

		char FileName2[strlen(Store->Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);

		size_t HashMapSize = NewSize * sizeof(hash_t);
		int HashFd = open(FileName2, O_RDWR | O_CREAT, 0777);
		ftruncate(HashFd, HashMapSize);
		hash_t *NewHashes = mmap(NULL, HashMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, HashFd, 0);
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

		munmap(Store->HashMap, Store->HashMapSize);
		close(Store->HashFd);

		char FileName[strlen(Store->Prefix) + 10];
		sprintf(FileName, "%s.hashes", Store->Prefix);
		rename(FileName2, FileName);

		Store->Hashes = NewHashes;
		Store->HashFd = HashFd;
		Store->HashMapSize = HashMapSize;
		Store->HashSize = NewSize;
	}
	return INVALID_INDEX;
}

size_t string_store_lookup(string_store_t *Store, const void *Buffer, size_t Length) {
	uint32_t Hash = hash(Buffer, Length);
	unsigned int Mask = Store->HashSize - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash) {
			int Cmp = string_store_compare(Store, Buffer, Length, Hashes[Index].Link);
			if (Cmp < 0) break;
			if (Cmp == 0) return Hashes[Index].Link;
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}
