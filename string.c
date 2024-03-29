#include "string_store.h"
#include "string_index.h"
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

#define STRING_STORE_SIGNATURE 0x53534152
#define STRING_STORE_VERSION MAKE_VERSION(1, 0)

typedef struct {
	uint32_t Link, Length;
} entry_t;

typedef struct {
	uint32_t Signature, Version;
	uint32_t NodeSize, ChunkSize;
	uint32_t NumEntries, NumNodes, NumFreeNodes, FreeNode;
	uint32_t FreeEntry, Reserved;
	entry_t Entries[];
} string_store_header_t;

struct string_store_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	string_store_header_t *Header;
	void *Data;
	size_t HeaderSize;
	int HeaderFd, DataFd;
};

#define NODE_LINK(Node) (*(uint32_t *)(Node + NodeSize - 4))

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS) {
#if defined(RADB_MEM_MALLOC)
	string_store_t *Store = malloc(sizeof(string_store_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	string_store_t *Store = GC_malloc(sizeof(string_store_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	string_store_t *Store = alloc(Allocator, sizeof(string_store_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	size_t NodeSize = 8;
	while (NodeSize < RequestedSize) NodeSize *= 2;
	if (!ChunkSize) ChunkSize = 512;
	int NumEntries = (512 - sizeof(string_store_header_t)) / sizeof(entry_t);
	int NumNodes = (ChunkSize + NodeSize - 1) / NodeSize;
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT | O_TRUNC, 0777);
	Store->HeaderSize = sizeof(string_store_header_t) + NumEntries * sizeof(entry_t);
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = STRING_STORE_SIGNATURE;
	Store->Header->Version = STRING_STORE_VERSION;
	Store->Header->NodeSize = NodeSize;
	Store->Header->ChunkSize = NumNodes;
	Store->Header->NumEntries = NumEntries;
	Store->Header->NumNodes = NumNodes;
	Store->Header->NumFreeNodes = NumNodes;
	Store->Header->FreeNode = 0;
	Store->Header->FreeEntry = 0;
	for (int I = 0; I < NumEntries; ++I) {
		Store->Header->Entries[I].Link = INVALID_INDEX;
		Store->Header->Entries[I].Length = 0;
	}
	sprintf(FileName, "%s.data", Prefix);
	Store->DataFd = open(FileName, O_RDWR | O_CREAT | O_TRUNC, 0777);
	ftruncate(Store->DataFd, NumNodes * NodeSize);
	Store->Data = mmap(NULL, Store->Header->NumNodes * Store->Header->NodeSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
	for (int I = 1; I <= NumNodes; ++I) {
		*(uint32_t *)(Store->Data + I * NodeSize - 4) = I;
	}
	//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	//msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_ASYNC);
	return Store;
}

string_store_open_t string_store_open2(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	if (stat(FileName, Stat)) return (string_store_open_t){NULL, RADB_FILE_NOT_FOUND};
#if defined(RADB_MEM_MALLOC)
	string_store_t *Store = malloc(sizeof(string_store_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	string_store_t *Store = GC_malloc(sizeof(string_store_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	string_store_t *Store = alloc(Allocator, sizeof(string_store_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	if (Store->Header->Signature != STRING_STORE_SIGNATURE) {
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		return (string_store_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	sprintf(FileName, "%s.data", Prefix);
	Store->DataFd = open(FileName, O_RDWR, 0777);
	Store->Data = mmap(NULL, Store->Header->NumNodes * Store->Header->NodeSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
	return (string_store_open_t){Store, RADB_SUCCESS};
}

string_store_t *string_store_open(const char *Prefix RADB_MEM_PARAMS) {
	return string_store_open2(Prefix RADB_MEM_ARGS).Store;
}

void string_store_close(string_store_t *Store) {
	msync(Store->Data, Store->Header->NumNodes * Store->Header->NodeSize, MS_SYNC);
	msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Data, Store->Header->NumNodes * Store->Header->NodeSize);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->DataFd);
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

size_t string_store_num_entries(string_store_t *Store) {
	return Store->Header->NumEntries;
}

size_t string_store_size(string_store_t *Store, size_t Index) {
	if (Index >= Store->Header->NumEntries) return 0;
	size_t Link = Store->Header->Entries[Index].Link;
	if (Link == INVALID_INDEX) return 0;
	return Store->Header->Entries[Index].Length;
}

size_t string_store_get(string_store_t *Store, size_t Index, void *Buffer, size_t Space) {
	if (Index >= Store->Header->NumEntries) return 0;
	size_t Link = Store->Header->Entries[Index].Link;
	if (Link == INVALID_INDEX) return 0;
	size_t Length = Store->Header->Entries[Index].Length;
	if (!Length) return Length;
	size_t NodeSize = Store->Header->NodeSize;
	void *Node = Store->Data + Link * NodeSize;
	size_t Total = (Space < Length) ? Space : Length;
	while (Length > NodeSize) {
		if (Space < NodeSize - 4) {
			memcpy(Buffer, Node, Space);
			return Total;
		}
		memcpy(Buffer, Node, NodeSize - 4);
		Buffer += NodeSize - 4;
		Length -= NodeSize - 4;
		Space -= NodeSize - 4;
		Node = Store->Data + NodeSize * NODE_LINK(Node);
	}
	memcpy(Buffer, Node, (Space < Length) ? Space : Length);
	return Total;
}

static int string_store_compare_unchecked(string_store_t *Store, const void *Other, size_t Length, size_t Index) {
	size_t Length2 = Store->Header->Entries[Index].Length;
	size_t Link = Store->Header->Entries[Index].Link;
	size_t NodeSize = Store->Header->NodeSize;
	void *Node = Store->Data + Link * NodeSize;
	while (Length2 > NodeSize) {
		if (Length < NodeSize - 4) {
			return memcmp(Other, Node, Length) ?: -1;
		}
		int Cmp = memcmp(Other, Node, NodeSize - 4);
		if (Cmp) return Cmp;
		Other += NodeSize - 4;
		Length2 -= NodeSize - 4;
		Length -= NodeSize - 4;
		Node = Store->Data + NodeSize * NODE_LINK(Node);
	}
	if (Length < Length2) {
		return memcmp(Other, Node, Length) ?: -1;
	} else if (Length > Length2) {
		return memcmp(Other, Node, Length2) ?: 1;
	} else {
		return memcmp(Other, Node, Length);
	}
}

int string_store_compare(string_store_t *Store, const void *Other, size_t Length, size_t Index) {
	if (Index >= Store->Header->NumEntries) return 1;
	return string_store_compare_unchecked(Store, Other, Length, Index);
}

static int string_store_compare2_unchecked(string_store_t *Store, size_t Index1, size_t Index2) {
	size_t Length1 = Store->Header->Entries[Index1].Length;
	size_t Length2 = Store->Header->Entries[Index2].Length;
	size_t Link1 = Store->Header->Entries[Index1].Link;
	size_t Link2 = Store->Header->Entries[Index2].Link;
	size_t NodeSize = Store->Header->NodeSize;
	void *Node1 = Store->Data + Link1 * NodeSize;
	void *Node2 = Store->Data + Link2 * NodeSize;
	while (Length1 > NodeSize && Length2 > NodeSize) {
		int Cmp = memcmp(Node1, Node2, NodeSize - 4);
		if (Cmp) return Cmp;
		Length1 -= NodeSize - 4;
		Length2 -= NodeSize - 4;
		Node1 = Store->Data + NodeSize * NODE_LINK(Node1);
		Node2 = Store->Data + NodeSize * NODE_LINK(Node2);
	}
	if (Length1 > NodeSize) {
		if (Length2 > NodeSize - 4) {
			int Cmp = memcmp(Node1, Node2, NodeSize - 4);
			if (Cmp) return Cmp;
			Length1 -= NodeSize - 4;
			Length2 -= NodeSize - 4;
			Node1 = Store->Data + NodeSize * NODE_LINK(Node1);
			return memcmp(Node1, Node2 + NodeSize - 4, Length2) ?: 1;
		} else {
			return memcmp(Node1, Node2, Length2) ?: 1;
		}
	} else if (Length2 > NodeSize) {
		if (Length1 > NodeSize - 4) {
			int Cmp = memcmp(Node1, Node2, NodeSize - 4);
			if (Cmp) return Cmp;
			Length1 -= NodeSize - 4;
			Length2 -= NodeSize - 4;
			Node2 = Store->Data + NodeSize * NODE_LINK(Node2);
			return memcmp(Node1 + NodeSize - 4, Node2, Length1) ?: -1;
		} else {
			return memcmp(Node1, Node2, Length1) ?: -1;
		}
	} else if (Length1 > Length2) {
		return memcmp(Node1, Node2, Length2) ?: 1;
	} else if (Length2 > Length1) {
		return memcmp(Node1, Node2, Length1) ?: -1;
	} else {
		return memcmp(Node1, Node2, Length1);
	}
}

int string_store_compare2(string_store_t *Store, size_t Index1, size_t Index2) {
	if (Index1 >= Store->Header->NumEntries) return -1;
	if (Index2 >= Store->Header->NumEntries) return 1;
	return string_store_compare2_unchecked(Store, Index1, Index2);
}

void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length) {
	if (Index >= Store->Header->NumEntries) {
		size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
		NumEntries += 512 - 1;
		NumEntries /= 512;
		NumEntries *= 512;
		size_t HeaderSize = Store->HeaderSize + NumEntries * sizeof(entry_t);
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		entry_t *Entries = Store->Header->Entries;
		for (int I = Store->Header->NumEntries; I < Store->Header->NumEntries + NumEntries; ++I) {
			Entries[I].Link = INVALID_INDEX;
			Entries[I].Length = 0;
		}
		Store->Header->NumEntries += NumEntries;
		Store->HeaderSize = HeaderSize;
	}
	size_t OldLength = Store->Header->Entries[Index].Length;
	Store->Header->Entries[Index].Length = Length;
	size_t NodeSize = Store->Header->NodeSize;
	size_t OldNumBlocks = (OldLength > NodeSize) ? 1 + (OldLength - 5) / (NodeSize - 4) : (OldLength != 0);
	size_t NewNumBlocks = (Length > NodeSize) ? 1 + (Length - 5) / (NodeSize - 4) : (Length != 0);
	if (OldNumBlocks > NewNumBlocks) {
		size_t FreeStart = Store->Header->Entries[Index].Link;
		if (NewNumBlocks) {
			void *Node = Store->Data + FreeStart * NodeSize;
			while (Length > NodeSize) {
				memcpy(Node, Buffer, NodeSize - 4);
				Buffer += NodeSize - 4;
				Length -= NodeSize - 4;
				Node = Store->Data + NodeSize * NODE_LINK(Node);
			}
			FreeStart = NODE_LINK(Node);
			memcpy(Node, Buffer, Length);
		}
		void *FreeEnd = Store->Data + FreeStart * NodeSize;
		size_t NumFree = OldNumBlocks - NewNumBlocks;
		Store->Header->NumFreeNodes += NumFree;
		for (int I = NumFree; --I > 0;) FreeEnd = Store->Data + NodeSize * NODE_LINK(FreeEnd);
		NODE_LINK(FreeEnd) = Store->Header->FreeNode;
		Store->Header->FreeNode = FreeStart;
	} else if (OldNumBlocks < NewNumBlocks) {
		size_t NumRequired = NewNumBlocks - OldNumBlocks;
		size_t NumFree = Store->Header->NumFreeNodes;
		if (NumRequired > NumFree) {
			int NumNodes = NumRequired - NumFree;
			NumNodes += Store->Header->ChunkSize - 1;
			NumNodes /= Store->Header->ChunkSize;
			NumNodes *= Store->Header->ChunkSize;
			size_t DataSize = (Store->Header->NumNodes + NumNodes) * NodeSize;
			//msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_SYNC);
			ftruncate(Store->DataFd, DataSize);
#ifdef Linux
			Store->Data = mremap(Store->Data, Store->Header->NumNodes * NodeSize, DataSize, MREMAP_MAYMOVE);
#else
			munmap(Store->Data, Store->Header->NumNodes * NodeSize);
			Store->Data = mmap(NULL, DataSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
#endif
			size_t FreeEnd;
			if (NumFree > 0) {
				FreeEnd = Store->Header->FreeNode;
				for (int I = NumFree; --I > 0;) FreeEnd = NODE_LINK(Store->Data + FreeEnd * NodeSize);
				FreeEnd = NODE_LINK(Store->Data + FreeEnd * NodeSize) = Store->Header->NumNodes;
			} else {
				FreeEnd = Store->Header->FreeNode = Store->Header->NumNodes;
			}
			Store->Header->NumNodes += NumNodes;
			Store->Header->NumFreeNodes += NumNodes - NumRequired;
			while (--NumNodes > 0) FreeEnd = NODE_LINK(Store->Data + FreeEnd * NodeSize) = FreeEnd + 1;
		} else {
			Store->Header->NumFreeNodes -= NumRequired;
		}
		if (OldNumBlocks) {
			void *Node = Store->Data + Store->Header->Entries[Index].Link * NodeSize;
			for (int I = OldNumBlocks; --I > 0;) {
				memcpy(Node, Buffer, NodeSize - 4);
				Buffer += NodeSize - 4;
				Length -= NodeSize - 4;
				Node = Store->Data + NodeSize * NODE_LINK(Node);
			}
			memcpy(Node, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			NODE_LINK(Node) = Store->Header->FreeNode;
		} else {
			Store->Header->Entries[Index].Link = Store->Header->FreeNode;
		}
		void *Node = Store->Data + Store->Header->FreeNode * NodeSize;
		while (Length > NodeSize) {
			memcpy(Node, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			Node = Store->Data + NodeSize * NODE_LINK(Node);
		}
		Store->Header->FreeNode = NODE_LINK(Node);
		memcpy(Node, Buffer, Length);
	} else {
		void *Node = Store->Data + Store->Header->Entries[Index].Link * NodeSize;
		while (Length > NodeSize) {
			memcpy(Node, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			Node = Store->Data + NodeSize * NODE_LINK(Node);
		}
		memcpy(Node, Buffer, Length);
	}
	//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	//msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_ASYNC);
}

void string_store_shift(string_store_t *Store, size_t Source, size_t Count, size_t Destination) {
	size_t Index = (Source > Destination ? Source : Destination) + Count;
	if (Index >= Store->Header->NumEntries) {
		size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
		NumEntries += 512 - 1;
		NumEntries /= 512;
		NumEntries *= 512;
		size_t HeaderSize = Store->HeaderSize + NumEntries * sizeof(entry_t);
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		entry_t *Entries = Store->Header->Entries;
		for (int I = Store->Header->NumEntries; I < Store->Header->NumEntries + NumEntries; ++I) {
			Entries[I].Link = INVALID_INDEX;
			Entries[I].Length = 0;
		}
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
	entry_t *Entries = Store->Header->Entries;
	if (SmallCount <= 64) {
		entry_t *SmallSaved = alloca(SmallCount * sizeof(entry_t));
		memcpy(SmallSaved, Entries + SmallSource, SmallCount * sizeof(entry_t));
		memmove(Entries + LargeDest, Entries + LargeSource, LargeCount * sizeof(entry_t));
		memcpy(Entries + SmallDest, SmallSaved, SmallCount * sizeof(entry_t));
	} else {
		entry_t *SmallSaved = malloc(SmallCount * sizeof(entry_t));
		memcpy(SmallSaved, Entries + SmallSource, SmallCount * sizeof(entry_t));
		memmove(Entries + LargeDest, Entries + LargeSource, LargeCount * sizeof(entry_t));
		memcpy(Entries + SmallDest, SmallSaved, SmallCount * sizeof(entry_t));
		free(SmallSaved);
	}
}

size_t string_store_alloc(string_store_t *Store) {
	size_t FreeEntry = Store->Header->FreeEntry;
	size_t Index = Store->Header->Entries[FreeEntry].Link;
	if (Index == INVALID_INDEX) {
		Index = FreeEntry + 1;
		if (Index >= Store->Header->NumEntries) {
			size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
			NumEntries += 512 - 1;
			NumEntries /= 512;
			NumEntries *= 512;
			size_t HeaderSize = Store->HeaderSize + NumEntries * sizeof(entry_t);
			ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
			Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
			munmap(Store->Header, Store->HeaderSize);
			Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
			entry_t *Entries = Store->Header->Entries;
			for (int I = Store->Header->NumEntries; I < Store->Header->NumEntries + NumEntries; ++I) {
				Entries[I].Link = INVALID_INDEX;
				Entries[I].Length = 0;
			}
			Store->Header->NumEntries += NumEntries;
			Store->HeaderSize = HeaderSize;
		}
	}
	Store->Header->FreeEntry = Index;
	return FreeEntry;
}

void string_store_free(string_store_t *Store, size_t Index) {
	size_t OldLength = Store->Header->Entries[Index].Length;
	Store->Header->Entries[Index].Length = 0;
	size_t NodeSize = Store->Header->NodeSize;
	size_t OldNumBlocks = (OldLength > NodeSize) ? 1 + (OldLength - 5) / (NodeSize - 4) : (OldLength != 0);
	if (OldNumBlocks > 0) {
		size_t FreeStart = Store->Header->Entries[Index].Link;
		void *FreeEnd = Store->Data + FreeStart * NodeSize;
		Store->Header->NumFreeNodes += OldNumBlocks;
		for (int I = OldNumBlocks; --I > 0;) FreeEnd = Store->Data + NodeSize * NODE_LINK(FreeEnd);
		NODE_LINK(FreeEnd) = Store->Header->FreeNode;
		Store->Header->FreeNode = FreeStart;
	}
	Store->Header->Entries[Index].Link = Store->Header->FreeEntry;
	Store->Header->FreeEntry = Index;
}

void string_store_writer_open(string_store_writer_t *Writer, string_store_t *Store, size_t Index) {
	if (Index >= Store->Header->NumEntries) {
		size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
		NumEntries += 512 - 1;
		NumEntries /= 512;
		NumEntries *= 512;
		size_t HeaderSize = Store->HeaderSize + NumEntries * sizeof(entry_t);
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		entry_t *Entries = Store->Header->Entries;
		for (int I = Store->Header->NumEntries; I < Store->Header->NumEntries + NumEntries; ++I) {
			Entries[I].Link = INVALID_INDEX;
			Entries[I].Length = 0;
		}
		Store->Header->NumEntries += NumEntries;
		Store->HeaderSize = HeaderSize;
	}
	size_t OldLength = Store->Header->Entries[Index].Length;
	size_t NodeSize = Store->Header->NodeSize;
	size_t OldNumBlocks = (OldLength > NodeSize) ? 1 + (OldLength - 5) / (NodeSize - 4) : (OldLength != 0);
	if (OldNumBlocks > 0) {
		size_t FreeStart = Store->Header->Entries[Index].Link;
		void *FreeEnd = Store->Data + FreeStart * NodeSize;
		Store->Header->NumFreeNodes += OldNumBlocks;
		for (int I = OldNumBlocks; --I > 0;) FreeEnd = Store->Data + NodeSize * NODE_LINK(FreeEnd);
		NODE_LINK(FreeEnd) = Store->Header->FreeNode;
		Store->Header->FreeNode = FreeStart;
	}
	Writer->Store = Store;
	Writer->Node = INVALID_INDEX;
	Writer->Index = Index;
	Store->Header->Entries[Index].Length = 0;
	Store->Header->Entries[Index].Link = INVALID_INDEX;
}

void string_store_writer_append(string_store_writer_t *Writer, string_store_t *Store, size_t Index) {
	if (Index >= Store->Header->NumEntries) {
		size_t NumEntries = (Index + 1) - Store->Header->NumEntries;
		NumEntries += 512 - 1;
		NumEntries /= 512;
		NumEntries *= 512;
		size_t HeaderSize = Store->HeaderSize + NumEntries * sizeof(entry_t);
		ftruncate(Store->HeaderFd, HeaderSize);
#ifdef Linux
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Header, Store->HeaderSize);
		Store->Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
#endif
		entry_t *Entries = Store->Header->Entries;
		for (int I = Store->Header->NumEntries; I < Store->Header->NumEntries + NumEntries; ++I) {
			Entries[I].Link = INVALID_INDEX;
			Entries[I].Length = 0;
		}
		Store->Header->NumEntries += NumEntries;
		Store->HeaderSize = HeaderSize;
	}
	Writer->Store = Store;
	Writer->Index = Index;
	size_t NodeIndex = Store->Header->Entries[Index].Link;
	if (NodeIndex != INVALID_INDEX) {
		size_t NodeSize = Store->Header->NodeSize;
		size_t Offset = Store->Header->Entries[Index].Length;
		while (Offset > NodeSize) {
			void *Node = Store->Data + NodeSize * NodeIndex;
			NodeIndex = NODE_LINK(Node);
			Offset -= (NodeSize - 4);
		}
		Writer->Node = NodeIndex;
		Writer->Remain = NodeSize - Offset;
	} else {
		Writer->Node = INVALID_INDEX;
	}
}


static inline size_t string_store_node_alloc(string_store_t *Store, size_t NodeSize) {
	if (!Store->Header->NumFreeNodes) {
		size_t NumNodes = Store->Header->ChunkSize;
		size_t DataSize = (Store->Header->NumNodes + NumNodes) * NodeSize;
		//msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_SYNC);
		ftruncate(Store->DataFd, DataSize);
#ifdef Linux
		Store->Data = mremap(Store->Data, Store->Header->NumNodes * NodeSize, DataSize, MREMAP_MAYMOVE);
#else
		munmap(Store->Data, Store->Header->NumNodes * NodeSize);
		Store->Data = mmap(NULL, DataSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
#endif
		size_t Index = Store->Header->NumNodes;
		size_t FreeEnd = Store->Header->FreeNode = Store->Header->NumNodes + 1;
		Store->Header->NumNodes += NumNodes;
		Store->Header->NumFreeNodes += --NumNodes ;
		while (--NumNodes > 0) FreeEnd = NODE_LINK(Store->Data + FreeEnd * NodeSize) = FreeEnd + 1;
		return Index;
	} else {
		--Store->Header->NumFreeNodes;
		size_t Index = Store->Header->FreeNode;
		Store->Header->FreeNode = NODE_LINK(Store->Data + NodeSize * Index);
		return Index;
	}
}

size_t string_store_writer_write(string_store_writer_t *Writer, const void *Buffer, size_t Length) {
	if (Length == 0) return Length;
	string_store_t *Store = Writer->Store;
	Store->Header->Entries[Writer->Index].Length += Length;
	size_t NodeSize = Store->Header->NodeSize;
	size_t NodeIndex = Writer->Node;
	size_t Remain = Length, Offset, Space;
	if (NodeIndex == INVALID_INDEX) {
		NodeIndex = string_store_node_alloc(Store, NodeSize);
		Store->Header->Entries[Writer->Index].Link = NodeIndex;
		Space = NodeSize;
		Offset = 0;
	} else {
		Space = Writer->Remain;
		Offset = NodeSize - Space;
	}
	void *Node = Store->Data + NodeSize * NodeIndex;
	if (Remain > Space) {
		if (Space <= 4) {
			uint32_t Save = NODE_LINK(Node);
			size_t NewIndex = string_store_node_alloc(Store, NodeSize);
			Node = Store->Data + NodeSize * NodeIndex;
			NODE_LINK(Node) = NewIndex;
			NodeIndex = NewIndex;
			Node = Store->Data + NodeSize * NodeIndex;
			*(uint32_t *)Node = Save;
			Offset = 4 - Space;
			Space = NodeSize - Offset;
		}
		while (Remain > Space) {
			memcpy(Node + Offset, Buffer, Space - 4);
			Buffer += Space - 4;
			Remain -= Space - 4;
			size_t NewIndex = string_store_node_alloc(Store, NodeSize);
			Node = Store->Data + NodeSize * NodeIndex;
			NODE_LINK(Node) = NewIndex;
			NodeIndex = NewIndex;
			Node = Store->Data + NodeSize * NodeIndex;
			Offset = 0;
			Space = NodeSize;
		}
	}
	memcpy(Node + Offset, Buffer, Remain);
	Writer->Node = NodeIndex;
	Writer->Remain = Space - Remain;
	return Length;
}

void string_store_reader_open(string_store_reader_t *Reader, string_store_t *Store, size_t Index) {
	Reader->Store = Store;
	Reader->Offset = 0;
	if (Index >= Store->Header->NumEntries) {
		Reader->Node = INVALID_INDEX;
		Reader->Remain = 0;
	} else {
		Reader->Node = Store->Header->Entries[Index].Link;
		Reader->Remain = Store->Header->Entries[Index].Length;
	}
}

size_t string_store_reader_read(string_store_reader_t *Reader, void *Buffer, size_t Length) {
	string_store_t *Store = Reader->Store;
	size_t NodeSize = Store->Header->NodeSize;
	size_t NodeIndex = Reader->Node;
	if (NodeIndex == INVALID_INDEX) return 0;
	size_t Offset = Reader->Offset;
	size_t Remain = Reader->Remain;
	size_t Copied = 0;
	for (;;) {
		void *Node = Store->Data + NodeSize * NodeIndex;
		if (Offset + Remain <= NodeSize) {
			// Last node
			if (Length <= Remain) {
				memcpy(Buffer, Node + Offset, Length);
				Reader->Node = NodeIndex;
				Reader->Offset = Offset + Length;
				Reader->Remain = Remain - Length;
				return Copied + Length;
			} else {
				memcpy(Buffer, Node + Offset, Remain);
				Reader->Node = INVALID_INDEX;
				return Copied + Remain;
			}
		} else {
			size_t Available = NodeSize - Offset - 4;
			if (Length <= Available) {
				memcpy(Buffer, Node + Offset, Length);
				Reader->Node = NodeIndex;
				Reader->Offset = Offset + Length;
				Reader->Remain = Remain - Length;
				return Copied + Length;
			} else {
				memcpy(Buffer, Node + Offset, Available);
				NodeIndex = NODE_LINK(Node);
				Offset = 0;
				Remain -= Available;
				Copied += Available;
				Buffer += Available;
				Length -= Available;
			}
		}
	}
}

#define STRING_INDEX_SIGNATURE 0x49534152
#define STRING_INDEX_VERSION MAKE_VERSION(1, 1)

typedef struct {
	uint32_t Hash;
	uint32_t Link;
} hash_t;

typedef struct {
	uint32_t Signature, Version;
	uint32_t Size, Space;
	uint32_t Deleted, Reserved;
	hash_t Hashes[];
} string_index_header_t;

struct string_index_t {
#ifdef RADB_MEM_PER_STORE
	void *Allocator;
	void *(*alloc)(void *, size_t);
	void *(*alloc_atomic)(void *, size_t);
	void (*free)(void *, void *);
#endif
	const char *Prefix;
	string_index_header_t *Header;
	string_store_t *Keys;
	size_t HeaderSize;
	int HeaderFd;
	int SyncCounter;
};

string_index_t *string_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS) {
#if defined(RADB_MEM_MALLOC)
	string_index_t *Store = malloc(sizeof(string_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	string_index_t *Store = GC_malloc(sizeof(string_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	string_index_t *Store = alloc(Allocator, sizeof(string_index_t));
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
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT | O_TRUNC, 0777);
	Store->HeaderSize = sizeof(string_index_header_t) + 64 * sizeof(hash_t);
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->Signature = STRING_INDEX_SIGNATURE;
	Store->Header->Version = STRING_INDEX_VERSION;
	Store->Header->Size = Store->Header->Space = 64;
	for (int I = 0; I < Store->Header->Size; ++I) Store->Header->Hashes[I].Link = INVALID_INDEX;
	Store->Keys = string_store_create(Prefix, KeySize, ChunkSize RADB_MEM_ARGS);
	//msync(Index->Header, Index->HeaderSize, MS_ASYNC);
	//msync(Index->Hashes, Index->Header->HashSize * sizeof(hash_t), MS_ASYNC);
	return Store;
}

typedef struct {
	uint32_t Signature, Version;
	uint32_t Size, Space;
	hash_t Hashes[];
} string_index_header_v0_t;

string_index_open_t string_index_open2(const char *Prefix RADB_MEM_PARAMS) {
	struct stat Stat[1];
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.index", Prefix);
	if (stat(FileName, Stat)) return (string_index_open_t){NULL, RADB_FILE_NOT_FOUND};
	string_store_open_t KeysOpen = string_store_open2(Prefix RADB_MEM_ARGS);
	if (!KeysOpen.Store) return (string_index_open_t){NULL, KeysOpen.Error + 3};
#if defined(RADB_MEM_MALLOC)
	string_index_t *Store = malloc(sizeof(string_index_t));
	Store->Prefix = strdup(Prefix);
#elif defined(RADB_MEM_GC)
	string_index_t *Store = GC_malloc(sizeof(string_index_t));
	Store->Prefix = GC_strdup(Prefix);
#else
	string_index_t *Store = alloc(Allocator, sizeof(string_index_t));
	Store->Prefix = radb_strdup(Prefix, Allocator, alloc_atomic);
	Store->Allocator = Allocator;
	Store->alloc = alloc;
	Store->alloc_atomic = alloc_atomic;
	Store->free = free;
#endif
	Store->HeaderFd = open(FileName, O_RDWR, 0777);
	Store->HeaderSize = Stat->st_size;
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	if (Store->Header->Version == MAKE_VERSION(1, 0)) {
		string_index_header_v0_t *HeaderV0 = (string_index_header_v0_t *)Store->Header;
		char FileName2[strlen(Prefix) + 10];
		sprintf(FileName2, "%s.temp", Store->Prefix);
		uint32_t HashSize = HeaderV0->Size;
		size_t HeaderSize = sizeof(string_index_header_t) + HashSize * sizeof(hash_t);
		int HeaderFd = open(FileName2, O_RDWR | O_CREAT | O_TRUNC, 0777);
		ftruncate(HeaderFd, HeaderSize);
		string_index_header_t *Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, HeaderFd, 0);
		Header->Signature = STRING_INDEX_SIGNATURE;
		Header->Version = STRING_INDEX_VERSION;
		Header->Size = HashSize;
		Header->Space = HeaderV0->Space;
		Header->Deleted = 0;
		memcpy(Header->Hashes, HeaderV0->Hashes, HashSize * sizeof(hash_t));
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		rename(FileName2, FileName);
		Store->HeaderSize = HeaderSize;
		Store->Header = Header;
		Store->HeaderFd = HeaderFd;
	} else if (Store->Header->Signature != STRING_INDEX_SIGNATURE) {
		munmap(Store->Header, Store->HeaderSize);
		close(Store->HeaderFd);
		string_store_close(KeysOpen.Store);
		return (string_index_open_t){NULL, RADB_HEADER_MISMATCH};
	}
	Store->Keys = KeysOpen.Store;
	return (string_index_open_t){Store, RADB_SUCCESS};
}

string_index_t *string_index_open(const char *Prefix RADB_MEM_PARAMS) {
	return string_index_open2(Prefix RADB_MEM_ARGS).Index;
}

void string_index_close(string_index_t *Store) {
	string_store_close(Store->Keys);
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

size_t string_index_num_entries(string_index_t *Store) {
	return Store->Header->Size - (Store->Header->Space + Store->Header->Deleted);
}

size_t string_index_num_deleted(string_index_t *Store) {
	return Store->Header->Deleted;
}

size_t string_index_size(string_index_t *Store, size_t Index) {
	return string_store_size(Store->Keys, Index);
}

size_t string_index_get(string_index_t *Store, size_t Index, void *Buffer, size_t Space) {
	return string_store_get(Store->Keys, Index, Buffer, Space);
}

static void sort_hashes(string_index_t *Store, hash_t *First, hash_t *Last) {
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
				Cmp = string_store_compare2_unchecked(Store->Keys, T.Link, P.Link);
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

index_result_t string_index_insert2(string_index_t *Store, const char *Key, size_t Length) {
	if (!Length) Length = strlen(Key);
	uint32_t Hash = hash(Key, Length);
	unsigned int Mask = Store->Header->Size - 1;
	for (;;) {
		unsigned int Incr = ((Hash >> 8) | 1) & Mask;
		unsigned int Index = Hash & Mask;
		hash_t *Hashes = Store->Header->Hashes;
		for (;;) {
			if (Hashes[Index].Link == INVALID_INDEX) break;
			if (Hashes[Index].Hash < Hash) break;
			if (Hashes[Index].Hash == Hash) {
				int Cmp = string_store_compare_unchecked(Store->Keys, Key, Length, Hashes[Index].Link);
				if (Cmp > 0) break;
				if (Cmp == 0) return (index_result_t){Hashes[Index].Link, 0};
			}
			Index += Incr;
			Index &= Mask;
		}
		size_t Space = Store->Header->Space;
		if (--Space > Store->Header->Size >> 3) {
			Store->Header->Space = Space;
			uint32_t Link = string_store_alloc(Store->Keys);
			string_store_set(Store->Keys, Link, Key, Length);
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
						int Cmp = string_store_compare2_unchecked(Store->Keys, Hashes[Index].Link, Old.Link);
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

		size_t HeaderSize = sizeof(string_index_header_t) + HashSize * sizeof(hash_t);
		int HeaderFd = open(FileName2, O_RDWR | O_CREAT | O_TRUNC, 0777);
		ftruncate(HeaderFd, HeaderSize);
		string_index_header_t *Header = mmap(NULL, HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, HeaderFd, 0);
		Header->Signature = STRING_INDEX_SIGNATURE;
		Header->Version = STRING_INDEX_VERSION;
		Header->Size = HashSize;
		Header->Space = Store->Header->Space + Store->Header->Deleted + (HashSize - Store->Header->Size);
		Header->Deleted = 0;
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

size_t string_index_insert(string_index_t *Store, const char *Key, size_t Length) {
	return string_index_insert2(Store, Key, Length).Index;
}

size_t string_index_search(string_index_t *Store, const char *Key, size_t Length) {
	if (!Length) Length = strlen(Key);
	uint32_t Hash = hash(Key, Length);
	unsigned int Mask = Store->Header->Size - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Header->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash && Hashes[Index].Link != DELETED_INDEX) {
			int Cmp = string_store_compare_unchecked(Store->Keys, Key, Length, Hashes[Index].Link);
			if (Cmp > 0) break;
			if (Cmp == 0) return Hashes[Index].Link;
		}
		Index += Incr;
		Index &= Mask;
	}
	return INVALID_INDEX;
}

size_t string_index_delete(string_index_t *Store, const char *Key, size_t Length) {
	if (!Length) Length = strlen(Key);
	uint32_t Hash = hash(Key, Length);
	unsigned int Mask = Store->Header->Size - 1;
	unsigned int Incr = ((Hash >> 8) | 1) & Mask;
	unsigned int Index = Hash & Mask;
	hash_t *Hashes = Store->Header->Hashes;
	for (;;) {
		if (Hashes[Index].Link == INVALID_INDEX) break;
		if (Hashes[Index].Hash < Hash) break;
		if (Hashes[Index].Hash == Hash && Hashes[Index].Link != DELETED_INDEX) {
			int Cmp = string_store_compare_unchecked(Store->Keys, Key, Length, Hashes[Index].Link);
			if (Cmp > 0) break;
			if (Cmp == 0) {
				uint32_t Link = Hashes[Index].Link;
				string_store_free(Store->Keys, Link);
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

int string_index_foreach(string_index_t *Store, void *Data, string_index_foreach_fn Callback) {
	hash_t *Hash = Store->Header->Hashes;
	hash_t *Limit = Hash + Store->Header->Size;
	while (Hash < Limit) {
		if (Hash->Link != INVALID_INDEX) if (Callback(Hash->Link, Data)) return 1;
		++Hash;
	}
	return 0;
}
