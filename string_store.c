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

typedef struct header_t {
	uint32_t NodeSize, ChunkSize;
	uint32_t NumEntries, NumFreeEntries, FreeEntry;
	uint32_t NumNodes, NumFreeNodes, FreeNode;
	entry_t Entries[];
} header_t;

struct string_store_t {
	const char *Prefix;
	header_t *Header;
	void *Data;
	size_t HeaderSize;
	int HeaderFd, DataFd;
};

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize) {
	uint32_t NodeSize = 16;
	while (NodeSize < RequestedSize) NodeSize *= 2;
	if (!ChunkSize) ChunkSize = 512;
	int NumEntries = (512 - sizeof(header_t)) / sizeof(entry_t);
	int NumNodes = ChunkSize / NodeSize;
	string_store_t *Store = new(string_store_t);
#ifndef NO_GC
	Store->Prefix = strdup(Prefix);
#else
	Store->Prefix = GC_strdup(Prefix);
#endif
	char FileName[strlen(Prefix) + 10];
	sprintf(FileName, "%s.entries", Prefix);
	Store->HeaderFd = open(FileName, O_RDWR | O_CREAT, 0777);
	Store->HeaderSize = sizeof(header_t) + NumEntries * sizeof(entry_t);
	ftruncate(Store->HeaderFd, Store->HeaderSize);
	Store->Header = mmap(NULL, Store->HeaderSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->HeaderFd, 0);
	Store->Header->NodeSize = NodeSize;
	Store->Header->ChunkSize = ChunkSize;
	Store->Header->NumEntries = Store->Header->NumFreeEntries = NumEntries;
	Store->Header->FreeEntry = 0;
	Store->Header->NumNodes = NumNodes;
	Store->Header->NumFreeNodes = NumNodes;
	Store->Header->FreeNode = 0;
	for (int I = 0; I < NumEntries; ++I) {
		Store->Header->Entries[I].Link = I + 1;
		Store->Header->Entries[I].Length = INVALID_INDEX;
	}
	sprintf(FileName, "%s.data", Prefix);
	Store->DataFd = open(FileName, O_RDWR | O_CREAT, 0777);
	ftruncate(Store->DataFd, NumNodes * NodeSize);
	Store->Data = mmap(NULL, Store->Header->NumNodes * Store->Header->NodeSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
	for (int I = 0; I < NumNodes; ++I) {
		((data_t *)(Store->Data + I * NodeSize))->Link = I + 1;
	}
	msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_ASYNC);
	return Store;
}

string_store_t *string_store_open(const char *Prefix) {
	string_store_t *Store = new(string_store_t);
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
	sprintf(FileName, "%s.data", Prefix);
	Store->DataFd = open(FileName, O_RDWR, 0777);
	Store->Data = mmap(NULL, Store->Header->NumNodes * Store->Header->NodeSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
	return Store;
}

void string_store_close(string_store_t *Store) {
	msync(Store->Data, Store->Header->NumNodes * Store->Header->NodeSize, MS_SYNC);
	msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Data, Store->Header->NumNodes * Store->Header->NodeSize);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->DataFd);
	close(Store->HeaderFd);
}

void string_store_reserve(string_store_t *Store, size_t Index) {
	while (Index >= Store->Header->NumEntries || Store->Header->Entries[Index].Length == INVALID_INDEX) string_store_alloc(Store);
}

size_t string_store_alloc(string_store_t *Store) {
	size_t Index;
	if (!Store->Header->NumFreeEntries) {
		int NumEntries = 512 / sizeof(entry_t);
		size_t HeaderSize = Store->HeaderSize + NumEntries * sizeof(entry_t);
		msync(Store->Header, Store->HeaderSize, MS_SYNC);
		ftruncate(Store->HeaderFd, HeaderSize);
		Store->Header = mremap(Store->Header, Store->HeaderSize, HeaderSize, MREMAP_MAYMOVE);
		entry_t *Entries = Store->Header->Entries;
		for (int I = Store->Header->NumEntries; I < Store->Header->NumEntries + NumEntries; ++I) {
			Entries[I].Link = I + 1;
			Entries[I].Length = INVALID_INDEX;
		}
		Store->Header->FreeEntry = Store->Header->NumEntries;
		Store->Header->NumEntries += NumEntries;
		Store->Header->NumFreeEntries = NumEntries;
		Store->HeaderSize = HeaderSize;
	}
	Index = Store->Header->FreeEntry;
	Store->Header->FreeEntry = Store->Header->Entries[Index].Link;
	--Store->Header->NumFreeEntries;
	Store->Header->Entries[Index].Link = INVALID_INDEX;
	Store->Header->Entries[Index].Length = 0;
	msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	return Index;
}

void string_store_free(string_store_t *Store, size_t Index) {
	string_store_set(Store, Index, NULL, 0);
	Store->Header->Entries[Index].Link = Store->Header->FreeEntry;
	Store->Header->Entries[Index].Length = INVALID_INDEX;
	Store->Header->FreeEntry = Index;
	++Store->Header->NumFreeEntries;
}

size_t string_store_get_size(string_store_t *Store, size_t Index) {
	if (Index >= Store->Header->NumEntries) return INVALID_INDEX;
	return Store->Header->Entries[Index].Length;
}

void string_store_get_value(string_store_t *Store, size_t Index, void *Buffer) {
	if (Index >= Store->Header->NumEntries) return;
	size_t Length = Store->Header->Entries[Index].Length;
	uint32_t Link = Store->Header->Entries[Index].Link;
	uint32_t NodeSize = Store->Header->NodeSize;
	data_t *Node = Store->Data + Link * NodeSize;
	while (Length > NodeSize) {
		memcpy(Buffer, Node->Chars, NodeSize - 4);
		Buffer += NodeSize - 4;
		Length -= NodeSize - 4;
		Node = Store->Data + Node->Link * NodeSize;
	}
	memcpy(Buffer, Node, Length);
}

void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length) {
	if (Index >= Store->Header->NumEntries) return;
	size_t OldLength = Store->Header->Entries[Index].Length;
	if (OldLength == INVALID_INDEX) return;
	Store->Header->Entries[Index].Length = Length;
	uint32_t NodeSize = Store->Header->NodeSize;
	size_t OldNumBlocks = (OldLength > NodeSize) ? 1 + (OldLength - 5) / (NodeSize - 4) : (OldLength != 0);
	size_t NewNumBlocks = (Length > NodeSize) ? 1 + (Length - 5) / (NodeSize - 4) : (Length != 0);
	if (OldNumBlocks > NewNumBlocks) {
		uint32_t FreeStart = Store->Header->Entries[Index].Link;
		if (NewNumBlocks) {
			data_t *Node = Store->Data + FreeStart * NodeSize;
			while (Length > NodeSize) {
				memcpy(Node->Chars, Buffer, NodeSize - 4);
				Buffer += NodeSize - 4;
				Length -= NodeSize - 4;
				Node = Store->Data + Node->Link * NodeSize;
			}
			FreeStart = Node->Link;
			memcpy(Node, Buffer, Length);
		}
		data_t *FreeEnd = Store->Data + FreeStart * NodeSize;
		size_t NumFree = OldNumBlocks - NewNumBlocks;
		Store->Header->NumFreeNodes += NumFree;
		for (int I = NumFree; --I > 0;) FreeEnd = Store->Data + FreeEnd->Link * NodeSize;
		FreeEnd->Link = Store->Header->FreeNode;
		Store->Header->FreeNode = FreeStart;
	} else if (OldNumBlocks < NewNumBlocks) {
		size_t NumRequired = NewNumBlocks - OldNumBlocks;
		size_t NumFree = Store->Header->NumFreeNodes;
		if (NumRequired > NumFree) {
			int Shortfall = Store->Header->ChunkSize / NodeSize;
			while (Shortfall < NumRequired - NumFree) Shortfall += Store->Header->ChunkSize / NodeSize;
			size_t HeaderSize = (Store->Header->NumNodes + Shortfall) * NodeSize;
			msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_SYNC);
			ftruncate(Store->DataFd, HeaderSize);
			Store->Data = mremap(Store->Data, Store->Header->NumNodes * NodeSize, HeaderSize, MREMAP_MAYMOVE);
			uint32_t FreeEnd;
			if (NumFree > 0) {
				FreeEnd = Store->Header->FreeNode;
				for (int I = NumFree; --I > 0;) FreeEnd = ((data_t *)(Store->Data + FreeEnd * NodeSize))->Link;
				FreeEnd = ((data_t *)(Store->Data + FreeEnd * NodeSize))->Link = Store->Header->NumNodes;
			} else {
				FreeEnd = Store->Header->FreeNode = Store->Header->NumNodes;
			}
			Store->Header->NumNodes += Shortfall;
			Store->Header->NumFreeNodes += Shortfall - NumRequired;
			while (--Shortfall > 0) FreeEnd = ((data_t *)(Store->Data + FreeEnd * NodeSize))->Link = FreeEnd + 1;
		} else {
			Store->Header->NumFreeNodes -= NumRequired;
		}
		if (OldNumBlocks) {
			data_t *Node = Store->Data + Store->Header->Entries[Index].Link * NodeSize;
			for (int I = OldNumBlocks; --I > 0;) {
				memcpy(Node->Chars, Buffer, NodeSize - 4);
				Buffer += NodeSize - 4;
				Length -= NodeSize - 4;
				Node = Store->Data + Node->Link * NodeSize;
			}
			Node->Link = Store->Header->FreeNode;
		} else {
			Store->Header->Entries[Index].Link = Store->Header->FreeNode;
		}
		data_t *Node = Store->Data + Store->Header->FreeNode * NodeSize;
		while (Length > NodeSize) {
			memcpy(Node->Chars, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			Node = Store->Data + Node->Link * NodeSize;
		}
		Store->Header->FreeNode = Node->Link;
		memcpy(Node, Buffer, Length);
	} else {
		data_t *Node = Store->Data + Store->Header->Entries[Index].Link * NodeSize;
		while (Length > NodeSize) {
			memcpy(Node->Chars, Buffer, NodeSize - 4);
			Buffer += NodeSize - 4;
			Length -= NodeSize - 4;
			Node = Store->Data + Node->Link * NodeSize;
		}
		memcpy(Node, Buffer, Length);
	}
	msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_ASYNC);
}
