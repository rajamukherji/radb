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
	size_t IndexMapSize, DataMapSize;
	int IndexFd, DataFd;
};

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize) {
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
