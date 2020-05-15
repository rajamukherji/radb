#include "string_store.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifndef NO_GC
#include <gc.h>
#define new(T) ((T *)GC_malloc(sizeof(T)))
#else
#define new(T) ((T *)malloc(sizeof(T)))
#endif

typedef struct entry_t {
	uint32_t Link, Length;
} entry_t;

typedef struct header_t {
	uint32_t NodeSize, ChunkSize;
	uint32_t NumEntries, NumNodes, NumFreeNodes, FreeNode;
	uint32_t FreeEntry, Extra;
	entry_t Entries[];
} header_t;

struct string_store_t {
	const char *Prefix;
	header_t *Header;
	void *Data;
	size_t HeaderSize;
	int HeaderFd, DataFd;
};

uint32_t string_store_get_extra(string_store_t *Store) {
	return Store->Header->Extra;
}

void string_store_set_extra(string_store_t *Store, uint32_t Value) {
	Store->Header->Extra = Value;
}

#define NODE_LINK(Node) (*(uint32_t *)(Node + NodeSize - 4))

string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize) {
	size_t NodeSize = 8;
	while (NodeSize < RequestedSize) NodeSize *= 2;
	if (!ChunkSize) ChunkSize = 512;
	int NumEntries = (512 - sizeof(header_t)) / sizeof(entry_t);
	int NumNodes = (ChunkSize + NodeSize - 1) / NodeSize;
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
	Store->DataFd = open(FileName, O_RDWR | O_CREAT, 0777);
	ftruncate(Store->DataFd, NumNodes * NodeSize);
	Store->Data = mmap(NULL, Store->Header->NumNodes * Store->Header->NodeSize, PROT_READ | PROT_WRITE, MAP_SHARED, Store->DataFd, 0);
	for (int I = 1; I <= NumNodes; ++I) {
		*(uint32_t *)(Store->Data + I * NodeSize - 4) = I;
	}
	//msync(Store->Header, Store->HeaderSize, MS_ASYNC);
	//msync(Store->Data, Store->Header->NumNodes * NodeSize, MS_ASYNC);
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
	//msync(Store->Data, Store->Header->NumNodes * Store->Header->NodeSize, MS_SYNC);
	//msync(Store->Header, Store->HeaderSize, MS_SYNC);
	munmap(Store->Data, Store->Header->NumNodes * Store->Header->NodeSize);
	munmap(Store->Header, Store->HeaderSize);
	close(Store->DataFd);
	close(Store->HeaderFd);
}

size_t string_store_num_entries(string_store_t *Store) {
	return Store->Header->NumEntries;
}

size_t string_store_get_size(string_store_t *Store, size_t Index) {
	if (Index >= Store->Header->NumEntries) return 0;
	return Store->Header->Entries[Index].Length;
}

void string_store_get_value(string_store_t *Store, size_t Index, void *Buffer) {
	if (Index >= Store->Header->NumEntries) return;
	size_t Length = Store->Header->Entries[Index].Length;
	size_t Link = Store->Header->Entries[Index].Link;
	size_t NodeSize = Store->Header->NodeSize;
	void *Node = Store->Data + Link * NodeSize;
	while (Length > NodeSize) {
		memcpy(Buffer, Node, NodeSize - 4);
		Buffer += NodeSize - 4;
		Length -= NodeSize - 4;
		Node = Store->Data + NodeSize * NODE_LINK(Node);
	}
	memcpy(Buffer, Node, Length);
}

int string_store_compare(string_store_t *Store, size_t Index, const void *Other, size_t Length) {
	if (Index >= Store->Header->NumEntries) return - 1;
	size_t Length2 = Store->Header->Entries[Index].Length;
	size_t Link = Store->Header->Entries[Index].Link;
	size_t NodeSize = Store->Header->NodeSize;
	void *Node = Store->Data + Link * NodeSize;
	while (Length2 > NodeSize) {
		if (Length < NodeSize) {
			return memcmp(Other, Node, Length2) ?: -1;
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

int string_store_compare2(string_store_t *Store, size_t Index1, size_t Index2) {
	if (Index1 >= Store->Header->NumEntries) return -1;
	if (Index2 >= Store->Header->NumEntries) return 1;
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
		return memcmp(Node1, Node2, Length1) ?: 1;
	} else {
		return memcmp(Node1, Node2, Length1);
	}
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
	if (OldLength == INVALID_INDEX) return;
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
	Writer->Node = 0;
	Writer->Index = Index;
	Store->Header->Entries[Index].Length = 0;
	Store->Header->Entries[Index].Link = INVALID_INDEX;
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
	void *Node = Writer->Node;
	size_t Remain = Length, Offset, Space;
	if (!Node) {
		size_t Index = string_store_node_alloc(Store, NodeSize);
		Store->Header->Entries[Writer->Index].Link = Index;
		Node = Store->Data + NodeSize * Index;
		Space = NodeSize;
		Offset = 0;
	} else {
		Space = Writer->Remain;
		Offset = NodeSize - Space;
	}
	if (Space < 4) {
		uint32_t Save = NODE_LINK(Node);
		size_t Index = string_store_node_alloc(Store, NodeSize);
		NODE_LINK(Node) = Index;
		Node = Store->Data + NodeSize * Index;
		*(uint32_t *)Node = Save;
		Offset = 4 - Space;
		Space += NodeSize - 4;
	}
	while (Remain > Space) {
		memcpy(Node + Offset, Buffer, Space - 4);
		Buffer += Space - 4;
		Remain -= Space - 4;
		size_t Index = string_store_node_alloc(Store, NodeSize);
		NODE_LINK(Node) = Index;
		Node = Store->Data + NodeSize * Index;
		Offset = 0;
		Space = NodeSize - 4;
	}
	memcpy(Node + Offset, Buffer, Remain);
	Writer->Node = Node;
	Writer->Remain = Space - Remain;
	return Length;
}

void string_store_reader_open(string_store_reader_t *Reader, string_store_t *Store, size_t Index) {
	Reader->Store = Store;
	Reader->Node = Store->Data + Store->Header->NodeSize * Store->Header->Entries[Index].Link;
	Reader->Offset = 0;
	Reader->Remain = Store->Header->Entries[Index].Length;
}

size_t string_store_reader_read(string_store_reader_t *Reader, void *Buffer, size_t Length) {
	string_store_t *Store = Reader->Store;
	size_t NodeSize = Store->Header->NodeSize;
	void *Node = Reader->Node;
	if (!Node) return 0;
	size_t Offset = Reader->Offset;
	size_t Remain = Reader->Remain;
	size_t Copied = 0;
	for (;;) {
		if (Offset + Remain <= NodeSize) {
			// Last node
			if (Length < Remain) {
				memcpy(Buffer, Node + Offset, Length);
				Reader->Node = Node;
				Reader->Offset = Offset + Length;
				Reader->Remain = Remain - Length;
				return Copied + Length;
			} else {
				memcpy(Buffer, Node + Offset, Remain);
				Reader->Node = 0;
				return Copied + Remain;
			}
		} else {
			size_t Available = NodeSize - Offset - 4;
			if (Length < Available) {
				memcpy(Buffer, Node + Offset, Length);
				Reader->Node = Node;
				Reader->Offset = Offset + Length;
				Reader->Remain = Remain - Length;
				return Copied + Length;
			} else {
				memcpy(Buffer, Node + Offset, Available);
				Node = Store->Data + NodeSize * NODE_LINK(Node);
				Offset = 0;
				Remain -= Available;
				Copied += Available;
				Buffer += Available;
				Length -= Available;
			}
		}
	}
}
