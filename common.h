#ifndef RADB_CONTEXT_H
#define RADB_CONTEXT_H

#ifdef RADB_MEM_PER_STORE

#define RADB_MEM_PARAMS \
	, void *(*alloc)(size_t) \
	, void *(*alloc_atomic)(size_t) \
	, void (*free)(void *)

#define RADB_MEM_ARGS \
	, alloc, alloc_atomic, free

#else

#define RADB_MEM_PARAMS
#define RADB_MEM_ARGS

#endif

#endif
