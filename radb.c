#include "minilang.h"
#include "ml_console.h"
#include "ml_compiler.h"
#include "ml_macros.h"
#include "ml_file.h"
#include "ml_object.h"
#include "ml_iterfns.h"
#include "stringmap.h"
#include <stdio.h>
#include <gc.h>

#include "string_store.h"
#include "string_index.h"

static stringmap_t Globals[1] = {STRINGMAP_INIT};

static ml_value_t *global_get(void *Data, const char *Name) {
	return stringmap_search(Globals, Name) ?: MLNil;
}

static ml_value_t *print(void *Data, int Count, ml_value_t **Args) {
	static ml_value_t *StringMethod = 0;
	if (!StringMethod) StringMethod = ml_method("string");
	for (int I = 0; I < Count; ++I) {
		ml_value_t *Result = Args[I];
		if (Result->Type != MLStringT) {
			Result = ml_call(StringMethod, 1, &Result);
			if (Result->Type == MLErrorT) return Result;
			if (Result->Type != MLStringT) return ml_error("ResultError", "string method did not return string");
		}
		fwrite(ml_string_value(Result), 1, ml_string_length(Result), stdout);
	}
	fflush(stdout);
	return MLNil;
}

static ml_value_t *error(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(2);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ML_CHECK_ARG_TYPE(1, MLStringT);
	return ml_error(ml_string_value(Args[0]), "%s", ml_string_value(Args[1]));
}

static ml_value_t *debug(void *Data, int Count, ml_value_t **Args) {
	if (Count > 0 && Args[0] == MLNil) {
		MLDebugClosures = 0;
	} else {
		MLDebugClosures = 1;
	}
	return MLNil;
}

typedef struct ml_string_store_t {
	const ml_type_t *Type;
	string_store_t *Handle;
} ml_string_store_t;

static ml_type_t *StringStoreT;

static ml_value_t *ml_string_store_open(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(1);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ml_string_store_t *Store = new(ml_string_store_t);
	Store->Type = StringStoreT;
	Store->Handle = string_store_open(ml_string_value(Args[0]));
	if (!Store->Handle) return ml_error("StoreError", "Error opening string store");
	return (ml_value_t *)Store;
}

static ml_value_t *ml_string_store_create(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(2);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ML_CHECK_ARG_TYPE(1, MLIntegerT);
	ml_string_store_t *Store = new(ml_string_store_t);
	Store->Type = StringStoreT;
	Store->Handle = string_store_create(ml_string_value(Args[0]), ml_integer_value(Args[1]));
	return (ml_value_t *)Store;
}

static ml_value_t *ml_string_store_alloc(void *Data, int Count, ml_value_t **Args) {
	ml_string_store_t *Store = (ml_string_store_t *)Args[0];
	return ml_integer(string_store_alloc(Store->Handle));
}

static ml_value_t *ml_string_store_get(void *Data, int Count, ml_value_t **Args) {
	ml_string_store_t *Store = (ml_string_store_t *)Args[0];
	size_t Index = ml_integer_value(Args[1]);
	size_t Length = string_store_get_size(Store->Handle, Index);
	char *Value = snew(Length);
	string_store_get_value(Store->Handle, Index, Value);
	Value[Length] = 0;
	return ml_string(Value, Length);
}

static ml_value_t *ml_string_store_set(void *Data, int Count, ml_value_t **Args) {
	ml_string_store_t *Store = (ml_string_store_t *)Args[0];
	size_t Index = ml_integer_value(Args[1]);
	string_store_set(Store->Handle, Index, ml_string_value(Args[2]), ml_string_length(Args[2]));
	return Args[2];
}

typedef struct ml_string_index_t {
	const ml_type_t *Type;
	string_index_t *Handle;
} ml_string_index_t;

static ml_type_t *StringIndexT;

static ml_value_t *ml_string_index_open(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(1);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ml_string_index_t *Store = new(ml_string_index_t);
	Store->Type = StringIndexT;
	Store->Handle = string_index_open(ml_string_value(Args[0]));
	if (!Store->Handle) return ml_error("StoreError", "Error opening string store");
	return (ml_value_t *)Store;
}

static ml_value_t *ml_string_index_create(void *Data, int Count, ml_value_t **Args) {
	ML_CHECK_ARG_COUNT(1);
	ML_CHECK_ARG_TYPE(0, MLStringT);
	ml_string_index_t *Store = new(ml_string_index_t);
	Store->Type = StringIndexT;
	Store->Handle = string_index_create(ml_string_value(Args[0]));
	return (ml_value_t *)Store;
}

static ml_value_t *ml_string_index_insert(void *Data, int Count, ml_value_t **Args) {
	ml_string_index_t *Store = (ml_string_index_t *)Args[0];
	size_t Index = string_index_insert(Store->Handle, ml_string_value(Args[1]));
	return ml_integer(Index);
}

static ml_value_t *ml_string_index_search(void *Data, int Count, ml_value_t **Args) {
	ml_string_index_t *Store = (ml_string_index_t *)Args[0];
	size_t Index = string_index_search(Store->Handle, ml_string_value(Args[1]));
	return ml_integer(Index);
}

int main(int Argc, const char *Argv[]) {
	ml_init();
	ml_file_init(Globals);
	ml_object_init(Globals);
	ml_iterfns_init(Globals);
	stringmap_insert(Globals, "print", ml_function(0, print));
	stringmap_insert(Globals, "error", ml_function(0, error));
	stringmap_insert(Globals, "debug", ml_function(0, debug));

	StringStoreT = ml_type(MLAnyT, "string-store");
	stringmap_insert(Globals, "string_store_open", ml_function(0, ml_string_store_open));
	stringmap_insert(Globals, "string_store_create", ml_function(0, ml_string_store_create));
	ml_method_by_name("alloc", 0, ml_string_store_alloc, StringStoreT, NULL);
	ml_method_by_name("get", 0, ml_string_store_get, StringStoreT, MLIntegerT, NULL);
	ml_method_by_name("set", 0, ml_string_store_set, StringStoreT, MLIntegerT, MLStringT, NULL);

	StringIndexT = ml_type(MLAnyT, "string-index");
	stringmap_insert(Globals, "string_index_open", ml_function(0, ml_string_index_open));
	stringmap_insert(Globals, "string_index_create", ml_function(0, ml_string_index_create));
	ml_method_by_name("insert", 0, ml_string_index_insert, StringIndexT, MLStringT, NULL);
	ml_method_by_name("search", 0, ml_string_index_search, StringIndexT, MLStringT, NULL);

	const char *FileName = 0;
	for (int I = 1; I < Argc; ++I) {
		if (Argv[I][0] == '-') {
			switch (Argv[I][1]) {
			case 'D': MLDebugClosures = 1; break;
			}
		} else {
			FileName = Argv[I];
		}
	}
	if (FileName) {
		ml_value_t *Closure = ml_load(global_get, 0, FileName);
		if (Closure->Type == MLErrorT) {
			printf("Error: %s\n", ml_error_message(Closure));
			const char *Source;
			int Line;
			for (int I = 0; ml_error_trace(Closure, I, &Source, &Line); ++I) printf("\t%s:%d\n", Source, Line);
			return 1;
		}
		ml_value_t *Result = ml_call(Closure, 0, NULL);
		if (Result->Type == MLErrorT) {
			printf("Error: %s\n", ml_error_message(Result));
			const char *Source;
			int Line;
			for (int I = 0; ml_error_trace(Result, I, &Source, &Line); ++I) printf("\t%s:%d\n", Source, Line);
			return 1;
		}
	} else {
		ml_console(global_get, Globals);
	}
	return 0;
}
