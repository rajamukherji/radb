.PHONY: clean all install

PLATFORM = $(shell uname)
MACHINE = $(shell uname -m)

all: libradb.a

*.o: *.h

CFLAGS += -std=gnu99 -fstrict-aliasing -Wstrict-aliasing -Wall \
	-I. -DGC_THREADS -D_GNU_SOURCE -D$(PLATFORM)

ifdef DEBUG
	CFLAGS += -g -DGC_DEBUG -DDEBUG
else
	CFLAGS += -O3 -g -momit-leaf-frame-pointer -foptimize-sibling-calls -fno-stack-protector -march=native -mtune=native -mno-sse2 -minline-all-stringops
endif

ifeq ($(RADB_MEM), MALLOC)
config.h: config.h.in
	sed 's/RADB_MEM_MODE/RADB_MEM_MALLOC/g' config.h.in > config.h
else ifeq ($(RADB_MEM), GC)
config.h: config.h.in
	sed 's/RADB_MEM_MODE/RADB_MEM_GC/g' config.h.in > config.h
else
config.h: config.h.in
	sed 's/RADB_MEM_MODE/RADB_MEM_PER_STORE/g' config.h.in > config.h
endif

common_objects = string.o fixed.o common.o linear_index.o string_index2.o fixed_index2.o linear_index0.o string_index0.o

platform_objects =

ifeq ($(MACHINE), i686)
	CFLAGS += -fno-pic
endif

ifeq ($(PLATFORM), Linux)
	platform_objects +=
	CFLAGS += -fcf-protection=none -mno-align-stringops -fno-tree-loop-distribute-patterns
endif

ifeq ($(PLATFORM), FreeBSD)
	platform_objects += 
	CFLAGS += -I/usr/local/include
endif

ifeq ($(PLATFORM), Darwin)
	platform_objects += 
endif

$(common_objects): config.h

libradb.a: $(common_objects) $(platform_objects)
	ar rcs $@ $(common_objects) $(platform_objects)

clean:
	rm -f config.h
	rm -f *.o
	rm -f libradb.a

PREFIX = /usr
install_include = $(DESTDIR)$(PREFIX)/include/radb
install_lib = $(DESTDIR)$(PREFIX)/lib

install_h = \
	$(install_include)/radb.h \
	$(install_include)/config.h \
	$(install_include)/common.h \
	$(install_include)/string_store.h \
	$(install_include)/string_index.h \
	$(install_include)/fixed_store.h \
	$(install_include)/fixed_index.h \
	$(install_include)/linear_index.h \
	$(install_include)/string_index2.h \
	$(install_include)/fixed_index2.h \
	$(install_include)/linear_index0.h \
	$(install_include)/string_index0.h

install_a = $(install_lib)/libradb.a

$(install_h): $(install_include)/%: %
	mkdir -p $(install_include)
	cp $< $@

$(install_a): $(install_lib)/%: %
	mkdir -p $(install_lib)
	cp $< $@

install: $(install_h) $(install_a)
