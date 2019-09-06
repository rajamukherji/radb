.PHONY: clean all install

PLATFORM = $(shell uname)
MACHINE = $(shell uname -m)

all: libradb.a radb

*.o: *.h

CFLAGS += -std=gnu99 -fstrict-aliasing -Wstrict-aliasing -Wall \
	-I. -I../minilang -pthread -DGC_THREADS -D_GNU_SOURCE -D$(PLATFORM)
LDFLAGS += -lm -L. -L../minilang

ifdef DEBUG
	CFLAGS += -g -DGC_DEBUG -DDEBUG
	LDFLAGS += -g
else
	CFLAGS += -O3 -g
	LDFLAGS += -g
endif

common_objects = \
	string_store.o \
	string_index.o

platform_objects =

ifeq ($(PLATFORM), Linux)
	platform_objects += 
	LDFLAGS += -lgc
endif

ifeq ($(PLATFORM), FreeBSD)
	platform_objects += 
	CFLAGS += -I/usr/local/include
	LDFLAGS += -L/usr/local/lib -lgc-threaded
endif

ifeq ($(PLATFORM), Darwin)
	platform_objects += 
	LDFLAGS += -lgc
endif

libradb.a: $(common_objects) $(platform_objects)
	ar rcs $@ $(common_objects) $(platform_objects)

radb: Makefile *.h radb.o libradb.a ../minilang/libminilang.a
	$(CC) radb.o $(LDFLAGS) -o$@ -lradb -lminilang

clean:
	rm -f *.o
	rm -f libradb.a

PREFIX = /usr
install_include = $(DESTDIR)$(PREFIX)/include/radb
install_lib = $(DESTDIR)$(PREFIX)/lib

install_h = \
	$(install_include)/radb.h

install_a = $(install_lib)/libradb.a

$(install_h): $(install_include)/%: %
	mkdir -p $(install_include)
	cp $< $@

$(install_a): $(install_lib)/%: %
	mkdir -p $(install_lib)
	cp $< $@

install: $(install_h) $(install_a)