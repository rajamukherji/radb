RADB
====

Introduction
------------

There are already numerous embedded key-value stores available, where the keys are null-terminated strings or arbitrary byte sequences. *RADB* (*Raja's Attempt at a Database*) is a small *C* library that for using integer-indexed value stores with either fixed or variable length data.

In addition *RADB* provides lookup tables for keys of either fixed or variable length, returning integer indices which can be used with the integer-indexed value stores (if desired).


Building
========

.. note::

   Currently *RADB* has been tested on *Linux*, *FreeBSD* and *MacOS*. It has no dependencies other than :c:`mmap()`. Being developed primarily on *Linux*, there may be some unintentional dependencies on *Glibc*, patches are welcome.

.. code-block:: console

    $ git clone https://github.com/rajamukherji/radb
    $ cd radb
    $ make [RADB_MEM=<MALLOC | GC>]
    $ make install [PREFIX=<install path>]


API
===

In order to be as versitile as possible, *RADB* support different memory management configurations when built. Building with ``make RADB_MEM=MALLOC`` or ``make RADB_MEM=GC`` will use the system :c:`malloc()` or :c:`GC_malloc()` from the Hans-Boehm garbage collector respectively. Omitting the ``RADB_MEM`` flag when building defaults to the user specified memory allocator per store. The :c:macro:`RADB_MEM_PARAMS` is defined accordingly:

.. c:macro:: RADB_MEM_PARAMS

   This macro is empty if ``RADB_MEM`` is defined as ``MALLOC`` or ``GC`` when *RADB* is built. Otherwise it defines the following additional parameters in functions for creating or opening any store or index. 

   :c:`void *Allocator`
      User-data for the allocator.
   :c:`void *(*alloc)(void *Allocator, size_t Size)`
      Allocate :c:`Size` bytes of memory with the allocator.
   :c:`void *(*alloc_atomic)(void *Allocator, size_t Size)`
      Allocate :c:`Size` bytes of pointer-free memory with the allocator.
   :c:`void (*free)(void *Allocator, void *Ptr)`
      Free memory at :c:`Ptr` from the allocator.


Stores
------

*RADB* stores allow values to be stored and retrieved using integer indices. Every index (currently limited to 2\ :sup:`32` - 2) is considered valid, stores will grow automatically when writing to an index beyond the current store size and return an empty value (for string stores) when reading beyond the current store size. This means users are free to decide how to allocate indices in a store. For convenience, stores also provide a chain based index allocator to maintain a free chain of indices.

.. warning::
   The free index chain must not be used with user allocated indices in the same store as the chain is maintained in the value storage and will be corrupted if a value is set without being allocated from the chain.
   

String Store
~~~~~~~~~~~~

String stores can contain arbitrary byte sequences (including null bytes). The values are split into blocks, the size of each block is set when the store is created. Each string store consists of two files: an *entries* file containing the length and initial block of each value in the store and a *data* file containing the blocks. Both files are created relative to the prefix path provided when a string store is created.

.. c:struct:: string_store_t

   A string store.

.. c:function:: string_store_t *string_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS)

   Creates a new string store.
   
   :return: An open string store.
   
   :param Prefix: The prefix path for the store.
   :param RequestedSize: The requested size for each block (rounded up to the nearest power of 2).
   :param ChunkSize: The additional number of bytes to allocate each time the entries or data file needs to grow (rounded up to the nearest multiple of 512).
   :param RADB_MEM_PARAMS: Additional parameters if per-store memory allocation is enabled, see :c:macro:`RADB_MEM_PARAMS`.

.. c:function:: string_store_t *string_store_open(const char *Prefix RADB_MEM_PARAMS)

   Opens an existing string store. Returns :c:`NULL` if an error occurs.
   
   :return: An open string store.
   
   :param Prefix: The prefix path for the store.
   
.. c:function:: void string_store_close(string_store_t *Store)

   Closes an open string store.
   
   :param Store: An open string store.

.. c:function:: size_t string_store_num_entries(string_store_t *Store)

   :return: The number of entries allocated in the store. Note that this is an upper bound and is usually larger than the actual number of entries set in the store.
   
   :param Store: An open string store.

.. c:function:: size_t string_store_size(string_store_t *Store, size_t Index)

   :return: The size of the :c:`Index`-th value. If no value has been set at :c:`Index` then :c:`0` is returned.
   
    :param Store: An open string store.
    :param Index: A valid index.

.. c:function:: size_t string_store_get(string_store_t *Store, size_t Index, void *Buffer, size_t Space)

   Gets at most :c:`Space` bytes from the :c:`Index`-th value into :c:`Buffer`.
   
   :return: The number of bytes copied.
   
   :param Store: An open string store.
   :param Index: A valid index.
   :param Buffer: A buffer of at least :c:`Space` bytes.
   :param Space: The number of bytes to get.

.. c:function:: void string_store_set(string_store_t *Store, size_t Index, const void *Buffer, size_t Length)

   Sets the :c:`Index`-th value to contents of :c:`Buffer`.
   
   :param Store: An open string store.
   :param Index: A valid index.
   :param Buffer: A buffer of at least :c:`Length` bytes.
   :param Length: The number of bytes to set.

.. c:function:: int string_store_compare(string_store_t *Store, const void *Other, size_t Length, size_t Index)

   Convenience function to compare (lexicographically) the :c:`Index`-th value to another value without copying.
   
   :return:
      * -1 if :c:`Other` occurs before the :c:`Index`-th value.
      * 1 if :c:`Other` occurs after the :c:`Index`-th value.
      * 0 if :c:`Other` is the same as the :c:`Index`-th value.
   
   :param Store: An open string store.
   :param Other: A buffer of at least :c:`Length` bytes.
   :param Length: The number of bytes to compare (at most).
   :param Index: A valid index.


.. c:function:: int string_store_compare2(string_store_t *Store, size_t Index1, size_t Index2)

   Convenience function to compare (lexicographically) the :c:`Index1`-th value to the :c:`Index2`-th value without copying.
   
   :return:
      * -1 if the :c:`Index1`-th value occurs before the :c:`Index`2-th value.
      * 1 if the :c:`Index1`-th value occurs before the :c:`Index`2-th value.
      * 0 if the :c:`Index1`-th value is the same as the :c:`Index`2-th value.
   
   :param Store: An open string store.
   :param Index1: A valid index.
   :param Index2: A valid index.

.. c:function:: size_t string_store_alloc(string_store_t *Store)

   Allocates an index from the free index chain.
   
   :return: A valid index
   
   :param Store: An open string store.

.. c:function:: void string_store_free(string_store_t *Store, size_t Index)

   Returns an index to the free index chain.
   
   :param Store: An open string store.
   :param Index: A valid index (from a previous call to :c:func:`string_store_alloc()`).

.. c:struct:: string_store_writer_t

   For writing to a value in a string store in a stream.

.. c:function:: void string_store_writer_open(string_store_writer_t *Writer, string_store_t *Store, size_t Index)

   Prepares a :c:struct:`string_store_writer_t` for writing to the :c:`Index`-th value. There is no need to close a writer. Writers may be reused by calling this function again.
   
   :param Writer: A writer.
   :param Store: An open string store.
   :param Index: A valid index.

.. c:function:: size_t string_store_writer_write(string_store_writer_t *Writer, const void *Buffer, size_t Length)

   Writes bytes from :c:`Buffer` to the selected value in the store.

   :return: The number of bytes written, which will always be :c:`Length` (excluding errors).

   :param Writer: An open writer.
   :param Buffer: A buffer of at least :c:`Length` bytes.
   :param Length: The number of bytes to write.

.. c:struct:: string_store_reader_t

   For reading the bytes of a value in a string store in a stream.

.. c:function:: void string_store_reader_open(string_store_reader_t *Reader, string_store_t *Store, size_t Index)

   Prepares a :c:struct:`string_store_reader_t` for reading from the :c:`Index`-th value. There is no need to close a reader. Readers may be reused by calling this function again.

.. c:function:: size_t string_store_reader_read(string_store_reader_t *Reader, void *Buffer, size_t Length)

   Reads bytes from the selected valuei in the store into :c:`Buffer`.
   
   :return: The number of bytes read. May be 0 if the end of the value has been reached.
   
   :param Reader: An open reader.
   :param Buffer: A buffer of at least :c:`Length` bytes.
   :param Length: The number of bytes to read (at most). 


Fixed Store
~~~~~~~~~~~

.. c:struct:: fixed_store_t

.. c:function:: fixed_store_t *fixed_store_create(const char *Prefix, size_t RequestedSize, size_t ChunkSize RADB_MEM_PARAMS)

.. c:function:: fixed_store_t *fixed_store_open(const char *Prefix RADB_MEM_PARAMS)

.. c:function:: void fixed_store_close(fixed_store_t *Store)

.. c:function:: size_t fixed_store_num_entries(fixed_store_t *Store)

.. c:function:: void *fixed_store_get(fixed_store_t *Store, size_t Index)

.. c:function:: size_t fixed_store_alloc(fixed_store_t *Store)

.. c:function:: void fixed_store_free(fixed_store_t *Store, size_t Index)

Indices
-------

String Index
~~~~~~~~~~~~

.. c:struct:: string_index_t

.. c:function:: string_index_t *string_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS)

.. c:function:: string_index_t *string_index_open(const char *Prefix RADB_MEM_PARAMS)

.. c:function:: size_t string_index_count(string_index_t *Store)

.. c:function:: void string_index_close(string_index_t *Store)

.. c:function:: size_t string_index_insert(string_index_t *Store, const char *Key, size_t Length)

.. c:function:: size_t string_index_search(string_index_t *Store, const char *Key, size_t Length)

.. c:struct:: string_index_result_t

   .. c:member:: size_t Index
   .. c:member:: int Created

.. c:function:: string_index_result_t string_index_insert2(string_index_t *Store, const char *Key, size_t Length)

.. c:function:: size_t string_index_size(string_index_t *Store, size_t Index)

.. c:function:: size_t string_index_get(string_index_t *Store, size_t Index, void *Buffer, size_t Space)

.. c:function:: size_t string_index_delete(string_index_t *Store, const char *Key, size_t Length)

Fixed Index
~~~~~~~~~~~

.. c:struct:: fixed_index_t

.. c:function:: fixed_index_t *fixed_index_create(const char *Prefix, size_t KeySize, size_t ChunkSize RADB_MEM_PARAMS)

.. c:function:: fixed_index_t *fixed_index_open(const char *Prefix RADB_MEM_PARAMS)

.. c:function:: size_t fixed_index_count(fixed_index_t *Store)

.. c:function:: void fixed_index_close(fixed_index_t *Store)

.. c:function:: size_t fixed_index_insert(fixed_index_t *Store, const char *Key)

.. c:function:: size_t fixed_index_search(fixed_index_t *Store, const char *Key)

.. c:struct:: fixed_index_result_t

   .. c:member:: size_t Index
   .. c:member:: int Created

.. c:function:: fixed_index_result_t fixed_index_insert2(fixed_index_t *Store, const char *Key)

.. c:function:: const void *fixed_index_get(fixed_index_t *Store, size_t Index)

.. c:function:: size_t fixed_index_delete(fixed_index_t *Store, const char *Key)

Index
=====

.. toctree::
   :maxdepth: 2
   
   /index
   /internals
