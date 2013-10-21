#ifndef _MAPREDUCE_H
#define _MAPREDUCE_H

#include <pthread.h>
#include <glib.h>
#include "uthash.h"
#include "queue.h"

#define SORT_ASC   1
#define SORT_DESC -1

unsigned long str_hash(unsigned char *str);

// Array
typedef struct {
  void **array;
  size_t used;
  size_t size;
} Array;

void init_array(Array *a, size_t initial_size);
void insert_array(Array *a, void *element);
void delete_array(Array *a, void *element);
void free_array(Array *a);


// Buffer
typedef struct {
    void *key;
    void *val;
} node;

// Key/value hash
typedef struct {
    char *key;
    Array values;
    UT_hash_handle hh;
} kvs_hash;

kvs_hash *find_map_key(kvs_hash *hash, char *key);

typedef struct {
    kvs_hash        **hash;
    pthread_mutex_t *lock;
} hash;

void hash_init(hash *h, int nthreads);

typedef struct {
    GSList *list;
} list;

// MapReduce structures
typedef struct map_context map_context;
struct map_context {
    hash *hash;
    int nthreads;

    void* (*write)(void *key, void *val, map_context *ctx);
};

typedef struct reduce_context reduce_context;
struct reduce_context {
    int value_size;
    list *list;
    void* (*write)(void *key, void *val, reduce_context *ctx);
};

typedef struct {
    queue_root *queue;
    int done;
} kv_buffer;

typedef struct {
    int (*function)(void *a, void *b);
    int order;
} sort_args;

typedef struct {
    char *input_file;
    int task_size;
    kv_buffer *buffer;
    void (*splitter)(void *data, int size, queue_root *queue, void (*buffer_put)(queue_root *queue, void *data));
} filereader_args;

typedef struct {
    int tid;
    int nthreads;
    int task_size;

    pthread_barrier_t *barrier;
    
    kv_buffer *buffer;
    hash   *hash;
    
    list list;
    
    sort_args *sort;
    
    void* (*map)(void *data, map_context *ctx);
    void* (*reduce)(void *key, void **val, reduce_context *ctx);
    
    void* (*map_writer)(void *key, void *val, map_context *ctx);
    void* (*reduce_writer)(void *key, void *val, reduce_context *ctx);
} worker_args;

typedef struct {
    int  nthreads;
    int  task_size;
    
    char *input_file;
    char *output_file;
    
    sort_args sort;
    
    void* (*map)(void *data, map_context *ctx);
    void* (*reduce)(void *key, void **val, reduce_context *ctx);
    
    void  (*output_writer)(FILE *out, void *key, void *val);
    
    void (*splitter)(void *data, int size, queue_root *queue, void (*buffer_put)(queue_root *queue, void *data));
} mapreduce_opts;

// MapReduce Functions
void *worker(void *_args);
void *mapper(void *_args);
void *mapper_writer(void *key, void *val, map_context *ctx);
void *reducer(void *_args);
void *reducer_writer(void *key, void *val, reduce_context *ctx);

void mapreduce_init(mapreduce_opts *opts);
void mapreduce_default_opts(mapreduce_opts *opts);

// Comparators
int key_str_comparator(void *a, void *b);
int key_int_comparator(void *a, void *b);
int val_int_comparator(void *a, void *b);

// Reducers
void *identity_reducer(void *key, void **val, reduce_context *ctx);

#endif
