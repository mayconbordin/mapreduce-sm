#include <sys/stat.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "mapreduce.h"

#ifdef WALL_TIME
#include <sys/time.h>

/* Return 1 if the difference is negative, otherwise 0.  */
int timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1)
{
    long int diff = (t2->tv_usec + 1000000 * t2->tv_sec) - (t1->tv_usec + 1000000 * t1->tv_sec);
    result->tv_sec = diff / 1000000;
    result->tv_usec = diff % 1000000;

    return (diff<0);
}

struct timeval tvBegin, tvEnd, tvDiff;
#endif

#ifdef CYCLE_TIME
uint64_t rdtsc() {
  uint32_t lo, hi;
  __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
  return (uint64_t)hi << 32 | lo;
}

uint64_t tickStart, tickEnd;

uint64_t *lockWaitTotal, *lockWaitStart, *lockWaitEnd;
#endif

unsigned long str_hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

// Comparators
// -----------------------------------------------------------------------------
gint comparator_wrapper(gconstpointer a, gconstpointer b, gpointer s) {
    sort_args *sort = (sort_args*) s;

    int result = sort->function((void*)a, (void*)b);
    
    return (sort->order == SORT_ASC) ? result : -result;
}

int key_str_comparator(void *a, void *b) {
    return strcmp(((node*)a)->key, ((node*)b)->key);
}

int key_int_comparator(void *a, void *b) {
    int *a_key = ((node*)a)->key;
    int *b_key = ((node*)b)->key;

    return (*a_key > *b_key) ? 1 : (*a_key < *b_key) ? -1 : 0;
}

int val_int_comparator(void *a, void *b) {
    int *a_val = ((node*)a)->val;
    int *b_val = ((node*)b)->val;

    return (*a_val > *b_val) ? 1 : (*a_val < *b_val) ? -1 : 0;
}

#define SWAP_PTRS(a, b) do { void *t = (a); (a) = (b); (b) = t; } while (0)

GSList* merge_lists(GSList* list1, GSList* list2, sort_args *sort) 
{
  GSList *list = NULL, **pnext = &list;

  if (list2 == NULL)
    return list1;

  while (list1 != NULL)
  {
    if (comparator_wrapper(list1->data, list2->data, sort) > 0)
      SWAP_PTRS(list1, list2);

    *pnext = list1;
    pnext = &list1->next;
    list1 = *pnext;
  }

  *pnext = list2;
  return list;
}


// Array
// -----------------------------------------------------------------------------
void init_array(Array *a, size_t initial_size) {
  a->array = (void*) malloc(initial_size * sizeof(void*));
  a->used = 0;
  a->size = initial_size;
}

void insert_array(Array *a, void *element) {
  if (a->used == a->size) {
    a->size *= 2;
    a->array = (void *) realloc(a->array, a->size * sizeof(void*));
  }
  a->array[a->used++] = element;
}

void free_array(Array *a) {
  free(a->array);
  a->array = NULL;
  a->used = a->size = 0;
}

void delete_array(Array *a, void *element) {
    for (int i=0; i<a->used; i++) {
        if (a->array[i] == element)
            while (i < a->used)
                a->array[i] = a->array[++i];
    }
    
    a->used--;
}

// HashTable definition
// -----------------------------------------------------------------------------
kvs_hash *find_map_key(kvs_hash *hash, char *key)
{
    kvs_hash *s;
    HASH_FIND_STR(hash, key, s);
    return s;
}

void hash_init(hash *h, int nthreads)
{
    h->hash = (kvs_hash**) malloc(nthreads * sizeof(kvs_hash*));
    h->lock = (pthread_mutex_t*) malloc(nthreads * sizeof(pthread_mutex_t));
    for (int i=0; i<nthreads; i++) {
        pthread_mutex_init(&h->lock[i], NULL);
        h->hash[i] = NULL;
    }
}

// Buffer
//
void buffer_put(queue_root *queue, void *data)
{
    queue_head *item = malloc(sizeof(queue_head));
    INIT_QUEUE_HEAD(item, data);
    queue_put(item, queue);
}


// -----------------------------------------------------------------------------

void *file_reader(void *_args)
{
    FILE *fp;
    char *line;
    size_t len = 0;
    ssize_t read;
    int line_count = 0;
    int line_size;
    
    int file_descriptor;
    struct stat file_info;
    char *data;
    
    filereader_args *args = (filereader_args*) _args;
    
    int task_count = 0;
    int task_size = args->task_size * 2;
    
#ifdef PRINT_INFO
    printf("File Reader started\n");
#endif

    // Open the input file in read-only mode
    file_descriptor = open(args->input_file, O_RDONLY);
    if (file_descriptor < 0)
    {
        printf("open %s failed: %s\n", args->input_file, strerror(errno));
        exit(EXIT_FAILURE);
    }
    
    // Get the info about the file
    if (fstat(file_descriptor, &file_info) < 0)
    {
        printf("fstat %s failed: %s\n", args->input_file, strerror(errno));
        exit(EXIT_FAILURE);
    }
    
    // Memory map the file
    data = mmap(NULL, file_info.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE, file_descriptor, 0);
    if (data == MAP_FAILED)
    {
        printf("mmap %s failed: %s\n", args->input_file, strerror (errno));
        exit(EXIT_FAILURE);
    }
    
    args->splitter(data, file_info.st_size, args->buffer->queue, &buffer_put);
    
    close(file_descriptor);
    
    // done reading the input
    args->buffer->done = 1;
    
#ifdef PRINT_INFO
    printf("File Reader finished\n");
#endif
}


void *worker(void *_args)
{
    worker_args *args = (worker_args*) _args;
    
#ifdef PRINT_INFO
    printf("Worker %d started\n", args->tid);
#endif
    
    mapper(_args);
    
    // Wait for all map work to be finished
    int rc = pthread_barrier_wait(args->barrier);
    if(rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        printf("Could not wait on barrier\n");
        exit(-1);
    }

    reducer(_args);
}

void *mapper(void *_args)
{
    worker_args *args = (worker_args*) _args;
    
    queue_head *item;
    
#ifdef PRINT_INFO
    printf("Mapper %d started\n", args->tid);
#endif

    map_context    ctx;
    ctx.write    = args->map_writer;
    ctx.hash     = args->hash;
    ctx.nthreads = args->nthreads;
    
    while (1)
    {
        item = queue_get(args->buffer->queue);
        if (item) {
            args->map(item->task, &ctx);
            free(item);
        }
        
        else if (args->buffer->done == 1) {
            break;
        }
    }
    
#ifdef PRINT_INFO
    printf("Mapper %d finished\n", args->tid);
#endif
}

void *mapper_writer(void *key, void *val, map_context *ctx)
{
    kvs_hash *s;
    unsigned long h = str_hash((char*)key);
    int p = h % ctx->nthreads;
    
    //s = find_map_key(ctx->hash->hash[p], (char*)key);
    
    //HASH_FIND_STR(ctx->hash->hash[p], (char*)key, s);
    
    pthread_mutex_lock(&ctx->hash->lock[p]);
    s = find_map_key(ctx->hash->hash[p], (char*)key);
    
    if (s == NULL) {
        s = (kvs_hash*) malloc(sizeof(kvs_hash));
        s->key = key;
        
        init_array(&s->values, 5);
        insert_array(&s->values, val);
        
        HASH_ADD_KEYPTR(hh, ctx->hash->hash[p], s->key, strlen(s->key), s);
    } else {
        insert_array(&s->values, val);
    }
    
    pthread_mutex_unlock(&ctx->hash->lock[p]);
}

void *reducer(void *_args)
{
    worker_args *args = (worker_args*) _args;
    
#ifdef PRINT_INFO
    printf("Reducer %d started\n", args->tid);
#endif

    
    reduce_context ctx;
    ctx.write = args->reduce_writer;
    ctx.list  = &args->list;
    
    kvs_hash *s;
    kvs_hash *hash = args->hash->hash[args->tid];
    
    while (1)
    {
        s = hash;
        if (s == NULL) {
            break;
        }
        
        hash = s->hh.next;

        ctx.value_size = s->values.used;
        args->reduce(s->key, s->values.array, &ctx);
        
        free(s);
    }
    
    args->list.list = g_slist_sort_with_data(args->list.list, comparator_wrapper, (gpointer)args->sort);
    
#ifdef PRINT_INFO
    printf("Reducer %d finished\n", args->tid);
#endif
}

void *reducer_writer(void *key, void *val, reduce_context *ctx)
{
    node *new = (node*) malloc(sizeof(node));
    new->key = key;
    new->val = val;
    
    ctx->list->list = g_slist_prepend(ctx->list->list, new);
}

void str_int_output_writer(FILE *out, void *key, void *val)
{
    fprintf(out, "%s\t%d\n", (char*)key, *((int*)val));
}


void *identity_reducer(void *key, void **val, reduce_context *ctx)
{
    for (int i=0; i<ctx->value_size; i++)
        ctx->write(key, val[i], ctx);
}

void mapreduce_default_opts(mapreduce_opts *opts)
{
    opts->nthreads      = 8;
    opts->task_size     = 20;
    opts->reduce        = &identity_reducer;
    opts->output_writer = &str_int_output_writer;
    opts->sort.function = &key_str_comparator;
    opts->sort.order    = SORT_ASC;
}

void mapreduce_init(mapreduce_opts *opts)
{
#ifdef WALL_TIME
    gettimeofday(&tvBegin, NULL);
#endif

#ifdef CYCLE_TIME
    tickStart = rdtsc();
#endif

    g_thread_init(NULL);

    // set variables
    int nthreads  = opts->nthreads;
    int i;
                             
    pthread_t thread_filereader;
    pthread_t thread_worker[nthreads];
    pthread_barrier_t barrier;
                    
    filereader_args filereader_opts;
    worker_args     worker_opts[nthreads];

    hash   h;
    kv_buffer buffer;
    buffer.queue = ALLOC_QUEUE_ROOT();
    buffer.done  = 0;

    // initialization
    hash_init(&h, nthreads);
    
    pthread_barrier_init(&barrier, NULL, nthreads);
    
    // start the file reader threads
    filereader_opts.input_file = opts->input_file;
    filereader_opts.buffer     = &buffer;
    filereader_opts.task_size  = opts->task_size;
    filereader_opts.splitter   = opts->splitter;
    
    pthread_create(&thread_filereader, NULL, file_reader, (void*) &filereader_opts);
    


    // start the worker threads
    for (i=0; i<nthreads; i++)
    {
        worker_opts[i].tid           = i;
        worker_opts[i].nthreads      = nthreads;
        worker_opts[i].task_size     = opts->task_size;
        worker_opts[i].barrier       = &barrier;
        worker_opts[i].buffer        = &buffer;
        worker_opts[i].hash          = &h;
        worker_opts[i].list.list     = NULL;
        worker_opts[i].sort          = &opts->sort;
        worker_opts[i].map           = opts->map;
        worker_opts[i].reduce        = opts->reduce;
        worker_opts[i].map_writer    = mapper_writer;
        worker_opts[i].reduce_writer = reducer_writer;

        pthread_create(&thread_worker[i], NULL, worker, (void*) &worker_opts[i]);
    }
    
    // join all threads
    pthread_join(thread_filereader, NULL);
    
    for (i=0; i<nthreads; i++)
    {
        pthread_join(thread_worker[i], NULL);
    }
    
    // join the reduce outputs
    FILE *fp;
    GSList *head;
    node *pair;
    
#ifdef PRINT_INFO
    printf("Merging reduce output\n");
    
    int num_keys = 0;
#endif
    
    fp = fopen(opts->output_file, "w");
    
    if (fp == NULL) {
        printf("Error: unable to create result file.\n");
        exit(1);
    }
    
    head = worker_opts[0].list.list;

    for (int i=1; i<nthreads; i++)
        head = merge_lists(head, worker_opts[i].list.list, &opts->sort);
        
#ifdef PRINT_INFO
    printf("Merge finished\n");
    printf("Writing output\n");
#endif
    
    while (1) {
        if (head == NULL) break;
        
        pair = (node*)head->data;
        
        opts->output_writer(fp, pair->key, pair->val);
        
        //fprintf(fp, "%s\t%d\n", (char*)pair->key, *((int*)pair->val));
        head = head->next;
        
#ifdef PRINT_INFO
        num_keys++;
#endif
    }
    
    fclose(fp);
    
#ifdef PRINT_INFO
    printf("Number of keys: %d\n", num_keys);
    printf("MapReduce Finished\n");
#endif

#ifdef WALL_TIME
    gettimeofday(&tvEnd, NULL);
    timeval_subtract(&tvDiff, &tvEnd, &tvBegin);
    
    printf("%ld.%06ld\t", tvDiff.tv_sec, tvDiff.tv_usec);
#endif

#ifdef CYCLE_TIME
    tickEnd = rdtsc();
    
    printf("%llu\t", (tickEnd-tickStart));
#endif

#if defined(WALL_TIME) || defined(CYCLE_TIME)
    printf("\n");
#endif
}
