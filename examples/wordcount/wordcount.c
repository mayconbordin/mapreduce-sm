#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>

#include "mapreduce.h"

#define TASK_SIZE 50

typedef struct {
    void *data;
    int index;
    int length;
} task;

char* strtoupper(char *str)
{
    char *p = (char*)str;
    for ( ; *p; ++p) *p = toupper(*p);
    return str;
}

void splitter(void *data, int size, queue_root *queue, void (*buffer_put)(queue_root *queue, void *data))
{
    unsigned int index  = 0;
    unsigned int length = 0;
    bool newline = true;
    char *ptr = data;
    char *stop = data + size;
    task *t = NULL;
    
    for (; ptr < stop; ptr++, length++)
    {
        if (index % TASK_SIZE == 0 && newline == true)
        {
            if (t != NULL) {
                t->length = length;
                buffer_put(queue, (void*)t);
            }
            
            t = (task*) malloc(sizeof(task));
            t->data  = ptr;
            t->index = index;
            length   = 0;
            newline  = false;
        }
        
        if(*ptr == '\n') {
            newline = true;
            ++index;
        }
    }
}

void *map(void *_data, map_context *ctx)
{
    task *t = (task*) _data;
    char *data = (char*) t->data;
    
    char ch;
    char *word;
    int state = 0;
    int i;
    static const int count_one = 1;
    
    for (i=0; i<t->length; i++) {
        ch = toupper(data[i]);
        
        if (state == 1) {
            if ((ch < 'A' || ch > 'Z') && ch != '\'') {
                
                data[i] = '\0';
                ctx->write((void*)strtoupper(word), (void*)&count_one, ctx);
            
                state = 0;
            }
        }
        
        else {
            if (ch >= 'A' && ch <= 'Z') {
                word = &data[i];
                state  = 1;
            }
        }
    }
    
    if (state == 1) {
        data[i] = '\0';
        ctx->write((void*)strtoupper(word), (void*)&count_one, ctx);
    }
}

void *reduce(void *key, void **val, reduce_context *ctx)
{
    int  **v  = (int**) val;
    int *sum = (int*) malloc(sizeof(int));
    *sum = 0;
    
    for (int i=0; i<ctx->value_size; i++)
        *sum += *v[i];
        
    ctx->write(key, (void*) sum, ctx);
}

int main(int argc, char *argv[])
{
    if (argc != 5) {
        printf("Usage: wordcount num_threads task_size input_file output_file\n");
        exit(1);
    }
    
    mapreduce_opts opts;
    mapreduce_default_opts(&opts);
    
    opts.nthreads      = atoi(argv[1]);
    opts.task_size     = atoi(argv[2]);
    opts.input_file    = argv[3];
    opts.output_file   = argv[4];
    opts.map           = &map;
    opts.reduce        = &reduce;
    opts.splitter      = &splitter;
    opts.sort.function = &val_int_comparator;
    opts.sort.order    = SORT_DESC;
    
    mapreduce_init(&opts);

}
