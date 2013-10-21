#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>

#include "mapreduce.h"

#define TASK_SIZE 20

char *word;

typedef struct {
    void *data;
    int index;
    int length;
} task;

char* strtolower(char *str)
{
    char *p = (char*)str;
    for ( ; *p; ++p) *p = tolower(*p);
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

int getnextline(char *in, int offset, int length, char **out)
{
    *out = in;
    int i = 0;
    
    for (i=offset; i<=length; i++)
    {
        if (in[i] == '\0' && i == offset) {
            return 0;
        }
        if (in[i] == '\n') {
            in[i] = '\0';
            break;
        }
    }
    
    return i;
}

void *map(void *_data, map_context *ctx)
{
    task *t = (task*) _data;
    char *ptr = (char*) t->data;
    char *stop = ptr + t->length;
    char *line = ptr;
    
    int line_count = t->index;
    int *key;
    
    for (; ptr < stop; ptr++)
    {
        if (*ptr == '\n')
        {
            *ptr = '\0';
            
            if (strcasestr(line, word) != NULL) {
                key = (int*) malloc(sizeof(int));
                *key = line_count;
                
                ctx->write((void*)key, (void*)line, ctx);
            }
            
            line = ++ptr;
        }
        
        line_count++;
    }
}

void output_writer(FILE *out, void *key, void *val)
{
    fprintf(out, "%d:%s\n", *((int*)key), (char*)val);
}

int main(int argc, char *argv[])
{
    if (argc != 6) {
        printf("Usage: %s num_threads task_size search_word input_folder output_file\n", argv[0]);
        exit(1);
    }
    
    mapreduce_opts opts;
    mapreduce_default_opts(&opts);
    
    opts.nthreads      = atoi(argv[1]);
    opts.task_size     = atoi(argv[2]);
    opts.input_file    = argv[4];
    opts.output_file   = argv[5];
    opts.map           = &map;
    opts.splitter      = &splitter;
    opts.sort.function = &key_int_comparator;
    opts.output_writer = &output_writer;
    
    word = argv[3];
    
    mapreduce_init(&opts);

}
