#ifndef QUEUE_H
#define QUEUE_H

typedef struct queue_root queue_root;

typedef struct queue_head queue_head;

struct queue_head {
	queue_head *next;
	void *task;
};

queue_root *ALLOC_QUEUE_ROOT();
void INIT_QUEUE_HEAD(queue_head *head, void *task);

void queue_put(queue_head *new, queue_root *root);

queue_head *queue_get(queue_root *root);

#endif // QUEUE_H
