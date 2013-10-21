#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "queue.h"

#define QUEUE_POISON1 ((void*)0xCAFEBAB5)

struct queue_root {
	queue_head *in_queue;
	queue_head *out_queue;
//	pthread_spinlock_t lock;
	pthread_mutex_t lock;
};


#ifndef _cas
# define _cas(ptr, oldval, newval) \
         __sync_bool_compare_and_swap(ptr, oldval, newval)
#endif

queue_root *ALLOC_QUEUE_ROOT()
{
	queue_root *root = malloc(sizeof(struct queue_root));

//	pthread_spin_init(&root->lock, PTHREAD_PROCESS_PRIVATE);
	pthread_mutex_init(&root->lock, NULL);

	root->in_queue = NULL;
	root->out_queue = NULL;
	return root;
}

void INIT_QUEUE_HEAD(queue_head *head, void *task)
{
	head->next = QUEUE_POISON1;
	head->task = task;
}

void queue_put(queue_head *new, queue_root *root)
{
	while (1) {
		queue_head *in_queue = root->in_queue;
		new->next = in_queue;
		if (_cas(&root->in_queue, in_queue, new)) {
			break;
		}
	}
}

queue_head *queue_get(queue_root *root)
{

	pthread_mutex_lock(&root->lock);

	if (!root->out_queue) {
		while (1) {
			queue_head *head = root->in_queue;
			if (!head) {
				break;
			}
			if (_cas(&root->in_queue, head, NULL)) {
				// Reverse the order
				while (head) {
					queue_head *next = head->next;
					head->next = root->out_queue;
					root->out_queue = head;
					head = next;
				}
				break;
			}
		}
	}

	queue_head *head = root->out_queue;
	if (head) {
		root->out_queue = head->next;
	}

	pthread_mutex_unlock(&root->lock);
	return head;
}
