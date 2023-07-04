#include <vector>
#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>

typedef struct qnode {
	std::atomic<struct qnode *>next;
	void *content;
} QueueNode;


class LockFreeQueue {
public:
	std::atomic<QueueNode *> head;
	std::atomic<QueueNode *> tail;
	std::atomic<QueueNode *> next;
	LockFreeQueue() {
		QueueNode *node = (QueueNode *)malloc(sizeof(QueueNode));
		node->next.store(NULL);
		head.store(node);
		tail.store(node);
	}

	void enq(QueueNode *node) {
		while(true) {
			auto *last = tail.load();
			auto *next = last->next.load();
			if(last == tail.load()) {
				if(next == NULL) {
					if(last->next.compare_exchange_strong(next, node)) {
						tail.compare_exchange_strong(last, node);
						return;
					}
				}
				else {
					tail.compare_exchange_strong(last, next);
				}
			}
		}
	}

	QueueNode *deq() {
		while(true) {
			auto *first = head.load();
			auto *last = tail.load();
			auto *next = first->next.load();
			if(first == head.load()) {
				if(first == last) {
					if(next == NULL) {
						return NULL;
					}
					tail.compare_exchange_strong(last, next);
				} else {
					if(head.compare_exchange_strong(first, next)) {
						return next;
					}
				}
			}
		}
	}

	bool isEmpty() {
		// check if queue is empty
        auto *first = head.load();
        auto *last = tail.load();
        auto *next = first->next.load();
		if(first == last && next == NULL) {
			return true;
		} else {
			return false;
		}
	}
};

LockFreeQueue *schedulerQueue = new LockFreeQueue();
LockFreeQueue *readOnlyQueue = new LockFreeQueue();