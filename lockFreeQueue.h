#include <vector>
#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <atomic>

typedef struct qnode {
	std::atomic<struct qnode *>next;
	int value;
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

	void enq(int val) {
		QueueNode *node = (QueueNode *)malloc(sizeof(QueueNode));
		node->value = val;
		while(true) {
			QueueNode *last = tail.load();
			QueueNode *next = last->next.load();
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

	int deq() {
		while(true) {
			QueueNode *first = head.load();
			QueueNode *last = tail.load();
			QueueNode *next = first->next.load();
			if(first == head.load()) {
				if(first == last) {
					if(next == NULL) {
						return -1;
					}
					tail.compare_exchange_strong(last, next);
				} else {
					int value = next->value;
					if(head.compare_exchange_strong(first, next)) {
						return value;
					}
				}
			}
		}
	}

	bool isEmpty() {
		// check if queue is empty
        QueueNode *first = head.load();
        QueueNode *last = tail.load();
        QueueNode *next = first->next.load();
		if(first == last && next == NULL) {
			return true;
		} else {
			return false;
		}
	}
};

LockFreeQueue *segmentQueue = new LockFreeQueue();
LockFreeQueue *readOnlyQueue = new LockFreeQueue();