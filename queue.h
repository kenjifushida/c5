#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <strings.h>
#include <math.h>
#include "debug.h"
#include <iostream>
#include <unordered_map>
using namespace std;

typedef struct record {
	int key;
	int newValue;
	std::atomic<int> timestamp;
} LogRecord;

typedef struct qnode {
	std::atomic<struct qnode *>next;
	void *content;
} QueueNode;

class LockFreeQueue {
public:
	std::atomic<QueueNode *> head;
	std::atomic<QueueNode *> tail;
	std::atomic<QueueNode *> next;
	std::atomic<uint> empty;
	LockFreeQueue() {
		QueueNode *node = (QueueNode *)malloc(sizeof(QueueNode));
		node->next.store(NULL);
		head.store(node);
		tail.store(node);
		empty.store(0x01);
	}

	void enq(QueueNode *node) {
		while(true) {
			auto *last = tail.load();
			auto *next = last->next.load();
			if(last == tail.load()) {
				if(next == NULL) {
					if(last->next.compare_exchange_strong(next, node)) {
						tail.compare_exchange_strong(last, node);
						empty.store(0x00);
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

	void testAndSetEmpty() {
		// check if queue is empty
	}
};