#include "bptree.h"
#include <pthread.h>
#include <vector>
#include <unordered_map>
#include <cstdlib>
#include <sys/time.h>

#define DbSize 1000000

int thread_num = 4;
int tx_size = 16;
int max_tx = 1000;
std::atomic<int> Counter(0);
std::atomic<int> ci[4];
struct timeval
cur_time(void)
{
	struct timeval t;
	gettimeofday(&t, NULL);
	return t;
}

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

LockFreeQueue *schedulerQueue = new LockFreeQueue();

void
print_tree_core(NODE *n)
{
	printf("["); 
	for (int i = 0; i < n->nkey; i++) {
		if (!n->isLeaf) print_tree_core(n->chi[i]); 
		printf("%d", n->key[i]); 
		if (i != n->nkey-1 && n->isLeaf) putchar(' ');
	}
	if (!n->isLeaf) print_tree_core(n->chi[n->nkey]);
	printf("]");
}

void
print_tree(NODE *node)
{
	print_tree_core(node);
	printf("\n"); fflush(stdout);
}

void
erase(NODE *node) {
	int i;
	for(i=0; i < N-1; i++) {
		node->chi[i] = nullptr;
		node->key[i] = 0;
	}
	node->chi[N-1] = nullptr;
	node->nkey = 0;
}

TEMP *
alloc_temp() {
	TEMP *node;
	if(!(node = (TEMP *)calloc(1, sizeof(TEMP)))) ERR;
	node->isLeaf=true;
	node->nkey=0;

	return node;
}

NODE *
alloc_leaf(NODE *parent)
{
	NODE *node;
	if (!(node = (NODE *)calloc(1, sizeof(NODE)))) ERR;
	node->isLeaf = true;
	node->parent = parent;
	node->nkey = 0;

	return node;
}

NODE *alloc_internal(NODE *parent){
	NODE *node;
	if (!(node = (NODE *)calloc(1, sizeof(NODE)))) ERR;
	node->isLeaf = false;
	node->parent = parent;
	node->nkey = 0;
	return node;
}

NODE *
find_leaf(NODE *node, int key)
{
	int kid;

	if (node->isLeaf) return node;
	for (kid = 0; kid < node->nkey; kid++) {
		if (key < node->key[kid]) break;
	}

	return find_leaf(node->chi[kid], key);
}

void copy_in_temp(TEMP *temp, NODE *node) {
	int i;
	for(i=0; i < N-1; i++) {
		temp->chi[i] = node->chi[i];
		temp->key[i] = node->key[i];
	}
	temp->chi[i] = node->chi[i];
	temp->nkey = node->nkey;
}

void copy_from_temp_to_left_parent(TEMP *temp, NODE *left_parent){
	for (int i = 0; i < (int)ceil((N+1)/2); i++) {
		left_parent->key[i] = temp->key[i];
		left_parent->chi[i] = temp->chi[i];
		left_parent->nkey++;
	}
	left_parent->chi[(int)ceil((N+1)/2)] = temp->chi[(int)ceil((N+1)/2)];
}

void copy_from_temp_to_right_parent(TEMP *temp, NODE *right_parent){
	int i;
	for (i = (int)ceil((N+1)/2) + 1; i < N; i++){
		right_parent->key[i - ((int)ceil((N+1)/2) + 1)] = temp->key[i];
		right_parent->chi[i - ((int)ceil((N+1)/2) + 1)] = temp->chi[i];
		right_parent->nkey++;
	}
	right_parent->chi[i - ((int)ceil((N+1)/2) + 1)] = temp->chi[i];
	for (int i = 0; i < right_parent->nkey+1; i++) right_parent->chi[i]->parent = right_parent;
}

void insert_after_left_child(NODE *parent, NODE *left_child, int rs_key, NODE *right_child){
	int li = 0;
	int ri = 0;
	
	for(int i = 0; i < parent->nkey + 1; i++){
		if (parent->chi[i] == left_child) {
			li = i; // left_child_id
			ri = li+1; 
			break; 
		}
	}
	
	
	for (int i = parent->nkey+1; i > ri; i--) {
		parent->chi[i] = parent->chi[i-1];
	}
	for (int i = parent->nkey; i > li; i--) {
		parent->key[i] = parent->key[i-1];
	}
	
	parent->key[li] = rs_key;
	parent->chi[ri] = right_child;
	parent->nkey++;
	
}

void insert_temp_after_left_child(TEMP *temp, NODE *left_child, int rs_key, NODE *right_child){
	int li = 0;
	int ri = 0;
	int i;
	
	for(i = 0; i < temp->nkey + 1; i++){
		if (temp->chi[i] == left_child) {
			li = i; // left_child_id
			ri = li+1; 
			break; 
		}
	}
	assert(i != temp->nkey+1);
	
	for (int i = temp->nkey+1; i > ri; i--) {
		temp->chi[i] = temp->chi[i-1];
	}
	for (int i = temp->nkey; i > li; i--) {
		temp->key[i] = temp->key[i-1];
	}
	
	temp->key[li] = rs_key;
	temp->chi[ri] = right_child;
	temp->nkey++;
}

void insert_in_temp(TEMP *temp, int key, void *ptr){
	int i;
	if (key < temp->key[0]) {
		for (i = temp->nkey; i > 0; i--) {
			temp->chi[i] = temp->chi[i-1] ;
			temp->key[i] = temp->key[i-1] ;
		}
		temp->key[0] = key;
		temp->chi[0] = (NODE *)ptr;
	}
	else {
		for (i = 0; i < temp->nkey; i++) {
			if (key < temp->key[i]) break;
		}
		for (int j = temp->nkey; j > i; j--) {
			temp->chi[j] = temp->chi[j-1] ;
			temp->key[j] = temp->key[j-1] ;
		}
		temp->key[i] = key;
		temp->chi[i] = (NODE *)ptr;
	}
	temp->nkey++;
}

NODE *
insert_in_leaf(NODE *leaf, int key, DATA *data)
{
	int i;
	if (key < leaf->key[0]) {
		for (i = leaf->nkey; i > 0; i--) {
			leaf->chi[i] = leaf->chi[i-1] ;
			leaf->key[i] = leaf->key[i-1] ;
		} 
		leaf->key[0] = key;
		leaf->chi[0] = (NODE *)data;
	}
	else {
		for (i = 0; i < leaf->nkey; i++) {
			if (key < leaf->key[i]) break;
		}
		for (int j = leaf->nkey; j > i; j--) {		
			leaf->chi[j] = leaf->chi[j-1] ;
			leaf->key[j] = leaf->key[j-1] ;
		} 
		leaf->key[i] = key;
		leaf->chi[i] = (NODE *)data;
    /* CodeQuiz */
	}
	leaf->nkey++;

	return leaf;
}

void
set_ptr(NODE *L, NODE * leaf) {
	L->chi[N-1] = leaf->chi[N-1];
	leaf->chi[N-1] = L;
}

void
insert_in_parent(NODE *leaf, int key, NODE *L)
{

	if(leaf == Root) {
		NODE *R;
		if (!( R =(NODE *)calloc(1, sizeof(NODE)))) ERR;
		R->key[0] = key;
		R->chi[0] = leaf;
		R->chi[1] = L;
		R->isLeaf = false;
		R->nkey = 1;
		leaf->parent = R;
		L->parent = R;
		Root = R;
		return;
	}

	NODE *P = leaf->parent;
	if(P->nkey < N - 1)
	{
		insert_after_left_child(P, leaf, key, L);
	} else {
		TEMP *T = alloc_temp();
		copy_in_temp(T, P);
		insert_temp_after_left_child(T, leaf, key, L);

		erase(P);
		NODE *right_parent = alloc_internal(P->parent);

		copy_from_temp_to_left_parent(T, P);
		int rs_key_parent = T->key[(int)ceil(N/2)];
		copy_from_temp_to_right_parent(T, right_parent);
		insert_in_parent(P, rs_key_parent, right_parent);
	}
}

void 
insert(int key, DATA *data)
{
	NODE *leaf;

	if (Root == NULL) {
		leaf = alloc_leaf(NULL);
		Root = leaf;
	}
	else {
		leaf = find_leaf(Root, key);
	}
	if (leaf->nkey < (N-1)) {
		insert_in_leaf(leaf, key, data);
	}
	else { // split
	int i, j;
	NODE *L = alloc_leaf(leaf->parent);

	// Copy L.P1 ... L.Kn-1 to temp block T
	TEMP *T = alloc_temp();

	copy_in_temp(T, leaf);
	insert_in_temp(T, key, data);
	set_ptr(L, leaf);
	//erase
	erase(leaf);
	// copy T.data1 thru T.data
	for(i=0; i < ceil(N/2); i++) {
		leaf->chi[i] = T->chi[i];
		leaf->key[i] = T->key[i];
		leaf->nkey++;
	}

	for(j = ceil(N/2); j < N; j++) {
		L->chi[j - (int)ceil(N/2)] = T->chi[j];
		L->key[j - (int)ceil(N/2)] = T->key[j];
		L->nkey++;
	}
	//get smallest key in L
	int _key = L->key[0];

	// insert in parent
	insert_in_parent(leaf, _key, L);
	}
}

void
init_root(void)
{
	Root = NULL;
}

int 
interactive()
{
  int key;

  std::cout << "Key: ";
  std::cin >> key;

  return key;
}

void
search(NODE *node, int key)
{
	int i;
	NODE *leaf = find_leaf(node, key);
	for(i = 0; i < leaf->nkey; i++) {
		if(leaf->key[i] == key) {
			DATA *record = (DATA *)leaf->chi[i];
			if(pthread_rwlock_tryrdlock(&(record->rwlock))==0) {
				printf("Read lock acquired\n");
				int readVal = record->val;
				printf("val: %d\n", readVal);
				if(pthread_rwlock_unlock(&(record->rwlock))!=0) ERR;
				return;
			}
			printf("Read lock acquistion failed\n");
			throw (-1);
		}
	}
	printf("Key %d not found\n", key);
	return;
}

void
update(NODE *node, int key, int newVal)
{
	int i;
	NODE *leaf = find_leaf(node, key);
	for(i=0; i<leaf->nkey; i++) {
		if(leaf->key[i] == key) {
			DATA *record = (DATA *)leaf->chi[i];
			if(pthread_rwlock_trywrlock(&(record->rwlock))==0) {
				record->val = newVal;
				printf("New value for key: %d is %d\n", key, record->val);
				if(pthread_rwlock_unlock(&(record->rwlock))!=0) ERR;
				return;
			}
			throw (-1);
		}
	}
	printf("Value couldn't be updated\n");
	return;
}

NODE *
find_start(NODE *node) 
{	
	if(node->isLeaf) {
		return node;
	}

	return find_start(node->chi[0]);
}

void *
scheduler(void *arg) {
	/*
	Process log records:
		1. Create row-queue if it doesn't exist already
		2. Enqueue each write to the respective row-queue
		3. Enqueue row-queue to scheduler queue
	*/
	unordered_map<int, LockFreeQueue *> rowQueues;
	vector<LogRecord *> logRecords = *(vector<LogRecord *> *)arg;
	for(int i = 0; i < logRecords.size(); i++) {
		QueueNode *node = new QueueNode();
		node->content = logRecords[i];
		auto r = rowQueues.find(logRecords[i]->key);
		if(r != rowQueues.end()) {
			auto isEmpty = r->second->empty.load();
			r->second->enq(node);
			if(isEmpty) {
				QueueNode *schedulerNode = new QueueNode();
				schedulerNode->content = r->second;
				schedulerQueue->enq(schedulerNode);
			}
		} else {
			// New row-queue
			LockFreeQueue *newRowQueue = new LockFreeQueue();
			newRowQueue->enq(node);
			// Add rowqueue to record->queue map
			rowQueues.insert({logRecords[i]->key, newRowQueue});
			// Add row-queue to scheduler queue
			QueueNode *schedulerNode = new QueueNode();
			schedulerNode->content = newRowQueue;
			schedulerQueue->enq(schedulerNode);
		}
	}
}

void *
snapshotter(void *arg) {
	// advance c and n by taking the minimum ci from each of the worker threads
}

void *
worker(void *arg) {
	/*
	TO DO:
		1. Dequeue row-queue from Scheduler Queue
		2. Dequeue write from row-queue
		3. Apply write
		4. If row-queue not empty, enqueue the row-queue back to the scheduler queue
	*/
	while(true) {
		QueueNode *dequeuedNode = schedulerQueue->deq();
		if(dequeuedNode==NULL) continue;
		LockFreeQueue *rowQueue = (LockFreeQueue *)dequeuedNode->content;
		QueueNode *dequeuedRecord = rowQueue->deq();
		LogRecord *record = (LogRecord *)dequeuedRecord->content;
		// TO DO: Modify to accept inserts and update records.
		update(Root, record->key, record->newValue);
	}
}

void generateLogRecords(vector<LogRecord *> &log) {
	int txOrder = 1;
	for(int i = 0; i < max_tx; i++) {
		log[i] = (LogRecord *)malloc(sizeof(LogRecord));
		log[i]->key = rand() % DbSize;
		log[i]->newValue = rand();
		log[i]->timestamp.store(txOrder);
		if(i % tx_size == 0) {
			txOrder++;
		}
	}
}

int
main(int argc, char *argv[])
{
	struct timeval begin, end;
	pthread_t thread[thread_num];

	init_root();


	// Initialize B+ tree with some default values
	int k = 0;
	while (k < DbSize) {
		DATA *record = (DATA *)malloc(sizeof(DATA));
		record->key = k;
		record->val = 0;
		record->timestamp = 0;
		int rc = pthread_rwlock_init(&(record->rwlock), NULL);
		if(rc==-1) ERR;
		insert(k, record);
		// print_tree(Root);
		k++;
	}

	// Generate log of committed transactions to replicate (update only Txs)
	vector<LogRecord *> log(max_tx, NULL);
	generateLogRecords(log); 

	cout << "|------------ Beginning of Log ---------------|" << endl;
	for(int i = 0; i < max_tx; i++) {
		cout << log[i]->key << endl;
	}
	cout << "|------------ End of Log ---------------------|" << endl;

	// begin = cur_time();

	// for(int i = 0; i < thread_num; i++) {
	// 	pthread_create(&thread[i], NULL, worker, (void *)NULL);
	// }

	// for(int i = 0; i < thread_num; i++) {
	// 	pthread_join(thread[i], NULL);
	// }

	// printf("Successful transactions: %u \n", Counter.load());
	// end = cur_time();

	return 0;
}