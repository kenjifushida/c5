#include "bptree.h"
#include "lockFreeQueue.h"
#include <pthread.h>
#include <vector>
#include <queue>
#include <unordered_map>
#include <atomic>
#include <cstdlib>
#include <sys/time.h>

#define DbSize 100000
#define thread_num 16
#define readers_num 4
#define workers_num 10
#define tx_size 16

int max_tx = 1000000;
int max_c = 0;
std::atomic<int> Counter(0);
std::atomic<int> n(0);
int ci[workers_num];
struct timeval begin, endWrite, endRead;

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
	std::atomic<int> prev_timestamp;
} LogRecord;

typedef struct segment {
	std::atomic<bool> preprocessed;
	LogRecord * records[250];
	int segmentSize;
} LogSegment;

typedef struct transaction {
	int readSet[tx_size];
	int ts;
} Transaction;

vector<LogSegment *> logSegments(40000, NULL);
vector<Transaction *> readOnlyTransactions(1000000, NULL);

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

// Read operation
int
search(NODE *node, int key, int txTimestamp)
{
	int i;
	NODE *leaf = find_leaf(node, key);
	for(i = 0; i < leaf->nkey; i++) {
		if(leaf->key[i] == key) {
			DATA *record = (DATA *)leaf->chi[i];
			// Get the latest version readable by the transaction
			while(record && record->timestamp > txTimestamp) {
				record = record->next;
			}
			// if(pthread_rwlock_tryrdlock(&(record->rwlock))==0) {
				int readVal = record->val;
				// printf("val: %d\n", readVal);
				// if(pthread_rwlock_unlock(&(record->rwlock))!=0) ERR;
				return readVal;
			// }
			// printf("Read lock acquistion failed\n");
			// throw (-1);
		}
	}
	printf("Key %d not found\n", key);
	return -1;
}

bool
update(NODE *node, LogRecord *updateRecord)
{
	int i;
	NODE *leaf = find_leaf(node, updateRecord->key);
	for(i=0; i<leaf->nkey; i++) {
		if(leaf->key[i] == updateRecord->key) {
			DATA *record = (DATA *)leaf->chi[i];
			DATA *newVersion = (DATA *)malloc(sizeof(DATA));
			newVersion->next = record;
			newVersion->key = updateRecord->key;
			newVersion->val = updateRecord->newValue;
			newVersion->timestamp = updateRecord->timestamp;
			// pthread_rwlock_init(&newVersion->rwlock, NULL);
			if(record->timestamp == updateRecord->prev_timestamp) {
				// cout << "record updated = " << "key: " << newVersion->key << "neWVal: " << newVersion->val << "ts: " << newVersion->timestamp << endl;
				leaf->chi[i] = (NODE *)newVersion;
				return true;
			}
		}
	}
	// cout << "record deferred = " << "key: " << updateRecord->key << "neWVal: " << updateRecord->newValue << "ts: " << updateRecord->timestamp << endl;
	return false;
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
	vector<int> logRecordMap(DbSize, 0);
	vector<LogSegment *> logSegments = *(vector<LogSegment *> *)arg;
	for(long unsigned int i = 0; i < logSegments.size(); i++) {
		for(int j = 0; j < logSegments[i]->segmentSize; j++) {
			int key = logSegments[i]->records[j]->key;
			logSegments[i]->records[j]->prev_timestamp.store(logRecordMap[key]);
			logRecordMap[key] = logSegments[i]->records[j]->timestamp;
		}
		logSegments[i]->preprocessed.store(true);
		segmentQueue->enq(i);
	}
	return NULL;
}

void *
snapshotter(void *arg) {
	// advance c and n by taking the minimum ci from each of the worker threads
	while(Counter.load() < max_c - 1) {
		int newN = ci[0];
		for(int i = 0; i < workers_num; i++) {
			newN = std::min(newN, ci[i]);
		}
		Counter.store(n);
		// cout << "C: " << Counter.load() << endl;
		n.store(newN);
		usleep(10000);
	}
	return NULL;
}

void update_local_counter(int *local, int timestamp) {
	*local = timestamp - 1;
}

void *
worker(void *arg) {
	/*
	TO DO:
		1. Dequeue row-queue from Scheduler Queue
		2. Dequeue write from row-queue
		3. Apply write
		4. If row-queue not empty, enqueue the row-queue back to the scheduler queue
		5. If writeTS - 1 of current write is bigger than ci, update the local variable.
	*/
	int *ci = (int *)arg;
	queue <LogRecord *> deferQueue;
	while(Counter.load() < max_c - 1) {
		int currentSegment = segmentQueue->deq();
		if(currentSegment < 0) {
			if(logSegments[logSegments.size()-1]->preprocessed.load()) {
				break;
			}
			continue;
		}
		int segmentSize = logSegments[currentSegment]->segmentSize;
		for(int i = 0; i < segmentSize; i++) {
			LogRecord * currentRecord = logSegments[currentSegment]->records[i];
			if(!update(Root, currentRecord)) {
				deferQueue.push(currentRecord);
			};
			if(deferQueue.empty()) {
				update_local_counter(ci, currentRecord->timestamp);
			}
		}
		// Check defer queue for any available writes
		while(!deferQueue.empty()) {
			LogRecord *deferredRecord = deferQueue.front();
			if(update(Root, deferredRecord)) {
				deferQueue.pop();
				update_local_counter(ci, deferredRecord->timestamp);
			}
		}
		// If all deferred writes are completed, update counter to end of current segment
		update_local_counter(ci, logSegments[currentSegment]->records[segmentSize-1]->timestamp);
	}
	update_local_counter(ci, logSegments[logSegments.size()-1]->records[249]->timestamp);
	endWrite = cur_time();
	return NULL;
}

void * 
reader(void *arg){
	/*
	TO DO:
		1. Dequeue incoming read only transactions
		2. Read most recent record version less than or equal to current snapshot
	*/
	while(!readOnlyQueue->isEmpty()) {
		int currentTx = readOnlyQueue->deq();
		Transaction *tx = readOnlyTransactions[currentTx];
		tx->ts = Counter.load();
		for(int i = 0; i < tx_size; i++) {
			int val = search(Root, tx->readSet[i], tx->ts);
		}
	}
	endRead = cur_time();
	return NULL;
}

void generate_log_records(void) {
	
	int txOrder = 0;
	int t = 0;
	for(long unsigned int i = 0; i < logSegments.size(); i++) {
		LogSegment *newSegment = (LogSegment *)malloc(sizeof(LogSegment));
		for(int j=0; j < 250; j++) {
			if(t % (tx_size/2) == 0 || j == 0) {
				txOrder++;
			}
			LogRecord *record = (LogRecord *)malloc(sizeof(LogRecord));
			record->key = rand() % DbSize;
			record->newValue = rand();
			record->timestamp.store(txOrder);
			newSegment->records[j]= record;
			t++;
		}
		newSegment->segmentSize = 250;
		newSegment->preprocessed.store(false);
		logSegments[i] = newSegment;
	}
	max_c = txOrder;
}

void generate_read_only_tx() {
	for(int i = 0; i < max_tx; i++) {
		Transaction *newReadOnlyTx = (Transaction *)malloc(sizeof(Transaction));
		for(int j = 0; j < tx_size; j++) {
			newReadOnlyTx->readSet[j] = rand() % DbSize;
		}
		readOnlyTransactions[i] = newReadOnlyTx;
		readOnlyQueue->enq(i);
	}
}

int
main(int argc, char *argv[])
{
	pthread_t thread[thread_num];

	init_root();


	// Initialize B+ tree with some default values
	int k = 0;
	while (k < DbSize) {
		DATA *record = (DATA *)malloc(sizeof(DATA));
		record->key = k;
		record->val = 0;
		record->timestamp = 0;
		// int rc = pthread_rwlock_init(&(record->rwlock), NULL);
		// if(rc==-1) ERR;
		insert(k, record);
		k++;
	}

	// Generate log of committed transactions to replicate (update only Txs)
	generate_log_records();
	cout << "max c: " << max_c << endl;

	// Generate incoming read only Tx
	generate_read_only_tx();

	// cout << "|------------ Beginning of Log ---------------|" << endl;
	// for(int i = 0; i < logSegments.size(); i++) {
	// 	for(int j = 0; j < logSegments[i]->records.size(); j++) {
	// 		cout << logSegments[i]->records[j]->key << endl;
	// 		cout << logSegments[i]->records[j]->timestamp << endl;
	// 	}
	// }
	// cout << "|------------ End of Log ---------------------|" << endl;

	timeval begin = cur_time();

	for(int i = 0; i < workers_num; i++) {
		pthread_create(&thread[i], NULL, worker, (void*)&ci[i]);
	}
	for(int i = 0; i < readers_num; i++) {
		pthread_create(&thread[workers_num+i], NULL, reader, (void *)NULL);
	}
	pthread_create(&thread[thread_num-1], NULL, scheduler, (void*)&logSegments);
	pthread_create(&thread[thread_num-2], NULL, snapshotter, (void*)NULL);

	for(int i = 0; i < thread_num; i++) {
		pthread_join(thread[i], NULL);
	}

	cout << "Time for write replication: " << (endWrite.tv_sec * 1000000 + endWrite.tv_usec) - (begin.tv_sec * 1000000 + begin.tv_usec) << endl;
	cout << "Time for read only tx: " << (endRead.tv_sec * 1000000 + endRead.tv_usec) - (begin.tv_sec * 1000000 + begin.tv_usec) << endl;

	return 0;
}
