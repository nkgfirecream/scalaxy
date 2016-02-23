#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>

typedef struct {
	int64_t* data;
	int      size;
	int      offset;
} memtable;

int memtable_init(memtable* mt, int size) {
	mt->data = (int64_t*) malloc(sizeof(int64_t) * size);
	if (mt->data == NULL) {
		return -1;
	}
	mt->size = size;
	mt->offset = 0;
	return 0;
}

void memtable_insert(memtable* mt, int64_t value) {
	int64_t offset;
	for (;;) {
		offset = __atomic_fetch_add(&(mt->offset), 1, __ATOMIC_SEQ_CST);
		if (offset < mt->size) {
			break;
		} else if (offset > mt->size) {
			continue; // TODO: FIX HOT CPU!
		} else {
			offset = 0;
			__atomic_and_fetch(&(mt->offset), 0, __ATOMIC_SEQ_CST);
			break;
		}
	}
	mt->data[offset] = value;
}

void memtable_free(memtable* mt) {
	free(mt->data);
}

void* memtable_insert_test(void* arg) {
	memtable *mt = (memtable*) arg;
	for (int i=0;i<100000000;i++) {
		memtable_insert(mt, 0x00);
	}
	return NULL;
}

int main(int argc, char* argv[]) {
	printf("memtable int64_t test\n");
	int ret;
	memtable mt;
	ret = memtable_init(&mt, 1024 * 1024 * 100);
	if (ret != 0) {
		perror("error init memtable\n");
	}
	int c = 8;
	pthread_t *threads = (pthread_t*) malloc(sizeof(pthread_t) * c);
	if (threads == NULL) {
		perror("cannot create pthread array\n");
	}
	for (int i=0; i<c; i++) {
		if (pthread_create(&threads[i], NULL, memtable_insert_test, (void*) &mt)) {
			perror("error create thread\n");
		}
	}
	for (int i=0; i<c; i++) {
		if (pthread_join(threads[i], NULL)) {
			perror("error join with thread\n");
		}
	}
	return 0;
}