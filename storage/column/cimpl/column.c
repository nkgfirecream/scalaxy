#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

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
	int offset;
	for (;;) {
		offset = __sync_fetch_and_add(&(mt->offset), 1);
		if (offset < mt->size) {
			break;
		} else if (offset > mt->size) {
			continue; // TODO: FIX HOT CPU!
		} else {
			mt->offset = offset = 0;
			break;
		}
	}
	mt->data[offset] = value;
}

void memtable_free(memtable* mt) {
	free(mt->data);
}

int main(int argc, char* argv[]) {
	printf("memtable int64_t test\n");
	int ret;
	memtable mt;
	ret = memtable_init(&mt, 1024 * 1024 * 100);
	if (ret != 0) {
		perror("error init memtable\n");
	}
	for (int i=0;i<1000000000;i++) {
		memtable_insert(&mt, 0x00);
	}
	return 0;
}