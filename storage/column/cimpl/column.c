#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

void current_utc_time(struct timespec *ts) {
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  ts->tv_sec = mts.tv_sec;
  ts->tv_nsec = mts.tv_nsec;
#else
  clock_gettime(CLOCK_REALTIME, ts);
#endif
}

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
		//offset = __sync_fetch_and_add(&mt->offset, 1);
		//offset = mt->offset++;
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
	//printf("offset: %d\n", offset);
	mt->data[offset] = value;
}

void memtable_free(memtable* mt) {
	free(mt->data);
}

const int N = 100000000;

void* memtable_insert_test(void* arg) {
	memtable *mt = (memtable*) arg;
	for (int i=0;i<N;i++) {
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
	int c = 3;
	pthread_t *threads = (pthread_t*) malloc(sizeof(pthread_t) * c);
	if (threads == NULL) {
		perror("cannot create pthread array\n");
	}
	struct timespec ts;
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
	current_utc_time(&ts);
	double s = ((double)ts.tv_sec + ts.tv_nsec) / 1e8;
	double ops = N * c / s;
	double bw = ops * 8;
	double bwb = bw * 8;
	printf("spent %.2fs, %.2fM ops, %.2fM ops/s, %.2fGB/s, %.2fGbit/s\n", s, ((double)N*c)/1e6, ops/1e6, bw / 1024 / 1024 / 1024, bwb / 1024 / 1024 / 1024);
	return 0;
}