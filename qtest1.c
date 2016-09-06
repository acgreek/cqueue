// vim : set noet
#include <stdlib.h>
#include <stdio.h>
#include "queue.h"
#include <libgen.h>
#include <string.h>

void fill15_test(struct Queue *q) {
	struct QueueData qd2;
	char buffer[1024];
	int i;
	for (i = 0; i < 15; i++) {
		qd2.v = buffer;
		qd2.vlen= sprintf(buffer,"%d",i);
		queue_push(q, &qd2);
	}
	int64_t count;
	if (0 != queue_count(q, &count)) {
	}
	if (15 != count) {
		fprintf(stderr, "ERROR: there should be 15 in the queue but got back %lld\n", (long long unsigned)count);
	}
	for (i = 0; i < 15; i++) {
		queue_pop(q, &qd2);
		int len = sprintf(buffer,"%d",i);
		if (len != qd2.vlen || 0 != memcmp(buffer, qd2.v, qd2.vlen)) {
			printf("pop value not expected at index %d: %s\n", i, (char *)qd2.v);
		}
		if (qd2.v)
			free(qd2.v);
	}
	for (i = 0; i < 15; i++) {
		qd2.v = buffer;
		qd2.vlen= sprintf(buffer,"%d",i);
		queue_push(q, &qd2);
	}
	if (0 != queue_count(q, &count)) {
	}
	if (15 != count) {
		fprintf(stderr, "ERROR: there should be 15 in the queue but got back %lld\n", (long long unsigned)count);
	}
	for (i = 0; i < 15; i++) {
		queue_peek(q, i,&qd2);
		int len = sprintf(buffer,"%d",i);
		if (len != qd2.vlen || 0 != memcmp(buffer, qd2.v, qd2.vlen)) {
			printf("pop value not expected at index %d:expect %d actual %s\n", i, i, (char *)qd2.v);
		}
	}
	for (i = 0; i < 15; i++) {
		ssize_t count;
		queue_count(q, &count);
		if (15- i != count) {
			fprintf(stderr, "ERROR: while popping off queue, expected %d but got %d\n", (int) 15-i,(int)count );
		}
		queue_pop(q, NULL);
	}
	if (0 != queue_count(q, &count)) {
	}
	if (0 != count) {
		fprintf(stderr, "ERROR: there should be 0 in the queue but got back %lld\n", (long long unsigned)count);
	}
}

int main(int argc, char **argv) {
	struct Queue *q;
	char template[] = "/tmp/qtest_XXXXXX";
	if (NULL == mkdtemp(template)) {
		puts("failed to create temp dir for running tests");
		return 1;
	}
	q = queue_open_with_options(template, "maxBinLogSize", 2,NULL);
	fill15_test(q);
	if(queue_close(q) != 0)
		puts("there was an error closing the queue!");
	puts("queue successfully closed!");
	return 0;
}
