// vim : set noet
#include <stdlib.h>
#include <stdio.h>
#include "queue.h"
#include <libgen.h>
#include <string.h>
#include <ExtremeCUnit.h>
#include "queue.c"

#define LOG_ERROR(cmd) logError(#cmd, __LINE__, cmd)

static void logError(const char * cmd, int line,  int rtn) {
    if (LIBQUEUE_SUCCESS != rtn) {
        printf("ERROR: cmd");
    }
}

static void fillQueues(struct Queue *q) {
	struct QueueData qd2;
	char buffer[1024];
	int i;
	for (i = 0; i < 15; i++) {
		qd2.v = buffer;
		qd2.vlen= sprintf(buffer,"%d",i);
		LOG_ERROR(queue_push(q, &qd2));
	}
}
static void pop15(struct Queue *q) {
	struct QueueData qd2;
	char buffer[1024];
	int i;
	for (i = 0; i < 15; i++) {
		LOG_ERROR(queue_pop(q, &qd2));
		int len = sprintf(buffer,"%d",i);
		if (len != qd2.vlen || 0 != memcmp(buffer, qd2.v, qd2.vlen)) {
			printf("pop value not expected at index %d: %s\n", i, (char *)qd2.v);
		}
		if (qd2.v)
			free(qd2.v);
	}
}

void fill15_test(struct Queue *q) {
	struct QueueData qd2;
	char buffer[1024];
	fillQueues(q);
	int64_t count;
	if (0 != queue_count(q, &count)) {
	}
	if (15 != count) {
		fprintf(stderr, "ERROR: there should be 15 in the queue but got back %lld\n", (long long unsigned)count);
	}
	int i;
	pop15(q);
	for (i = 0; i < 15; i++) {
		qd2.v = buffer;
		qd2.vlen= sprintf(buffer,"%d",i);
		LOG_ERROR(queue_push(q, &qd2));
	}
	if (0 != queue_count(q, &count)) {
	}
	if (15 != count) {
		fprintf(stderr, "ERROR: there should be 15 in the queue but got back %lld\n", (long long unsigned)count);
	}
	for (i = 0; i < 15; i++) {
		LOG_ERROR(queue_peek(q, i,&qd2));
		int len = sprintf(buffer,"%d",i);
		if (len != qd2.vlen || 0 != memcmp(buffer, qd2.v, qd2.vlen)) {
			printf("pop value not expected at index %d:expect %d actual %s\n", i, i, (char *)qd2.v);
		}
	}
	for (i = 0; i < 15; i++) {
		ssize_t count;
		LOG_ERROR(queue_count(q, &count));
		if (15- i != count) {
			fprintf(stderr, "ERROR: while popping off queue, expected %d but got %d\n", (int) 15-i,(int)count );
		}
		LOG_ERROR(queue_pop(q, NULL));
	}
	if (0 != queue_count(q, &count)) {
	}
	if (0 != count) {
		fprintf(stderr, "ERROR: there should be 0 in the queue but got back %lld\n", (long long unsigned)count);
	}
}
TEST(TwoByteFiles) {
	struct Queue *q;
	char template[] = "/tmp/qtest_XXXXXX";
	if (NULL == mkdtemp(template)) {
		puts("failed to create temp dir for running tests");
		return 1;
	}
	q = queue_open_with_options(template, "maxBinLogSize", 1,NULL);
	fill15_test(q);
	Assert(0 ==queue_close(q));
	return 0;
}

TEST(OneByteFiles) {
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
	return 0;
}
TEST(Coruption1) {
	struct Queue *q;
	char template[] = "/tmp/qtest_XXXXXX";
	if (NULL == mkdtemp(template)) {
		puts("failed to create temp dir for running tests");
		return 1;
	}
	q = queue_open_with_options(template, "maxBinLogSize", 2,NULL);
	fillQueues(q);

	fwrite(template, 1,1 ,q->write.binlogfd);
	Assert(queue_close(q) == 0);
	q = queue_open_with_options(template, "maxBinLogSize", 2,NULL);
	pop15(q) ;

	struct QueueData qd2;
	Assert(LIBQUEUE_FAILURE == queue_pop(q, &qd2));
	fillQueues(q);
	pop15(q) ;

	Assert(queue_close(q) == 0);
	return 0;
}
TEST(Coruption1to2000) {
	struct Queue *q;
	char template[] = "/tmp/qtest_XXXXXX";
	if (NULL == mkdtemp(template)) {
		puts("failed to create temp dir for running tests");
		return 1;
	}
	int i=1;
	for (; i < 2000; i++) {
		q = queue_open_with_options(template, "maxBinLogSize", 2,NULL);
		fillQueues(q);

		fwrite(template, i,1 ,q->write.binlogfd);
		Assert(queue_close(q) == 0);
		q = queue_open_with_options(template, "maxBinLogSize", 2,NULL);
		pop15(q) ;

		struct QueueData qd2;
		Assert(LIBQUEUE_FAILURE == queue_pop(q, &qd2));
		fillQueues(q);
		pop15(q) ;
		Assert(queue_close(q) == 0);
	}
	return 0;
}
