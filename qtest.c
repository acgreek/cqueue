// vim : set noet
#include <stdlib.h>
#include <stdio.h>
#include "queue.h"
#include <libgen.h>
#include <string.h>
#include <pthread.h>

void hdl_error(int rtn) {
	if (0 > rtn) {
		exit(EXIT_FAILURE);
	}
}

void henry_push_test(struct Queue *q) {
	struct QueueData qd = { "HENRY" , sizeof("HENRY") };
	struct QueueData qd2;
	hdl_error(puts("queue successfully opened!"));
	if(queue_push(q, &qd) != 0)
		hdl_error(puts("pushing HENRY failed!"));
	if(queue_pop(q, &qd2) != 0) {
		hdl_error(puts("popping HENRY failed!"));
	} else {
		hdl_error(puts("here's what popped!"));
		hdl_error(puts((const char*)qd2.v));
		free(qd2.v);
	}
}
void  int32_push_test(struct Queue *q) {
	struct QueueData qd = { "HENRY" , sizeof("HENRY") };
	struct QueueData qd2;
	int32_t ft = 42;
	qd.v = &ft;
	qd.vlen = sizeof(int32_t);
	queue_push(q, &qd);
	queue_pop(q, &qd2);
	printf("popping forty-two: %d\n", *(int32_t*)qd2.v);
	if (qd2.v)
		free(qd2.v);
}
void push_count(struct Queue *q, int count) {
	struct QueueData qd2;
	int i;
	char buffer[1024];
	for (i = 0; i < 15; i++) {
		qd2.v = buffer;
		qd2.vlen= sprintf(buffer,"%d",i);
		queue_push(q, &qd2);
	}
}
void fill15_test(struct Queue *q) {
	struct QueueData qd2;
	int i;
	char buffer[1024];
	push_count(q, 15) ;
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
		if (qd2.vlen && qd2.v)
			free(qd2.v);
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
void maxEntries (const char * template) {
	struct Queue *q=queue_open_with_options(template,"maxEntries",4, NULL);
	struct QueueData qd;
	int i = 10;
	qd.v = &i;
	qd.vlen = sizeof(i);
	if (LIBQUEUE_SUCCESS != queue_push(q, &qd))
		fprintf(stdout,"%d queue push did not succeed", __LINE__);
	if (LIBQUEUE_SUCCESS != queue_push(q, &qd))
		fprintf(stdout,"%d queue push did not succeed", __LINE__);
	if (LIBQUEUE_SUCCESS != queue_push(q, &qd))
		fprintf(stdout,"%d queue push did not succeed", __LINE__);
	if (LIBQUEUE_SUCCESS != queue_push(q, &qd))
		fprintf(stdout,"%d queue push did not succeed", __LINE__);
	if (LIBQUEUE_FAILURE!= queue_push(q, &qd))
		fprintf(stdout,"%d queue push succeed when it shouldn't have", __LINE__);
	if (LIBQUEUE_SUCCESS != queue_pop(q, &qd))
		fprintf(stdout,"%d queue pop did not succeed", __LINE__);
	if (qd.v)
		free(qd.v);
	if (LIBQUEUE_SUCCESS != queue_push(q, &qd))
		fprintf(stdout,"%d queue push did not succeed", __LINE__);
	if (LIBQUEUE_FAILURE!= queue_push(q, &qd))
		queue_close(q);
}
void *writerThread(void* templatep) {
	const char * template = (const char *)templatep;
	struct Queue *q;
	q = queue_open(template);
	push_count(q, 100) ;
	pthread_yield();
	push_count(q, 100) ;
	queue_close(q);
	return NULL;
}
void threadTest(const char *template) {
	pthread_t child;
	pthread_create(&child,NULL, writerThread,(void *) template);
	struct Queue *q;
	q = queue_open(template);
	void *rtn;
	queue_close(q);
	pthread_join(child,&rtn);

}

int main(int argc, char **argv) {
	struct Queue *q;
	puts(argv[0]);

	char template[] = "/tmp/qtest_XXXXXX";
	if (NULL == mkdtemp(template)) {
		puts("failed to create temp dir for running tests");
		return 1;
	}
	q = queue_open(template);
	if(0 == queue_is_opened(q)) {
		fprintf(stderr,"Failed to open the queue:%s", queue_get_last_error(q));
		queue_close(q);
		return EXIT_FAILURE;
	}
	queue_close(q);
	int i;
	for (i=1; i < 20; i++  ) {
		q = queue_open_with_options(template, "maxBinLogSize", i,NULL);
		fill15_test(q);
		queue_close(q);
	}
	q = queue_open(template);

	henry_push_test(q);
	int32_push_test(q);
	fill15_test(q);
	queue_close(q);
	maxEntries (template);


	if(queue_close(q) != 0)
		puts("there was an error closing the queue!");
	puts("queue successfully closed!");

	// lets delete the queue
	char cmdline[1024];
	sprintf(cmdline, "rm -rf %s", template);
	int resultsOfDelete = system(cmdline);
	if (EXIT_SUCCESS != resultsOfDelete) {
		puts("there was an error running delete of temp dir command ");
		return 1;
	}
	q=queue_open_with_options(template,"failIfMissing", NULL);
	if (NULL == queue_get_last_error(q)) {
		puts("queue was created when it shouldn't have been");
	}
	queue_close(q);

	threadTest(template);
	return 0;
}
