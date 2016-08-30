#include <stdlib.h>
#include <stdio.h>
#include "queue.h"
#include <libgen.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

int main(int argc, char **argv) {
	size_t bufSize= 10*1024*1024;
	size_t blockSize= 1024;
	struct Queue *q;
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
	char *buf = malloc (bufSize);
	time_t start = time(NULL);
	intptr_t offset=0;
	for(; offset < bufSize; offset +=blockSize) {
		struct QueueData qd;
		qd.vlen = blockSize;
		qd.v = buf +  offset;
		queue_push(q, &qd);
	}
	time_t end= time(NULL);
	time_t difft = end-start;
	printf("it took %lu to push %lu(%g) in %lu(%g) blocks\n", (unsigned long )difft, bufSize,bufSize/ (double) difft,  blockSize, blockSize/(double)difft);

	start = time(NULL);
	offset=0;
	for(; offset < bufSize; offset +=blockSize) {
		struct QueueData qd;
		queue_pop(q, &qd);
	}
	end= time(NULL);

	difft = end-start;
	printf("it took %lu to pop %lu(%g) in %lu(%g) blocks\n", (unsigned long) difft,bufSize,bufSize/ (double) difft,  blockSize, blockSize/(double)difft);
    queue_close(q);
	return 0;
}
