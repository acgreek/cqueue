/* vim: set noet
 * libqueue - provides persistent, named data storage queues
 * Copyright (C) 2014-2016 Jens Oliver John <dev@2ion.de>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * */

#include "queue.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <stdarg.h>
#include <limits.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <stdio.h>

#define IFF(X) {if (NULL != X) {free(X);}}
#define IFFN(X) {if (NULL != X) {free(X); X =NULL;}}
#define IFFNF(X,FUNC) {if (NULL != X) {FUNC(X); X =NULL;}}
#define UNUSED __attribute__((unused))

#define MAX_FILE_NAME 1024

typedef struct _FileKey {
	time_t time;
	time_t clock;
}FileKey;
#define FILE_KEY_EQUAL(A,B) (A.time == B.time && A.clock == B.clock)
#define FILE_KEY_GREATER(A,B) (A.time > B.time || (A.time == B.time && A.clock > B.clock))

struct FileItr {
	FileKey key;
	ssize_t catalogIdx;
	FILE *journalfd;
	FILE *binlogfd;
	ssize_t jsize;
	ssize_t bsize;
};

int fileItr_opened(struct FileItr *itrp ) {
	return itrp->journalfd != NULL;
}

struct Queue {
	char * path;
	struct FileItr read;
	struct FileItr write;

	// error string holder
	char * error_strp;

	// stats
	size_t count; // at startup we get the count by reading all the journals, then inc/dec as we push and pop
	size_t jour_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop
	size_t bin_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop
	size_t catalog_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop

	//settings
	size_t max_bin_log_size; //
	char   fail_if_missing;
};

const char * queue_get_last_error(const struct Queue * const q) {
	return q->error_strp;
}
void queue_set_error(struct Queue *const q, const char *what, const char *errstr) {
	IFF(q->error_strp);
	char * ptr;
	asprintf(&ptr, "%s %s", what, errstr);
	q->error_strp= ptr;
}

int queue_is_opened (const struct Queue * const q) {
	return NULL != q->path;
}

static struct Queue * readoptions (va_list argp) {
	struct Queue * q = malloc(sizeof (struct Queue));
	memset(q, 0, sizeof(struct Queue));
	const char * p;
	q->max_bin_log_size= 10 *1024 *1024;
	for (p = va_arg(argp, char *); p != NULL; p = va_arg(argp,char *)) {
		if (0 == strcmp(p, "maxBinLogSize")) {
			q->max_bin_log_size= va_arg(argp, size_t);
		}
		if (0 == strcmp(p, "failIfMissing")) {
		   q-> fail_if_missing = 1;
		}
	}
	return q;
}
static const char *getFileName(const char *prefix, time_t time,clock_t clock ,const char * path, char *file) {
	snprintf(file, MAX_FILE_NAME-1,"%s/%s.%lu.%lu", path,prefix, (unsigned long)time,(unsigned long)clock);
	return file;
}

static const char *getBinLogFileName(time_t time,clock_t clock, const char * path, char *file) {
	return getFileName("bin_log",time,clock,path, file);
}

static const char *getJournalFileName(time_t time,clock_t clock,const char * path, char *file) {
	return getFileName("journal",time,clock,path, file);
}

static ssize_t getFileSize(FILE * fd) {
	if (NULL == fd) return 0;
	struct stat stat;
	fstat(fileno(fd), &stat);
	return stat.st_size;
}

static int openJournalAtTime(time_t time,clock_t clock, const char * path, struct FileItr * itr) {
	char file[MAX_FILE_NAME];
	itr->journalfd = fopen (getJournalFileName(time,clock, path, file), "r+");
	if (NULL == itr->journalfd)
		itr->journalfd = fopen (file, "w+");
	itr->jsize = getFileSize(itr->journalfd);
	getBinLogFileName(time,clock,path, file);
	itr->binlogfd= fopen (getBinLogFileName(time,clock, path, file), "r+");
	if (NULL == itr->binlogfd)
		itr->binlogfd= fopen (file, "w+");
	itr->bsize = getFileSize(itr->binlogfd);
	itr->key.time = time;
	itr->key.clock= clock;
	return 0;
}

struct catalogEntry {
	FileKey key;
	char done;
};
struct JournalEntry {
	unsigned long offset;
	unsigned long size;
	char done;
};

/**
 * @return number of entries in the binlog that are done
 */
static ssize_t doneEntries(const char * file) {
	FILE * cf = fopen(file, "r");
	if (NULL == cf)
		return 0;
	struct JournalEntry je;
	ssize_t delCount=0;
	while (1 == fread(&je, sizeof(je),1, cf)) {
		if (0 == je.done)
			break;
		delCount++;
	}
	fclose(cf);
	return delCount;
}

static void setCountLengthByStatingFiles(struct Queue *q) {
	q->count = q->jour_size= q->bin_size= q->catalog_size=0;
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd;
	if (q->fail_if_missing)
		journalsfd = fopen(file, "r");
	else
		journalsfd = fopen(file, "r+");
	if (NULL == journalsfd) {
		q-> error_strp = strdup("failed to open catalog");
		return ;
	}
	struct catalogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1, journalsfd)) {
		if (entry.done != 0)
			continue;
		struct stat bin_stat, jour_stat;
		if (0 == stat(getJournalFileName(entry.key.time,entry.key.clock, q->path, file), &jour_stat) &&
				0 == stat(getBinLogFileName(entry.key.time,entry.key.clock,  q->path, file), &bin_stat)) {
			q->count += jour_stat.st_size / sizeof(struct JournalEntry) ;
			q->count -=doneEntries(getJournalFileName(entry.key.time,entry.key.clock, q->path, file));
			q->jour_size+= jour_stat.st_size;
			q->bin_size+= bin_stat.st_size;
		}
	}
	fclose (journalsfd);
}

static void getOldestJournal(struct Queue *q,time_t * timep, clock_t *clockp) {
	*timep=0;
	*clockp=0;
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd)
		return;
	struct catalogEntry entry;
	struct catalogEntry oldest_entry;
	oldest_entry.key.time = ULONG_MAX;
	oldest_entry.key.clock= ULONG_MAX;
	while (!feof(journalsfd)) {
		fread(&entry, sizeof(entry), 1, journalsfd);
		if (0 == entry.done && (((unsigned long) entry.key.time) < (unsigned long)oldest_entry.key.time ||
				((((unsigned long) entry.key.time) == (unsigned long)oldest_entry.key.time )
				 && ((unsigned long) entry.key.clock) < (unsigned long)oldest_entry.key.clock))) {
			oldest_entry = entry;
		}
	}
	fclose (journalsfd);
	if (ULONG_MAX != oldest_entry.key.time) {
		*timep = oldest_entry.key.time;
		*clockp= oldest_entry.key.clock;
	}
}

static void newestEntry(struct Queue *q,time_t * timep, clock_t *clockp) {
	*timep=0;
	*clockp=0;
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd)
		return ;
	struct catalogEntry entry;
	struct catalogEntry newest_entry;
	newest_entry.key.time = 0;
	while (!feof(journalsfd)) {
		fread(&entry, sizeof(entry), 1, journalsfd);
		if (0 == entry.done && ( entry.key.time > newest_entry.key.time ||
				(entry.key.time == newest_entry.key.time &&  entry.key.clock > newest_entry.key.clock))) {
			newest_entry = entry;
		}
	}
	fclose (journalsfd);
	if (0 != newest_entry.key.time) {
		*timep = newest_entry.key.time;
		*clockp= newest_entry.key.clock;
	}
}
static void setcatalogEntryDone(struct Queue *q,time_t time, clock_t clock) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd){
		fprintf(stderr, "error opening catalog file %s: %s\n",file , strerror(errno));
		journalsfd = fopen(file, "w+");
		if (NULL == journalsfd){
			fprintf(stderr, "error create catalog file %s: %s\n",file , strerror(errno));
			return;
		}
	}
	struct catalogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1, journalsfd)) {
		if (time  == entry.key.time && clock == entry.key.clock) {
			fseek(journalsfd,-sizeof(entry), SEEK_CUR);
			entry.done= 1;
			fwrite(&entry, sizeof(entry), 1, journalsfd);
			fflush(journalsfd);
			break;
		}
	}
	fclose (journalsfd);
}
static void putEntry(struct Queue *q, time_t time, clock_t ct) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd){
		journalsfd = fopen(file, "w+");
		if (NULL == journalsfd){
			fprintf(stderr, "error create catalog file %s: %s\n",file , strerror(errno));
			return;
		}
	}
	struct catalogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1, journalsfd)) {
		if (1 == entry.done) {
			fseek(journalsfd,-sizeof(entry), SEEK_CUR);
			break;
		}
	}
	entry.done= 0;
	entry.key.time = time;
	entry.key.clock = ct;
	fwrite(&entry, sizeof(entry), 1, journalsfd);
	fflush(journalsfd);
	fclose(journalsfd);
}

static void setFileToWriteTo(struct Queue * q) {
	time_t last;
	clock_t ct;
	newestEntry(q,&last,&ct);
	if (0 == last ) {
		last = time(NULL);
		ct = clock();
		putEntry(q, last,ct);
	}
	openJournalAtTime(last, ct, q->path, &q->write);
}

struct Queue * queue_open_with_options(const char * const path,... ) {
	va_list argp;
	va_start(argp, path);
	struct Queue * q = readoptions(argp);
	va_end(argp);
	if (F_OK != access(path, R_OK|W_OK)) {
		q-> error_strp = strdup("can not access path");
		return q;
	}
	q->path = strdup(path);
	setCountLengthByStatingFiles(q);
	return q;
}
struct Queue * queue_open(const char * const path) {
	return queue_open_with_options(path,NULL);
}

void queue_repair_with_options(const char * const path,... ) {
	va_list argp;
	va_start(argp, path);
	UNUSED struct Queue * q = readoptions(argp);
	va_end(argp);
}
void queue_repair(const char * path) {
	return queue_repair_with_options(path,NULL);
}
static void closeFileItr(struct FileItr * fip){
	if (fip->journalfd) fclose(fip->journalfd);
	fip->journalfd =NULL;
	if (fip->binlogfd) fclose(fip->binlogfd);
	fip->binlogfd=NULL;
}

int queue_close(struct Queue *q) {
	assert(q != NULL);
	IFFN(q->path);
	closeFileItr(&q->read);
	closeFileItr(&q->write);
	IFFN(q->error_strp);
	memset(q,0, sizeof(struct Queue));
	IFFN(q);
	return LIBQUEUE_SUCCESS;
}

int queue_push(struct Queue * const q, struct QueueData * const d) {
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	if (!fileItr_opened(&q->write))
		setFileToWriteTo(q);
	if (q->write.bsize + d->vlen > q->max_bin_log_size ) {
		closeFileItr (&q->write);
		time_t last = time(NULL);
		clock_t ct = clock();
		putEntry(q, last,ct);
		openJournalAtTime(last,ct, q->path, &q->write);
	}

	fseek(q->write.journalfd, 0, SEEK_END);
	fseek(q->write.binlogfd, 0, SEEK_END);
	struct JournalEntry entry;

	entry.offset = ftell(q->write.binlogfd);
	entry.size = d->vlen * fwrite(d->v, d->vlen, 1, q->write.binlogfd);
	fflush(q->write.binlogfd);
	entry.done = 0;
	fwrite((char *)&entry, sizeof(entry), 1, q->write.journalfd);

	fflush(q->write.journalfd);
	q->count++;
	q->write.bsize +=d->vlen;
	q->write.jsize += sizeof(entry);
	return LIBQUEUE_SUCCESS;
}
static int queue_peek_h(struct Queue * const q,  int64_t idx, struct QueueData * const d,struct JournalEntry  *je) {
	assert(q != NULL);
	assert(d != NULL);
	if (0 ==  fileItr_opened(&q->read) ) {
		time_t oldest ;
		clock_t ct;
		getOldestJournal(q, &oldest, &ct);
		if (0 ==oldest) {
			return LIBQUEUE_FAILURE;
		}
		openJournalAtTime(oldest,ct, q->path, &q->read);
	}
	if (!fileItr_opened(&q->read)) {
		return LIBQUEUE_FAILURE;
	}
	int read=0;
	while (1 == fread(je, sizeof(struct JournalEntry),1,q->read.journalfd )) {
		if (0 == je->done ) {
			read++;
			break;
		}
	}
	if (0 == read) {
		if (FILE_KEY_EQUAL(q->write.key, q->read.key)) {
			return LIBQUEUE_FAILURE;
		}
		char file[MAX_FILE_NAME];
		closeFileItr(&q->read);
		if (0 != unlink(getBinLogFileName(q->read.key.time,q->read.key.clock, q->path, file)) &&
				0 != unlink(getJournalFileName(q->read.key.time,q->read.key.clock, q->path, file)))  {
			queue_set_error(q, "failed to delete binlog or journal: ", strerror(errno));
			return LIBQUEUE_FAILURE;
		}
		setcatalogEntryDone(q,q->read.key.time,q->read.key.clock);
		return queue_peek_h(q,idx,d,je);
	}
	fseek(q->read.journalfd, -sizeof (struct JournalEntry),  SEEK_CUR );
	if (d) {
		d->vlen = je->size;
		d->v = malloc (d->vlen );
		fseek(q->read.binlogfd, je->offset,  SEEK_SET);
		fread(d->v,  d->vlen,1, q->read.binlogfd);
	}
	return LIBQUEUE_SUCCESS;
}
int queue_peek(struct Queue * const q, int64_t idx, struct QueueData * const d) {
	struct JournalEntry  je;
	return queue_peek_h(q, idx, d,&je);
}

int queue_pop(struct Queue * const q, struct QueueData * const d) {
	struct JournalEntry  je;
	if (LIBQUEUE_SUCCESS != queue_peek_h(q,0, d,&je))
		return LIBQUEUE_FAILURE;
	je.done = 1;
	if (1 != fwrite(&je, sizeof(je),1,q->read.journalfd )){
		queue_set_error(q, "failed to mark entry done: ", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	if (0 != fflush(q->read.journalfd)) {
		queue_set_error(q, "failed to flush mark entry done: ", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	q->count--;
	return LIBQUEUE_SUCCESS;
}

int queue_count(struct Queue * const q, int64_t * const countp) {
	assert(q != NULL);
	assert(countp != NULL);
	*countp=q->count;
	return LIBQUEUE_SUCCESS;

}
int queue_compact(struct Queue *q) {
	assert(q != NULL);
	return LIBQUEUE_SUCCESS;
}

int queue_len(struct Queue * const q, int64_t * const lenbuf) {
	assert(q != NULL);
	assert(lenbuf != NULL);
	if (0 == q->count )
		*lenbuf = 0;
	else
		*lenbuf =q->jour_size + q->bin_size + q->catalog_size;
	return LIBQUEUE_SUCCESS;
}

int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d){
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	return LIBQUEUE_SUCCESS;
}

