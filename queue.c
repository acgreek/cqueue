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

#define IFFN(X) {if (NULL != X) {free(X); X =NULL;}}
#define IFFNF(X,FUNC) {if (NULL != X) {FUNC(X); X =NULL;}}

#define MAX_FILE_NAME 1024

struct FileItr {
	time_t time;
	FILE *journalfd;
	FILE *binlogfd;
};

int fileItr_opened(struct FileItr *itrp ) {
	return itrp->journalfd != NULL;
}

struct Queue {
	char * path;
	struct FileItr read;
	struct FileItr write;
	char * error_strp;
	size_t count; // at startup we get the count by reading all the journals, then inc/dec as we push and pop
	size_t jour_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop
	size_t bin_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop
	size_t catalog_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop
};

const char * queue_get_last_error(const struct Queue * const q) {
	return q->error_strp;
}

int queue_is_opened (const struct Queue * const q) {
	return NULL != q->path;
}

static struct Queue * readoptions (va_list argp) {
	struct Queue * q = malloc(sizeof (struct Queue));
	memset(q, 0, sizeof(struct Queue));
	return q;
}
static const char *getFileName(const char *prefix, time_t time,const char * path, char *file) {
	snprintf(file, MAX_FILE_NAME-1,"%s/%s.%lu", path,prefix, (unsigned long)time);
	return file;
}

static const char *getBinLogFileName(time_t time,const char * path, char *file) {
	return getFileName("bin_log",time,path, file);
}

static const char *getJournalFileName(time_t time,const char * path, char *file) {
	return getFileName("journal",time,path, file);
}

int openJournalAtTime(time_t time,const char * path, struct FileItr * itr) {
	char file[MAX_FILE_NAME];
	itr->journalfd = fopen (getJournalFileName(time, path, file), "r+");
	if (NULL == itr->journalfd)
		itr->journalfd = fopen (file, "w+");
	getBinLogFileName(time,path, file);
	itr->binlogfd= fopen (getBinLogFileName(time, path, file), "r+");
	if (NULL == itr->binlogfd)
		itr->binlogfd= fopen (file, "w+");
	itr->time = time;
	return 0;
}


struct CatelogEntry {
	time_t time;
	char done;
};
struct JournalEntry {
	unsigned long offset;
	unsigned long size;
	char done;
};


static void setCountLengthByStatingFiles(struct Queue *q) {
	q->count =0;
	q->jour_size=0;
	q->bin_size=0;
	q->catalog_size=0;
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd)
		return ;
	struct CatelogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1, journalsfd)) {
		if (entry.done != 0)
			continue;
		struct stat bin_stat, jour_stat;
		if (0 == stat(getJournalFileName(entry.time, q->path, file), &jour_stat) &&
				0 == stat(getBinLogFileName(entry.time, q->path, file), &bin_stat)) {
			q->count += jour_stat.st_size / sizeof(struct JournalEntry) ;
			q->jour_size+= jour_stat.st_size;
			q->bin_size+= bin_stat.st_size;
		}
	}
	fclose (journalsfd);

}

static time_t getOldestJournal(struct Queue *q) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd)
		return 0;
	struct CatelogEntry entry;
	struct CatelogEntry oldest_entry;
	oldest_entry.time = ULONG_MAX;
	while (!feof(journalsfd)) {
		fread(&entry, sizeof(entry), 1, journalsfd);
		if (0 == entry.done && ((unsigned long) entry.time) < (unsigned long)oldest_entry.time) {
			oldest_entry = entry;
		}
	}
	fclose (journalsfd);
	return ULONG_MAX == oldest_entry.time ? 0 : oldest_entry.time;
}
static time_t newestEntry(struct Queue *q) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd)
		return 0;
	struct CatelogEntry entry;
	struct CatelogEntry newest_entry;
	newest_entry.time = 0;
	while (!feof(journalsfd)) {
		fread(&entry, sizeof(entry), 1, journalsfd);
		if (0 == entry.done && entry.time > newest_entry.time) {
			newest_entry = entry;
		}
	}
	fclose (journalsfd);
	return ULONG_MAX == newest_entry.time ? 0 : newest_entry.time;
}
static void setCatelogEntryDone(struct Queue *q,time_t time) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd){
		fprintf(stderr, "error opening catelog file %s: %s\n",file , strerror(errno));
		journalsfd = fopen(file, "w+");
		if (NULL == journalsfd){
			fprintf(stderr, "error create catelog file %s: %s\n",file , strerror(errno));
			return;
		}
	}
	struct CatelogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1, journalsfd)) {
		if (time  == entry.time) {
			fseek(journalsfd,-sizeof(entry), SEEK_CUR);
			entry.done= 1;
			fwrite(&entry, sizeof(entry), 1, journalsfd);
			fflush(journalsfd);
			break;
		}
	}
	fclose (journalsfd);
}
static void putEntry(struct Queue *q, time_t time) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * journalsfd = fopen(file, "r+");
	if (NULL == journalsfd){
		journalsfd = fopen(file, "w+");
		if (NULL == journalsfd){
			fprintf(stderr, "error create catelog file %s: %s\n",file , strerror(errno));
			return;
		}
	}
	struct CatelogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1, journalsfd)) {
		if (1 == entry.done) {
			fseek(journalsfd,-sizeof(entry), SEEK_CUR);
			break;
		}
	}
	entry.done= 0;
	entry.time = time;
	fwrite(&entry, sizeof(entry), 1, journalsfd);
	fflush(journalsfd);
	fclose(journalsfd);
}

static void setFileToWriteTo(struct Queue * q) {
	time_t last = newestEntry(q);
	if (0 != last ) {
		openJournalAtTime(last, q->path, &q->write);
	}
	else {
		last = time(NULL);
		putEntry(q, last);
	}
	openJournalAtTime(last, q->path, &q->write);
}

struct Queue * queue_open_with_options(const char * const path,... ) {
	va_list argp;
	va_start(argp, path);
	struct Queue * q = readoptions(argp);
	va_end(argp);
	if (F_OK != access(path, R_OK|W_OK)) {
		printf("can't access: %s\n", path);
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
#define UNUSED __attribute__((unused))

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
	IFFN(q);
	return LIBQUEUE_SUCCESS;
}

int queue_push(struct Queue * const q, struct QueueData * const d) {
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	if (!fileItr_opened(&q->write))
		setFileToWriteTo(q);
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
	return LIBQUEUE_SUCCESS;
}

int queue_pop(struct Queue * const q, struct QueueData * const d) {
	assert(q != NULL);
	if (NULL == q->read.journalfd ) {
	}
	if (0 ==  fileItr_opened(&q->read) ) {
		time_t oldest = getOldestJournal(q);
		if (0 ==oldest) {
			return LIBQUEUE_FAILURE;
		}
		openJournalAtTime(oldest, q->path, &q->read);
	}
	if (!fileItr_opened(&q->read)) {
		return LIBQUEUE_FAILURE;

	}
	struct JournalEntry  je;
	int read=0;
	while (1 == fread(&je, sizeof(je),1,q->read.journalfd )) {
		if (0 == je.done ) {
			read++;
			break;
		}
	}
	if (0 == read) {
		if (q->write.time == q->read.time) {
			// not done with file yet
			return LIBQUEUE_FAILURE;
		}
		char file[MAX_FILE_NAME];
		closeFileItr(&q->read);
		unlink(getBinLogFileName(q->read.time, q->path, file));
		unlink(getJournalFileName(q->read.time, q->path, file));
		setCatelogEntryDone(q,q->read.time);
		return queue_pop(q,d);
	}
	fseek(q->read.journalfd, -sizeof (je),  SEEK_CUR );
	if (d) {
		d->vlen = je.size;
		d->v = malloc (d->vlen );
		fseek(q->read.binlogfd, je.offset,  SEEK_SET);
		fread(d->v,  d->vlen,1, q->read.binlogfd);
	}
	je.done = 1;
	fwrite(&je, sizeof(je),1,q->read.journalfd );
	fflush(q->read.journalfd);
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
	*lenbuf =q->jour_size + q->bin_size + q->catalog_size;
	return LIBQUEUE_SUCCESS;
}

int queue_peek(struct Queue * const q, int64_t idx, struct QueueData * const d) {
	assert(q != NULL);
	assert(d != NULL);
	return LIBQUEUE_SUCCESS;
}
int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d){
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	return LIBQUEUE_SUCCESS;
}

