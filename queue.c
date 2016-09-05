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

#define DEFAULT_MAX_Q_SIZE_IN_BYTES 2lu*1024*1024*1024
#define DEFAULT_MAX_Q_ENTRIES  ULONG_MAX

#define FILE_KEY_EQUAL(A,B) (A.time == B.time && A.clock == B.clock)
#define FILE_KEY_GREATER(A,B) ((unsigned long)A.time > (unsigned long)B.time || (A.time == B.time && (unsigned long)A.clock > (unsigned long)B.clock))
#define FILE_KEY_LESS(A,B) ((unsigned long)A.time < (unsigned long)B.time || ((unsigned long)A.time == (unsigned long)B.time && (unsigned long)A.clock < (unsigned long)B.clock))

typedef struct _FileKey {
	time_t time;
	time_t clock;
}FileKey;

struct FileItr {
	FileKey key;
	ssize_t catalogIdx;
	FILE *journalfd;
	FILE *binlogfd;
	ssize_t jsize;
	ssize_t bsize;
};


struct Queue {
	char * path;
	FILE * catalogFd;

	// push and pop iter
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
	ssize_t max_size_in_bytes;
	ssize_t max_entries;
};

struct catalogEntry {
	FileKey key;
	char done;
};

struct JournalEntry {
	unsigned long offset;
	unsigned long size;
	char done;
};

int fileItr_opened(struct FileItr *itrp ) {
	return itrp->journalfd != NULL && itrp->binlogfd != NULL;
}

const char * queue_get_last_error(const struct Queue * const q) {
	return q->error_strp;
}
static void queue_set_error(struct Queue *const q, const char *what, const char *errstr) {
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
	q->max_size_in_bytes = DEFAULT_MAX_Q_SIZE_IN_BYTES;
	q->max_entries = DEFAULT_MAX_Q_ENTRIES;
	for (p = va_arg(argp, char *); p != NULL; p = va_arg(argp,char *)) {
		if (0 == strcmp(p, "maxBinLogSize")) {
			q->max_bin_log_size= va_arg(argp, ssize_t);
		}
		if (0 == strcmp(p, "maxSizeInBytes")) {
			q->max_size_in_bytes= va_arg(argp, ssize_t);
		}
		if (0 == strcmp(p, "maxEntries")) {
			q->max_entries= va_arg(argp, ssize_t);
		}
		if (0 == strcmp(p, "failIfMissing")) {
		   q-> fail_if_missing = 1;
		}
	}
	return q;
}
static const char *getFileName(const char *prefix, FileKey *keyp ,const char * path, char *file) {
	snprintf(file, MAX_FILE_NAME-1,"%s/%s.%lu.%lu", path,prefix, (unsigned long)keyp->time,(unsigned long)keyp->clock);
	return file;
}

static const char *getBinLogFileName(FileKey *keyp, const char * path, char *file) {
	return getFileName("bin_log",keyp,path, file);
}

static const char *getJournalFileName(FileKey *keyp,const char * path, char *file) {
	return getFileName("journal",keyp,path, file);
}

static ssize_t getFileSize(FILE * fd) {
	if (NULL == fd) return 0;
	struct stat stat;
	fstat(fileno(fd), &stat);
	return stat.st_size;
}

static int openJournalAtTime(FileKey * keyp, const char * path, struct FileItr * itr) {
	char file[MAX_FILE_NAME];
	itr->journalfd = fopen (getJournalFileName(keyp, path, file), "r+");
	if (NULL == itr->journalfd)
		itr->journalfd = fopen (file, "w+");
	if (NULL ==itr->journalfd ) {
		return -1;
	}
	setbuf(itr->journalfd, NULL);
	itr->jsize = getFileSize(itr->journalfd);
	getBinLogFileName(keyp,path, file);
	itr->binlogfd= fopen (getBinLogFileName(keyp, path, file), "r+");
	if (NULL == itr->binlogfd)
		itr->binlogfd= fopen (file, "w+");
	if (NULL ==itr->binlogfd) {
		return -1;
	}
	setbuf(itr->binlogfd, NULL);
	itr->bsize = getFileSize(itr->binlogfd);
	itr->key = *keyp;
	return 0;
}

/**
 * @return number of entries in the journal that are marked done
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

static int setCountLengthByStatingFiles(struct Queue *q) {
	q->count = q->jour_size= q->bin_size= q->catalog_size=0;
	fseek(q->catalogFd, 0, SEEK_SET);
	struct catalogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1,q->catalogFd)) {
		if (entry.done != 0)
			continue;
		struct stat bin_stat, jour_stat;
		char file[MAX_FILE_NAME];
		if (0 == stat(getJournalFileName(&entry.key, q->path, file), &jour_stat) &&
				0 == stat(getBinLogFileName(&entry.key,  q->path, file), &bin_stat)) {
			q->count += jour_stat.st_size / sizeof(struct JournalEntry) ;
			q->count -=doneEntries(getJournalFileName(&entry.key, q->path, file));
			q->jour_size+= jour_stat.st_size;
			q->bin_size+= bin_stat.st_size;
		}
		else {
			// this next line makes the queue not openned. Don't like
			IFFN(q->path);
			queue_set_error(q, "journal or binlog file is missing", "");
			return LIBQUEUE_FAILURE;
		}
	}
	struct stat cat_stat;
	fstat(fileno(q->catalogFd), &cat_stat);
	q->catalog_size+= cat_stat.st_size;
	return LIBQUEUE_SUCCESS;
}

static FileKey getNextOldestJournal(const struct Queue *q, FileKey *oldkey) {
	FileKey key = {0,0};
	struct catalogEntry entry;
	struct catalogEntry oldest_entry = {{ULONG_MAX,ULONG_MAX}, 0 };
	fseek(q->catalogFd, 0, SEEK_SET);
	while (1 == fread(&entry, sizeof(entry), 1, q->catalogFd)) {
		if (0 == entry.done && FILE_KEY_LESS(entry.key,oldest_entry.key) &&
				FILE_KEY_GREATER(entry.key, (*oldkey))) {
			oldest_entry = entry;
		}
	}
	if (ULONG_MAX != oldest_entry.key.time) {
		return oldest_entry.key;
	}
	return key;
}
static FileKey getOldestJournal(const struct Queue *q) {
	FileKey key = {0,0};
	return getNextOldestJournal(q,&key);
}

/**
 * @return 1 if newest entry found, 0 there are no entries
 */
static int newestEntry(struct Queue *q,FileKey * key) {
	struct catalogEntry entry;
	struct catalogEntry newest_entry;
	newest_entry.key.time = 0;
	fseek(q->catalogFd, 0, SEEK_SET);
	while (1 == fread(&entry, sizeof(entry), 1, q->catalogFd)) {
		if (0 == entry.done && ( FILE_KEY_GREATER(entry.key,newest_entry.key))) {
			newest_entry = entry;
		}
	}
	if (0 != newest_entry.key.time) {
		*key = newest_entry.key;
		return 1;
	}
	return 0;
}
static int writeAndFlushData(FILE *file, const void * data, ssize_t size) {
	if (1 != fwrite(data, size, 1, file) ){
		return LIBQUEUE_FAILURE;
	}
	if (-1 == fflush(file)) {
		return LIBQUEUE_FAILURE;
	}
	return LIBQUEUE_SUCCESS;
}


static int setcatalogEntryDone(struct Queue *q,FileKey * keyp) {
	fseek(q->catalogFd, 0, SEEK_SET);
	struct catalogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1,q->catalogFd)) {
		if (FILE_KEY_EQUAL((*keyp),entry.key)) {
			fseek(q->catalogFd,-sizeof(entry), SEEK_CUR);
			entry.done= 1;
			if (LIBQUEUE_FAILURE == writeAndFlushData(q->catalogFd, &entry, sizeof(entry))) {
				queue_set_error(q, "failed update catalog: ", strerror(errno));
				return LIBQUEUE_FAILURE;
			}
			break;
		}
	}
	return LIBQUEUE_SUCCESS;
}
static FILE * openCatalog(struct Queue *q) {
	char file[MAX_FILE_NAME];
	snprintf(file, sizeof(file)-1,"%s/catalog", q->path);
	FILE * catalogFd = fopen(file, "r+");
	if (NULL == catalogFd){
		catalogFd= fopen(file, "w+");
		if (NULL ==catalogFd){
			fprintf(stderr, "error create catalog file %s: %s\n",file , strerror(errno));
			return NULL;
		}
	}
	setbuf(catalogFd, NULL);
	flock(fileno(catalogFd), LOCK_EX);
	return catalogFd;
}

static int putEntry(struct Queue *q, const FileKey const  * keyp) {
	if (-1 == fseek(q->catalogFd, 0, SEEK_SET)) {
		queue_set_error(q, "failed to seek to start of  catalog", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	struct catalogEntry entry;
	int replacing=0;
	while (1 == fread(&entry, sizeof(entry), 1,q->catalogFd)) {
		if (1 == entry.done) {
			replacing=1;
			fseek(q->catalogFd,-sizeof(entry), SEEK_CUR);
			break;
		}
	}
	entry.done= 0;
	entry.key = *keyp;
	if (LIBQUEUE_FAILURE == writeAndFlushData(q->catalogFd, &entry, sizeof(entry))) {
		queue_set_error(q, "failed to update catalog", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	if (0 == replacing) {
		q->catalog_size  +=sizeof(entry);
	}
	return LIBQUEUE_SUCCESS;
}

static void setFileToWriteTo(struct Queue * q) {
	FileKey key;
	if (!newestEntry(q,&key)) {
		key.time= time(NULL);
		key.clock= clock();
		putEntry(q, &key);
	}
	if (-1 == openJournalAtTime(&key, q->path, &q->write)) {
		queue_set_error(q, "failed to binlog or journal: ", strerror(errno));
	}
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
	q->catalogFd = openCatalog(q);
	if (q->fail_if_missing) {
		return q;
	}
	setCountLengthByStatingFiles(q);
	return q;
}
struct Queue * queue_open(const char * const path) {
	return queue_open_with_options(path,NULL);
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
	// catalog should be the last file to close to prevent race on the queue
	if (NULL != q->catalogFd) {
		flock(fileno(q->catalogFd), LOCK_UN);
		fclose(q->catalogFd);
	}
	IFFN(q->error_strp);
	memset(q,0, sizeof(struct Queue));
	IFFN(q);
	return LIBQUEUE_SUCCESS;
}

int queue_push(struct Queue * const q, struct QueueData * const d) {
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	if (q->count +1 > q->max_entries) {
		queue_set_error(q, "max entries reached","");
		return LIBQUEUE_FAILURE;
	}
	if ((q->bin_size + q->jour_size + q->catalog_size +d->vlen)  > q->max_size_in_bytes) {
		queue_set_error(q, "max size in bytes would be exceeded","");
		return LIBQUEUE_FAILURE;
	}

	if (!fileItr_opened(&q->write))
		setFileToWriteTo(q);
	if (q->write.bsize + d->vlen > q->max_bin_log_size ) {
		closeFileItr (&q->write);
		FileKey key= {time(NULL), clock()};
		putEntry(q, &key);
		if (-1 == openJournalAtTime(&key, q->path, &q->write)) {
			queue_set_error(q, "failed to open binlog or journal", strerror(errno));
			return LIBQUEUE_FAILURE;
		}
	}
	if (-1 == fseek(q->write.journalfd, 0, SEEK_END) ) {
		queue_set_error(q, "failed to seek to end of journal", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	if (-1 == fseek(q->write.binlogfd, 0, SEEK_END)) {
		queue_set_error(q, "failed to seek to end of binlog", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	struct JournalEntry entry;

	entry.offset = ftell(q->write.binlogfd);
	entry.size = d->vlen;
	entry.done = 0;
	if (LIBQUEUE_FAILURE == writeAndFlushData(q->write.binlogfd, d->v, d->vlen)) {
		queue_set_error(q, "failed to write data to binlog ", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	if (LIBQUEUE_FAILURE == writeAndFlushData(q->write.journalfd,(char *)&entry, sizeof(entry))) {
		queue_set_error(q, "failed to write data to binlog ", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	q->count++;
	q->write.bsize +=d->vlen;
	q->write.jsize += sizeof(entry);
	return LIBQUEUE_SUCCESS;
}

static int queue_peek_h(struct Queue * const q,  int64_t idx, struct QueueData * const d,struct JournalEntry  *je) {
	assert(q != NULL);
	assert(d != NULL);
	if (0 ==  fileItr_opened(&q->read) ) {
		FileKey key =  getOldestJournal(q);
		if (0 ==key.time) {
			return LIBQUEUE_FAILURE;
		}
		openJournalAtTime(&key, q->path, &q->read);
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
		if (0 != unlink(getBinLogFileName(&q->read.key, q->path, file)) &&
				0 != unlink(getJournalFileName(&q->read.key, q->path, file)))  {
			queue_set_error(q, "failed to delete binlog or journal: ", strerror(errno));
			return LIBQUEUE_FAILURE;
		}
		setcatalogEntryDone(q,&q->read.key);
		return queue_peek_h(q,idx,d,je);
	}
	fseek(q->read.journalfd, -sizeof (struct JournalEntry),  SEEK_CUR );
	if (d) {
		d->vlen = je->size;
		d->v = malloc (d->vlen );
		fseek(q->read.binlogfd, je->offset,  SEEK_SET);
		if (1 != fread(d->v,  d->vlen,1, q->read.binlogfd)) {
			return LIBQUEUE_FAILURE;
		}
	}
	return LIBQUEUE_SUCCESS;
}

static int queue_index_lookup(const struct Queue * const q,  int64_t idx, struct FileItr * itr,struct QueueData * const d, struct JournalEntry  *je) {
	assert(q != NULL);
	assert(d != NULL);
	if (0 ==  fileItr_opened(itr) ) {
		FileKey key =  getOldestJournal(q);
		if (0 ==key.time) {
			queue_set_error((struct Queue *)q,"queue is empty","");
			return LIBQUEUE_FAILURE;
		}
		openJournalAtTime(&key, q->path, itr);
	}
	if (!fileItr_opened(itr)) {
		return LIBQUEUE_FAILURE;
	}
	int read=0;
	while (1 == fread(je, sizeof(struct JournalEntry),1,itr->journalfd )) {
		if (0 == je->done ) {
			read++;
			if (0 == idx)
				break;
			read--;
			idx--;
		}
	}
	if (0 == read || idx > 0) {
		if (FILE_KEY_EQUAL(q->write.key, itr->key)) {
			return LIBQUEUE_FAILURE;
		}
		closeFileItr(itr);

		itr->key = getNextOldestJournal(q, &(itr->key));
		if (0 == itr->key.time )
			return LIBQUEUE_FAILURE;
		openJournalAtTime(&itr->key, q->path, itr);
		return queue_index_lookup(q,idx,itr, d,je);
	}
	fseek(itr->journalfd, -sizeof (struct JournalEntry),  SEEK_CUR );
	if (d) {
		d->vlen = je->size;
		d->v = malloc (d->vlen );
		fseek(itr->binlogfd, je->offset,  SEEK_SET);
		if ( 1 !=  fread(d->v,  d->vlen,1, itr->binlogfd)) {
			return LIBQUEUE_FAILURE;
		}
	}
	return LIBQUEUE_SUCCESS;
}


/**
 * NOTE, you can only peek 0, idx not implemented yet
 */
int queue_peek(struct Queue * const q, int64_t idx, struct QueueData * const d) {
	struct FileItr itr;
	memset(&itr, 0, sizeof(itr));
	struct JournalEntry  je;
	int results = queue_index_lookup(q,  idx, &itr,d, &je);
	closeFileItr(&itr);
	return results;
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

/**
 * not implemented
 */
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

/**
 * not implemented
 */
int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d){
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	struct FileItr itr;
	memset(&itr, 0, sizeof(itr));
	struct JournalEntry  je;
	int results = queue_index_lookup(q,  idx, &itr,NULL, &je);
	if (results == LIBQUEUE_FAILURE) {
		closeFileItr(&itr);
		return LIBQUEUE_FAILURE;
	}
	if (je.size < d->vlen) {
		queue_set_error(q, "size of existing entry smaller that data needing to be replaced","");
		closeFileItr(&itr);
		return LIBQUEUE_FAILURE;
	}
	fseek(itr.binlogfd, je.offset,  SEEK_SET);
	if (LIBQUEUE_FAILURE == writeAndFlushData(itr.binlogfd, d->v, d->vlen)) {
		queue_set_error(q, "failed to update binlog with poke replacement data", "");
		closeFileItr(&itr);
		return LIBQUEUE_FAILURE;
	}
	if (je.size != d->vlen) {
		if (LIBQUEUE_FAILURE == writeAndFlushData(itr.journalfd, &je, sizeof(je))) {
			queue_set_error(q, "failed to update journal with poke replacement size","");
			closeFileItr(&itr);
			return LIBQUEUE_FAILURE;
		}
	}

	closeFileItr(&itr);
	return LIBQUEUE_SUCCESS;
}

static void correctAllJournalEntries(struct FileItr* itr) {
	fseek(itr->journalfd, 0, SEEK_SET);
	struct JournalEntry entry;
	struct JournalEntry nentry;
	ssize_t previous_end = 0;
	ssize_t file_size= getFileSize(itr->binlogfd);
	ssize_t next_start = file_size;
	while (1 == fread(&entry, sizeof(entry), 1,itr->journalfd)) {
		if (1 == fread(&nentry, sizeof(entry), 1,itr->journalfd)) {
			next_start = nentry.offset;
			fseek(itr->journalfd,-sizeof(entry), SEEK_CUR);
		}
		else {
			next_start = file_size ;
		}

		if (entry.offset < previous_end  || entry.offset +entry.size > next_start) {
			fseek(itr->journalfd,-sizeof(entry), SEEK_CUR);
			entry.done= 1;
			if (LIBQUEUE_FAILURE == writeAndFlushData(itr->journalfd, &entry, sizeof(entry))) {
				// what do we do
			}
		}
	}

}
static int fix_catalog_entries(struct Queue *q) {
	q->count = q->jour_size= q->bin_size= q->catalog_size=0;
	fseek(q->catalogFd, 0, SEEK_SET);
	struct catalogEntry entry;
	while (1 == fread(&entry, sizeof(entry), 1,q->catalogFd)) {
		if (entry.done != 0)
			continue;
		struct stat bin_stat, jour_stat;
		char file[MAX_FILE_NAME];
		if (0 == stat(getJournalFileName(&entry.key, q->path, file), &jour_stat) &&
				0 == stat(getBinLogFileName(&entry.key,  q->path, file), &bin_stat)) {
			// ok, now we need to go through the entries and check that they are correct
			if (-1 == openJournalAtTime(&entry.key, q->path, &q->write)) {
				//hmm ,what do I do now
			}
			else {
				correctAllJournalEntries(&q->write);
				closeFileItr(&q->write);
			}
		}
		else {
			unlink(getJournalFileName(&entry.key, q->path, file));
			unlink(getBinLogFileName(&entry.key, q->path, file));
			ssize_t offset = fseek(q->catalogFd, 0, SEEK_CUR);
			setcatalogEntryDone(q,&entry.key);
			fseek(q->catalogFd, offset, SEEK_SET);
		}
	}
	struct stat cat_stat;
	fstat(fileno(q->catalogFd), &cat_stat);
	q->catalog_size+= cat_stat.st_size;
	return LIBQUEUE_SUCCESS;
}
/**
 * not implemented yet
 */
void queue_repair_with_options(const char * const path,... ) {
	va_list argp;
	va_start(argp, path);
	UNUSED struct Queue * q = readoptions(argp);
	va_end(argp);

	q->path = strdup(path);
	q->catalogFd = openCatalog(q);
	fix_catalog_entries(q);
	queue_close(q);
}

/**
 * not implemented yet
 */
void queue_repair(const char * path) {
	return queue_repair_with_options(path,NULL);
}

