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
#include <dirent.h>

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
	FILE *binlogfd;
	ssize_t bsize;
};


struct Queue {
	char * path;

	// push and pop iter
	struct FileItr read;
	struct FileItr write;

	// error string holder
	char * error_strp;

	// stats
	size_t count; // at startup we get the count by reading all the journals, then inc/dec as we push and pop
	size_t bin_size; //  at startup we get the count by reading all the journal bin log size, then inc/dec as we push and pop

	//settings
	size_t max_bin_log_size; //
	char   fail_if_missing;
	ssize_t max_size_in_bytes;
	ssize_t max_entries;
};

struct JournalEntry {
	unsigned long size;
	unsigned long csum;
	char done;
};

struct Footer{
	ssize_t offsetToJournalEntry;
};

int fileItr_opened(struct FileItr *itrp ) WARN_UNUSED_RETURN;
int fileItr_opened(struct FileItr *itrp ) {
	return itrp->binlogfd != NULL;
}

const char * queue_get_last_error(const struct Queue * const q) WARN_UNUSED_RETURN;
const char * queue_get_last_error(const struct Queue * const q) {
	return q->error_strp;
}
static void queue_set_error(struct Queue *const q, const char *what, const char *errstr) {
	IFF(q->error_strp);
	char * ptr;
	asprintf(&ptr, "%s %s", what, errstr);
	q->error_strp= ptr;
}

int queue_is_opened (const struct Queue * const q) WARN_UNUSED_RETURN;
int queue_is_opened (const struct Queue * const q) {
	return NULL != q->path;
}

static struct Queue * readoptions (va_list argp) WARN_UNUSED_RETURN;
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

static ssize_t getFileSize(FILE * fd) WARN_UNUSED_RETURN;
static ssize_t getFileSize(FILE * fd) {
	if (NULL == fd) return 0;
	struct stat stat;
	fstat(fileno(fd), &stat);
	return stat.st_size;
}

static unsigned int chksum(char *d, ssize_t size) WARN_UNUSED_RETURN;
static unsigned int chksum(char *d, ssize_t size) {
	unsigned int sum =0;
	while (size) {
		switch (size) {
			default:
				sum += *d;
				d++;size--;
			case 3:
				sum += *d;
				d++;size--;
			case 2:
				sum += *d;
				d++;size--;
			case 1:
				sum += *d;
				d++;size--;
		}
	}
	return sum;
}

static int chopOffIncompleteWrite(FILE *fd) WARN_UNUSED_RETURN;
static int chopOffIncompleteWrite(FILE *fd) {
	fseek(fd, 0, SEEK_SET);
	struct Footer foot;
	struct JournalEntry je;
	ssize_t goodOffset = 0;
	while (1 == fread(&je, sizeof (je),1, fd)) {
		if (-1 == fseek (fd, je.size, SEEK_CUR)) {
			// data did not complete write
			return ftruncate (fileno(fd), goodOffset);
		}
		if (0 == fread(&foot, sizeof(foot), 1, fd)) {
			// foot was not written
			return ftruncate (fileno(fd), goodOffset);
		}
		if (foot.offsetToJournalEntry != goodOffset)  {
			// foot is corrupt
			return ftruncate (fileno(fd), goodOffset);
		}
		goodOffset = ftell(fd);
	}
	return 0;
}

static int checkLastEntry(FILE *fd, ssize_t filesize) WARN_UNUSED_RETURN;
static int checkLastEntry(FILE *fd, ssize_t filesize) {
	fseek(fd, - sizeof(struct Footer), SEEK_END);
	struct Footer foot;
	int rtn = fread(&foot, sizeof(foot), 1, fd);
	if (rtn != 0 && rtn != 1) {
		return -1;
	}
	if (foot.offsetToJournalEntry > (filesize-sizeof(foot) )) {
		if (0 != chopOffIncompleteWrite(fd)) {
			return -1;
		}
	}
	fseek(fd, foot.offsetToJournalEntry , SEEK_SET);
	struct JournalEntry je = {0};

	rtn = fread(&je, sizeof(je), 1, fd);
	if (rtn != 1 &&  rtn != 0) {
		return -1;
	}
	if (filesize == foot.offsetToJournalEntry+ je.size + sizeof(foot))
		return 0; // we are good
	return chopOffIncompleteWrite(fd);
}

static int openJournalAtTime(FileKey * keyp, const char * path, struct FileItr * itr) WARN_UNUSED_RETURN;
static int openJournalAtTime(FileKey * keyp, const char * path, struct FileItr * itr) {
	char file[MAX_FILE_NAME];
	getBinLogFileName(keyp,path, file);
	itr->binlogfd= fopen (getBinLogFileName(keyp, path, file), "r+");
	if (NULL == itr->binlogfd)
		itr->binlogfd= fopen (file, "w+");
	if (NULL ==itr->binlogfd) {
		return -1;
	}
	//setbuf(itr->binlogfd, NULL);
	itr->bsize = getFileSize(itr->binlogfd);
	itr->key = *keyp;
	if (0 == itr->bsize )
		return 0;
	if (itr->bsize <  (sizeof(struct JournalEntry) + sizeof(struct Footer))) {
		//must not of completed the write of the first entry of the file
		return ftruncate (fileno(itr->binlogfd), 0);
	} else {
		if (0 != checkLastEntry(itr->binlogfd, itr->bsize)) {
			return -1;
		}
	}
	return 0 != fseek(itr->binlogfd,0,SEEK_SET) ? -1 : 0;
}

/**
 * @return number of entries in the journal that are marked done
 */
static ssize_t countEntries(const char * file) WARN_UNUSED_RETURN;
static ssize_t countEntries(const char * file) {
	FILE * cf = fopen(file, "r");
	if (NULL == cf) {
		return 0;
	}
	struct JournalEntry je;
	ssize_t count=0;
	while (1 == fread(&je, sizeof(je),1, cf)) {
		if (0 == je.done)
			count++;
		fseek(cf, je.size + sizeof(struct Footer), SEEK_CUR);
	}
	fclose(cf);
	return count;
}

static int binlogfilter(const struct dirent *ent) WARN_UNUSED_RETURN;
static int binlogfilter(const struct dirent *ent) {
	return (0 == strncmp(ent->d_name, "bin_log.", 8))? 1 : 0;
}

static void getTimeClockFromName(const char * d_name, time_t *timep,clock_t *clockp) {
	char * clockstrp;
	*timep = strtoull(d_name +8, &clockstrp, 10);
	*clockp = strtoull(clockstrp+1, NULL, 10);
}
static int binlogsort(const struct dirent ** app, const struct dirent ** bpp) WARN_UNUSED_RETURN;
static int binlogsort(const struct dirent ** app, const struct dirent ** bpp) {
	time_t timea, timeb;
	clock_t clocka, clockb;
	getTimeClockFromName((*app)->d_name, &timea,&clocka);
	getTimeClockFromName((*bpp)->d_name, &timeb,&clockb);
	return timea == timeb ? clocka -clockb : timea - timeb;
}

static int setCountLengthByStatingFiles(struct Queue *q) {
	q->count = q->bin_size= 0;
	struct stat bin_stat;
	struct dirent **namelist;
	char file[MAX_FILE_NAME];
	int n;
	n = scandir(q->path, &namelist, binlogfilter, binlogsort);
	if (n < 0) {
		queue_set_error(q, "failed to scan dir", strerror(errno));
		return LIBQUEUE_FAILURE;
	} else {
		while (n--) {
			FileKey key;
			getTimeClockFromName(namelist[n]->d_name, &key.time,&key.clock);
			if (0 == stat(getBinLogFileName(&key,  q->path, file), &bin_stat)) {
				q->count =countEntries(getBinLogFileName(&key, q->path, file));
				q->bin_size+= bin_stat.st_size;
			}
			free(namelist[n]);
		}
		free(namelist);
	}
	return LIBQUEUE_SUCCESS;
}

static FileKey getNextOldestJournal(const struct Queue *q, FileKey *oldkey) WARN_UNUSED_RETURN;
static FileKey getNextOldestJournal(const struct Queue *q, FileKey *oldkey) {
	FileKey key = {0,0};
	FileKey ckey= {0,0};
	FileKey oldestkey = {ULONG_MAX,ULONG_MAX};
	struct dirent **namelist;
	int n = scandir(q->path, &namelist, binlogfilter, binlogsort);
	if (n < 0) {
		queue_set_error((struct Queue *)q, "failed to scan dir", strerror(errno));
		return key;
	} else {
		while (n--) {
			getTimeClockFromName(namelist[n]->d_name, &ckey.time,&ckey.clock);
			if (FILE_KEY_LESS(ckey,oldestkey) &&
					FILE_KEY_GREATER(ckey, (*oldkey))) {
				oldestkey = ckey;
			}
			free(namelist[n]);
		}
		free(namelist);
	}
	if (ULONG_MAX != oldestkey.time) {
		return oldestkey;
	}
	return key;
}
static FileKey getOldestJournal(const struct Queue *q) WARN_UNUSED_RETURN;
static FileKey getOldestJournal(const struct Queue *q) {
	FileKey key = {0, 0};
	return getNextOldestJournal(q, &key);
}

/**
 * @return 1 if newest entry found, 0 there are no entries
 */
static int newestEntry(struct Queue *q, FileKey * key) WARN_UNUSED_RETURN;
static int newestEntry(struct Queue *q, FileKey * key) {
	int found =0;
	struct dirent **namelist;
	int n;
	if (NULL == q->path) {
		return 0;
	}
	n = scandir(q->path, &namelist, binlogfilter, binlogsort);
	if (n < 0) {
		queue_set_error((struct Queue *)q, "failed to scan dir", strerror(errno));
		return 0;
	} else {
		if (n > 0) {
			getTimeClockFromName(namelist[n-1]->d_name, &(key->time),&(key->clock));
			found =1;
		}
		while (n--) {
			free(namelist[n]);
		}
		free(namelist);
	}
	return found;
}
static int writeAndFlushData(FILE *file, const void * data, ssize_t size) WARN_UNUSED_RETURN;
static int writeAndFlushData(FILE *file, const void * data, ssize_t size) {
	if (1 != fwrite(data, size, 1, file) ) {
		return LIBQUEUE_FAILURE;
	}
	if (-1 == fflush(file)) {
		return LIBQUEUE_FAILURE;
	}
	return LIBQUEUE_SUCCESS;
}


static void setFileToWriteTo(struct Queue * q) {
	FileKey key;
	if (!newestEntry(q,&key)) {
		key.time= time(NULL);
		key.clock= clock();
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
	if (fip->binlogfd) {
		fclose(fip->binlogfd);
	}
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
	if (q->count +1 > q->max_entries) {
		queue_set_error(q, "max entries reached","");
		return LIBQUEUE_FAILURE;
	}
	if ((q->bin_size + d->vlen + sizeof(struct JournalEntry) + sizeof(struct Footer))  > q->max_size_in_bytes) {
		queue_set_error(q, "max size in bytes would be exceeded","");
		return LIBQUEUE_FAILURE;
	}
	if (!fileItr_opened(&q->write)) {
		setFileToWriteTo(q);
	}
	if (!fileItr_opened(&q->write)) {
		queue_set_error(q, "failed to open bin log", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	if (q->write.bsize + d->vlen > q->max_bin_log_size ) {
		closeFileItr (&q->write);
		FileKey key= {time(NULL), clock()};
		if (-1 == openJournalAtTime(&key, q->path, &q->write)) {
			queue_set_error(q, "failed to open binlog or journal", strerror(errno));
			return LIBQUEUE_FAILURE;
		}
	}
	if (-1 == fseek(q->write.binlogfd, 0, SEEK_END)) {
		queue_set_error(q, "failed to seek to end of binlog", strerror(errno));
		return LIBQUEUE_FAILURE;
	}
	struct JournalEntry entry;
	entry.size = d->vlen;
	entry.csum= chksum(d->v, d->vlen);
	entry.done = 0;
	struct Footer foot;
	foot.offsetToJournalEntry = ftell (q->write.binlogfd);
	if (1 != fwrite((char *)&entry, sizeof(entry),1, q->write.binlogfd)  ||
			1 != fwrite(d->v, d->vlen,1, q->write.binlogfd)  ||
			1 != fwrite(&foot,sizeof(foot),1, q->write.binlogfd)  ) {
		queue_set_error(q, "failed to write data to binlog ", strerror(errno));
		fseek(q->read.binlogfd,foot.offsetToJournalEntry , SEEK_SET);
		return LIBQUEUE_FAILURE;
	}
	if ( 0 !=  fflush(q->write.binlogfd)) {
		queue_set_error(q, "failed to write data to binlog ", strerror(errno));
		fseek(q->read.binlogfd,foot.offsetToJournalEntry , SEEK_SET);
		return LIBQUEUE_FAILURE;
	}
	q->count++;
	q->write.bsize +=d->vlen;
	q->write.bsize += sizeof(entry);
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
		if (0 != openJournalAtTime(&key, q->path, &q->read)) {
			return LIBQUEUE_FAILURE;
		}

	}
	if (!fileItr_opened(&q->read)) {
		return LIBQUEUE_FAILURE;
	}
	int read=0;
	while (1 == fread(je, sizeof(struct JournalEntry),1,q->read.binlogfd)) {
		if (0 == je->done ) {
			read++;
			break;
		}
		fseek(q->read.binlogfd,je->size + sizeof(struct Footer), SEEK_CUR);
	}
	if (0 == read) {
		if (FILE_KEY_EQUAL(q->write.key, q->read.key)) {
			return LIBQUEUE_FAILURE;
		}
		char file[MAX_FILE_NAME];
		closeFileItr(&q->read);
		if (0 != unlink(getBinLogFileName(&q->read.key, q->path, file)))   {
			queue_set_error(q, "failed to delete binlog : ", strerror(errno));
			return LIBQUEUE_FAILURE;
		}
		return queue_peek_h(q,idx,d,je);
	}
	ssize_t offset = ftell(q->read.binlogfd);
	if (d) {
		d->vlen = je->size;
		d->v = malloc (d->vlen );
		if (1 != fread(d->v,  d->vlen,1, q->read.binlogfd)) {
			// record has not been fully written yet;
			fseek(q->read.binlogfd,offset - sizeof(*je), SEEK_SET);
			return LIBQUEUE_FAILURE;
		}
	}
	fseek(q->read.binlogfd,offset - sizeof(*je), SEEK_SET);
	return LIBQUEUE_SUCCESS;
}

static int queue_index_lookup(const struct Queue * const q,  int64_t idx, struct FileItr * itr,struct QueueData * const d, struct JournalEntry  *je) {
	assert(q != NULL);
	assert(d != NULL);
	if (0 ==  fileItr_opened(itr) ) {
		FileKey key =  getOldestJournal(q);
		if (0 ==key.time) {
			queue_set_error((struct Queue *)q,"queue is empty","");
			d->vlen = 0;
			d->v = NULL;
			return LIBQUEUE_FAILURE;
		}
		if (-1 == openJournalAtTime(&key, q->path, itr)) {
			return LIBQUEUE_FAILURE;
		}
	}
	if (!fileItr_opened(itr)) {
		d->vlen = 0;
		d->v = NULL;
		return LIBQUEUE_FAILURE;
	}
	int read=0;
	while (1 == fread(je, sizeof(struct JournalEntry),1,itr->binlogfd)) {
		if (0 == je->done ) {
			read++;
			if (0 == idx)
				break;
			read--;
			idx--;
		}
		fseek(itr->binlogfd,je->size + sizeof(struct Footer), SEEK_CUR);
	}
	if (0 == read || idx > 0) {
		if (FILE_KEY_EQUAL(q->write.key, itr->key)) {
			d->vlen = 0;
			d->v = NULL;
			return LIBQUEUE_FAILURE;
		}
		closeFileItr(itr);
		itr->key = getNextOldestJournal(q, &(itr->key));
		if (0 == itr->key.time ) {
			d->vlen = 0;
			d->v = NULL;
			return LIBQUEUE_FAILURE;
		}
		if (0 != openJournalAtTime(&itr->key, q->path, itr)) {
			return LIBQUEUE_FAILURE;
		}
		return queue_index_lookup(q,idx,itr, d,je);
	}
	ssize_t offset = ftell(itr->binlogfd);
	if (d) {
		d->vlen = je->size;
		d->v = malloc (d->vlen );
		if ( 1 !=  fread(d->v,  d->vlen,1, itr->binlogfd)) {
			return LIBQUEUE_FAILURE;
		}
	}
	fseek(itr->binlogfd,offset - sizeof(*je), SEEK_SET);
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
	if (LIBQUEUE_SUCCESS != queue_peek_h(q,0, d,&je)) {
		if (d) {
			d->vlen = 0;
			d->v = NULL;
		}
		return LIBQUEUE_FAILURE;
	}
	je.done = 1;

	if (LIBQUEUE_FAILURE == writeAndFlushData(q->read.binlogfd, &je,sizeof(je))) {
		queue_set_error(q, "failed to mark entry done: ", strerror(errno));
		d->vlen = 0;
		d->v = NULL;
		return LIBQUEUE_FAILURE;
	}
	fseek(q->read.binlogfd,je.size + sizeof(struct Footer), SEEK_CUR);

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
		*lenbuf =q->bin_size ;
	return LIBQUEUE_SUCCESS;
}

/**
 * not implemented
 */
int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d){
	assert(q != NULL);
	assert(d != NULL);
	assert(d->v != NULL);
	/*
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
	*/
	return LIBQUEUE_SUCCESS;
}

/*
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
*/
static int fix_catalog_entries(struct Queue *q) {
	/*
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
		}
	}
	*/
	return LIBQUEUE_SUCCESS;
}
/**
 * not implemented yet
 */
int queue_repair_with_options(const char * const path,... ) {
	va_list argp;
	va_start(argp, path);
	UNUSED struct Queue * q = readoptions(argp);
	va_end(argp);

	q->path = strdup(path);
//	q->catalogFd = openCatalog(q);
	fix_catalog_entries(q);
	return queue_close(q);
}

/**
 * not implemented yet
 */
int queue_repair(const char * path) {
	return queue_repair_with_options(path,NULL);
}

