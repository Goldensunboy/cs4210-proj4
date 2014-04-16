/* Andrew Wilder *
 * Nick Johnson  */

#include "rvm.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>

#include <map>
#include <string>

struct rvm_segment_metadata {
	uint8_t *segment_memory;
	off_t length;

	trans_t transaction;
	off_t undo_record_start;
	off_t undo_record_end;
	void *undo_record_data;

	int data_fd;
	int log_fd;
};

static char * _directory;

static std::map<void *, struct rvm_segment_metadata *> _segment_metadata;
static trans_t _transaction_counter = 1;

static void _rvm_truncate_log(rvm_t rvm, const char *segname);

static int _read_all(int fd, void *buf, size_t size);
static int _write_all(int fd, void *buf, size_t size);

// Initialize the library with the specified directory as backing store
rvm_t rvm_init(const char *directory) {
	_directory = strdup(directory);

	if (mkdir(directory, 0755) && errno != EEXIST) {
		return -1;
	}

	return 0;
}

// Map a segment from disk into memory.
// If the segment does not already exist, then create it and give it size
// size_to_create. If the segment exists but is shorter than size_to_create,
// then extend it until it is long enough. It is an error to try to map the
// same segment twice.
void *rvm_map(rvm_t rvm, const char *segname, int size_to_create) {

	// 
	// open data file
	//

	char *data_path = (char*) malloc(strlen(_directory) + 5 + strlen(segname) + 1);
	strcpy(data_path, _directory);
	strcat(data_path, "/seg.");
	strcat(data_path, segname);

	int data_fd = open(data_path, O_RDWR | O_CREAT, 0644);

	if (data_fd < 0) {
		return NULL;
	}
	
	// 
	// open log file
	//

	char *log_path = (char*) malloc(strlen(_directory) + 5 + strlen(segname) + 1);
	strcpy(log_path, _directory);
	strcat(log_path, "/log.");
	strcat(log_path, segname);

	int log_fd = open(log_path, O_RDWR | O_CREAT, 0644);
	free(log_path);

	if (log_fd < 0) {
		close(data_fd);
		return NULL;
	}

	// create segment metadata entry
	struct rvm_segment_metadata *segment = 
		(struct rvm_segment_metadata*) malloc(sizeof(struct rvm_segment_metadata));
	segment->transaction = -1;
	segment->undo_record_data = NULL;
	segment->data_fd = data_fd;
	segment->log_fd = log_fd;
	
	// resize segment data if needed
	struct stat st;
	fstat(data_fd, &st);
	segment->length = st.st_size;

	if (segment->length < (off_t) size_to_create) {
		ftruncate(data_fd, (off_t) size_to_create);
		segment->length = (off_t) size_to_create;
	}

	// allocate segment in memory
	segment->segment_memory = (uint8_t*) malloc(segment->length);

	// copy data file to in-memory segment
	if (_read_all(data_fd, segment->segment_memory, segment->length)) {
		goto error;
	}

	// play log file over in-memory segment
	while (1) {

		// read transaction header
		uint32_t record_count;
		ssize_t record_count_bytes_read = read(log_fd, &record_count, sizeof(uint32_t));
		if (record_count_bytes_read == 0) {
			// end of log file
			break;
		}
		else if (record_count_bytes_read != sizeof(uint32_t)) {
			goto error;
		}

		for (uint32_t i = 0; i < record_count; i++) {
			uint32_t record_offset;
			uint32_t record_length;

			ssize_t record_offset_bytes_read = read(log_fd, &record_offset, sizeof(uint32_t));
			ssize_t record_length_bytes_read = read(log_fd, &record_length, sizeof(uint32_t));

			if (record_length_bytes_read != sizeof(uint32_t) ||
				record_offset_bytes_read != sizeof(uint32_t)) {
				goto error;
			}

			// apply redo record to in-memory segment
			if (_read_all(log_fd, &segment->segment_memory[record_offset], record_length)) {
				goto error;
			}
		}
	}

	// register segment metadata
	_segment_metadata[segment->segment_memory] = segment;

	return segment->segment_memory;

	error:
	close(log_fd);
	close(data_fd);
	free(segment->segment_memory);
	free(segment);
	return NULL;
}

// Unmap a segment from memory
void rvm_unmap(rvm_t rvm, void *segbase) {
	struct rvm_segment_metadata *segment = _segment_metadata[segbase];

	// free in-memory segment
	free(segment->segment_memory);
	
	// close file descriptors
	close(segment->log_fd);
	close(segment->data_fd);
}

// Destroy a segment completely, erasing its backing store.
// This function should not be called on a segment that is currently mapped.
void rvm_destroy(rvm_t rvm, const char *segname) {

	// remove data file
	char *data_path = (char*) malloc(strlen(_directory) + 5 + strlen(segname) + 1);
	strcpy(data_path, _directory);
	strcat(data_path, "/seg.");
	strcat(data_path, segname);
	unlink(data_path);
	free(data_path);

	// remove log file
	char *log_path = (char*) malloc(strlen(_directory) + 5 + strlen(segname) + 1);
	strcpy(log_path, _directory);
	strcat(log_path, "/log.");
	strcat(log_path, segname);
	unlink(log_path);
	free(log_path);
}

// Begin a transaction that will modify the segments listed in segbases.
// If any of the specified segments is already being modified by a transaction,
// then the call should fail and return (trans_t) -1. Note that trant_t needs
// to be able to be typecasted to an integer type.
trans_t rvm_begin_trans(rvm_t rvm, int numsegs, void **segbases) {

	// check if any of the segments are already in a transaction
	for (int i = 0; i < numsegs; i++) {
		void *segbase = segbases[i];
		if (_segment_metadata.find(segbase) == _segment_metadata.end()) {
			return -1;
		}

		struct rvm_segment_metadata *segment = _segment_metadata[segbase];

		if (segment->transaction != -1) {
			return -1;
		}
	}

	// allocate transaction id and structure
	trans_t trans_id = _transaction_counter++;

	// mark all segments with transaction id
	for (int i = 0; i < numsegs; i++) {
		void *segbase = segbases[i];
		struct rvm_segment_metadata *segment = _segment_metadata[segbase];
		segment->transaction = trans_id;
		segment->undo_record_data = NULL;
	}

	return trans_id;
}

// Declare that the library is about to modify a specified range of memory in
// the specified segment. The segment must be one of the segments specified in
// the call to rvm_begin_trans. Your library needs to ensure that the old
// memory has been saved, in case an abort is executed. It is legal call
// rvm_about_to_modify multiple times on the same memory area.
void rvm_about_to_modify(trans_t tid, void *segbase, int offset, int size) {
	struct rvm_segment_metadata *segment = _segment_metadata[segbase];

	if (segment->undo_record_data == NULL) {
		// new undo record
		segment->undo_record_start = offset;
		segment->undo_record_end = offset + size;
		segment->undo_record_data = malloc(size);
		memcpy(segment->undo_record_data, &segment->segment_memory[offset], size);
	}
	else {
		
		if (offset > segment->undo_record_start && 
			offset + size <= segment->undo_record_end) {
			// region already contained in undo record, no-op
			return;
		}
		
		off_t new_undo_record_start = (segment->undo_record_start < offset) 
			? segment->undo_record_start 
			: offset;
		off_t new_undo_record_end = (segment->undo_record_end > offset + size)
			? segment->undo_record_end
			: offset + size;

		uint8_t *new_undo_record_data = (uint8_t*) malloc(new_undo_record_end - new_undo_record_start);

		// copy new undo record from in-memory segment
		memcpy(new_undo_record_data, 
			&segment->segment_memory[new_undo_record_start], 
			new_undo_record_end - new_undo_record_start);

		// copy old undo record into new undo record
		memcpy(&new_undo_record_data[segment->undo_record_start - new_undo_record_start], 
			segment->undo_record_data, 
			segment->undo_record_end - segment->undo_record_start);

		// release old undo record
		free(segment->undo_record_data);

		// set new undo record
		segment->undo_record_end = new_undo_record_end;
		segment->undo_record_start = new_undo_record_start;
		segment->undo_record_data = new_undo_record_data;
	}
}

// Commit all changes that have been made within the specified transaction.
// When the call returns, then enough information should have been saved to
// disk so that, even if the program crashes, the changes will be seen by the
// program when it restarts.
void rvm_commit_trans(trans_t tid) {

	for (std::map<void *, struct rvm_segment_metadata *>::iterator i = _segment_metadata.begin(); 
			i != _segment_metadata.end(); i++) {

		struct rvm_segment_metadata *segment = i->second;
		if (segment->transaction != tid) {
			continue;
		}

		uint32_t record_count = 1;
		_write_all(segment->log_fd, &record_count, sizeof(uint32_t));

		uint32_t record_offset = segment->undo_record_start;
		uint32_t record_length = segment->undo_record_end - segment->undo_record_start;
		_write_all(segment->log_fd, &record_offset, sizeof(uint32_t));
		_write_all(segment->log_fd, &record_length, sizeof(uint32_t));

		_write_all(segment->log_fd, &segment->segment_memory[record_offset], record_length);

		segment->transaction = -1;
		free(segment->undo_record_data);
		segment->undo_record_data = NULL;
	}
}

// Undo all changes that have happened within the specified transaction.
void rvm_abort_trans(trans_t tid) {

	for (std::map<void *, struct rvm_segment_metadata *>::iterator i = _segment_metadata.begin(); 
			i != _segment_metadata.end(); i++) {

		struct rvm_segment_metadata *segment = i->second;

		if (segment->transaction == tid) {
			segment->transaction = -1;

			if (segment->undo_record_data) {
				memcpy(&segment->segment_memory[segment->undo_record_start],
					segment->undo_record_data,
					segment->undo_record_end - segment->undo_record_start);
				free(segment->undo_record_data);
				segment->undo_record_data = NULL;
			}
		}
	}
}

static void _rvm_truncate_log(rvm_t rvm, const char *segname) {

	// open data file
	char *data_path = (char*) malloc(strlen(_directory) + 5 + strlen(segname) + 1);
	strcpy(data_path, _directory);
	strcat(data_path, "/seg.");
	strcat(data_path, segname);

	int data_fd = open(data_path, O_RDWR | O_CREAT, 0644);

	if (data_fd < 0) {
		return;
	}
	
	// open log file
	char *log_path = (char*) malloc(strlen(_directory) + 5 + strlen(segname) + 1);
	strcpy(log_path, _directory);
	strcat(log_path, "/log.");
	strcat(log_path, segname);

	int log_fd = open(log_path, O_RDWR | O_CREAT, 0644);
	free(log_path);

	if (log_fd < 0) {
		close(data_fd);
		return;
	}

	// replay log file into data segment
	while (1) {

		// read transaction header
		uint32_t record_count;
		ssize_t record_count_bytes_read = read(log_fd, &record_count, sizeof(uint32_t));
		if (record_count_bytes_read == 0) {
			// end of log file
			break;
		}
		else if (record_count_bytes_read != sizeof(uint32_t)) {
			goto error;
		}

		for (uint32_t i = 0; i < record_count; i++) {
			uint32_t record_offset;
			uint32_t record_length;

			ssize_t record_offset_bytes_read = read(log_fd, &record_offset, sizeof(uint32_t));
			ssize_t record_length_bytes_read = read(log_fd, &record_length, sizeof(uint32_t));

			if (record_length_bytes_read != sizeof(uint32_t) ||
				record_offset_bytes_read != sizeof(uint32_t)) {
				goto error;
			}

			// fetch redo record
			uint8_t *record_data = (uint8_t*) malloc(record_length);
			if (_read_all(log_fd, record_data, record_length)) {
				free(record_data);
				goto error;
			}

			lseek(data_fd, record_offset, SEEK_SET);
			if (_write_all(data_fd, record_data, record_length)) {
				free(record_data);
				goto error;
			}
			free(record_data);
		}
	}

	// truncate log
	ftruncate(log_fd, 0);
	return;

	error:
	close(data_fd);
	close(log_fd);
	return;
}

// Play through any committed or aborted items in the log file(s) and shrink
// the log file(s) as much as possible.
void rvm_truncate_log(rvm_t rvm) {
	DIR *dir = opendir(_directory);
	struct dirent *dirent;
	while ((dirent = readdir(dir)) != NULL) {
		const char *name = dirent->d_name;
		if (name[0] == 's' && name[1] == 'e' && name[2] == 'g' && name[3] == '.') {
			_rvm_truncate_log(rvm, &name[4]);
		}
	}
	closedir(dir);
}

static int _read_all(int fd, void *buf, size_t size) {
	
	uint8_t *bbuf = (uint8_t*) buf;
	while (size) {
		ssize_t bytes = read(fd, bbuf, size);
		if (bytes == -1) {
			return -1;
		}
		
		size -= bytes;
		bbuf = &bbuf[bytes];
	}

	return 0;
}

static int _write_all(int fd, void *buf, size_t size) {

	uint8_t *bbuf = (uint8_t*) buf;
	while (size) {
		ssize_t bytes = write(fd, bbuf, size);
		if (bytes == -1) {
			return -1;
		}
		
		size -= bytes;
		bbuf = &bbuf[bytes];
	}

	return 0;
}
