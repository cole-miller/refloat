#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>

#define LOG_HEADER_SIZE (3*8)
#define LOG_FILE_PERMS (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

struct LogHandle {
	int parent;
	int primary;
	int undo;
};

// note: after this call, dir is "owned" by the LogHandle and the parent
// should not close it directly, whether or not a valid LogHandle* was
// returned
struct LogHandle *log_open(int dir) {
	int parent = dir, primary = -1, undo = -1;
	bool did_lock = false;
	struct LogHandle *handle = NULL;
	struct flock lock_primary = {
		.l_type = F_WRLCK,
		.l_whence = SEEK_SET,
		.l_start = 0,
		.l_len = 1
	};

	primary = openat(dir, "primary", O_RDWR|O_CREAT|O_NOFOLLOW, LOG_FILE_PERMS);
	if (primary < 0) {
		return NULL;
	}
	if (fcntl(primary, F_SETLK, &lock_primary) < 0) {
		goto fail_with_cleanup;
	}
	did_lock = true;
	undo = openat(dir, "undo", O_RDWR|O_CREAT|O_NOFOLLOW, LOG_FILE_PERMS);
	if (undo < 0) {
		goto fail_with_cleanup;
	}
	handle = malloc(sizeof(struct LogHandle));
	if (!handle) {
		goto fail_with_cleanup;
	}
	handle->parent = parent;
	handle->primary = primary;
	handle->undo = undo;
	return handle;

fail_with_cleanup:
	if (handle) {
		free(handle);
	}
	if (undo >= 0) {
		close(undo);
	}
	if (did_lock) {
		lock_primary.l_type = F_UNLCK;
		fcntl(primary, F_SETLK, &lock_primary);
	}
	if (primary >= 0) {
		close(primary);
	}
	close(parent);
	return NULL;
}

int log_recover(struct LogHandle *handle) {
	// TODO fsync

	off_t undo_len = lseek(handle->undo, 0, SEEK_END);
	if (undo_len == -1) {
		return -1;
	}
	if (undo_len < LOG_HEADER_SIZE) {
		goto truncate_and_succeed;
	}
	uint64_t header[3];
	if (pread(handle->undo, header, LOG_HEADER_SIZE, 0) < LOG_HEADER_SIZE) {
		return -1;
	}
	uint64_t vote = ntoh64(header[0]), term = ntoh64(header[1]), index = ntoh64(header[2]);
	bool have_entries = (vote >> 63) != 0;
	uint64_t entries_start = 0, entries_len = 0;
	if (have_entries) {
		if (undo_len < LOG_HEADER_SIZE + SECONDARY_HEADER_SIZE) {
			goto truncate_and_succeed;
		}
		uint64_t secondary_header[2];
		if (pread(handle->undo, secondary_header, SECONDARY_HEADER_SIZE, LOG_HEADER_SIZE) < SECONDARY_HEADER_SIZE) {
			return -1;
		}
		entries_start = ntoh64(header[0]);
		entries_len = ntoh64(header[1]);
		if (undo_len != LOG_HEADER_SIZE + SECONDARY_HEADER_SIZE + entries_len * ENTRY_SIZE) {
			goto truncate_and_succeed;
		}
	} else if (undo_len != LOG_HEADER_SIZE) {
		goto truncate_and_succeed;
	}

	// at this point we know the undo file is well-formed, so we'll restore the primary
	// first, restore the header
	header[0] &= !hton64(1 << 63);
	if (pwrite(handle->primary, header, LOG_HEADER_SIZE, 0) < LOG_HEADER_SIZE) {
		return -1;
	}
	// now set the length and restore the entries
	

truncate_and_succeed:
	if (ftruncate(handle->undo, 0) < 0) {
		return -1;
	}
	return 0;
}

void log_close(struct LogHandle *handle) {
	close(handle->undo);
	close(handle->primary);
	close(handle->parent);
	free(handle);
}

