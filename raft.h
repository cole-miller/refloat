#ifndef REFLOAT_RAFT_H
#define REFLOAT_RAFT_H

#include <stdbool.h>
#include <stdint.h>

#define RAFT_MAX_SERVERS 101
#define RAFT_PAYLOAD_SIZE 160

enum RaftReportKind {
	RAFT_MSG_NOT_LEADER,
	RAFT_MSG_BECAME_LEADER
};

typedef uint64_t RaftMsgTag;

struct RaftSubmission {
	RaftMsgTag tag;
	uint8_t payload[RAFT_PAYLOAD_SIZE];
};

#endif
