#ifndef REFLOAT_SERVER_H
#define REFLOAT_SERVER_H

#include "raft.h"

#include <stdbool.h>
#include <stdint.h>

#define SERVER_MAX_APPEND_ENTRIES 10

typedef uint16_t ServerId;
typedef uint64_t ServerTerm;
typedef uint64_t ServerLogIndex;

enum ServerEntryKind {
	SERVER_ENTRY_NOP = 0,
	SERVER_ENTRY_NORMAL
};

struct ServerEntry {
	ServerTerm term_added;
	RaftMsgTag tag;
	uint8_t kind;
	uint8_t payload[RAFT_PAYLOAD_SIZE];
};

enum ServerMsgKind {
	SERVER_MSG_WANT_VOTE,
	SERVER_MSG_DENY_VOTE,
	SERVER_MSG_GRANT_VOTE,

	SERVER_MSG_TRY_APPEND,
	SERVER_MSG_REFUSE_APPEND,
	SERVER_MSG_ACCEPT_APPEND
};

struct ServerMsg {
	ServerTerm sender_term;
	ServerLogIndex index;
	ServerTerm term;
	ServerLogIndex commit;
	ServerId sender_id;
	uint16_t num_entries;
	uint16_t kind;
	struct ServerEntry entries[SERVER_MAX_APPEND_ENTRIES];
};

struct ServerInbox {
	bool has_client_message;
	struct RaftSubmission from_client;
	bool has_server_message;
	struct ServerMsg from_server;
};

enum TimeoutKind {
	TIMEOUT_NONE,
	TIMEOUT_VOTES,
	TIMEOUT_HEARTBEATS
};

struct ServerEnv {
	uint16_t (*num_servers)(void *context);
	ServerId (*my_id)(void *context);

	bool (*receive_messages)(struct ServerInbox *inbox,
			enum TimeoutKind kind,
			void *context);

	void (*send_to_client)(enum RaftReportKind kind,
			RaftMsgTag tag,
			const uint8_t *payload,
			void *context);
	void (*send_to_server)(ServerId dest, enum ServerMsgKind kind,
			ServerLogIndex index,
			uint16_t num_entries,
			void *context);

	ServerTerm (*current_term)(void *context);
	int (*update_term)(ServerTerm new_term, void *context);
	bool (*can_vote_for)(ServerId candidate, void *context);
	void (*record_vote)(ServerId candidate, void *context);
	void (*advance_term_and_vote_for_self)(void *context);

	ServerLogIndex (*last_log_index)(void *context);
	ServerLogIndex (*committed_index)(void *context);
	struct ServerEntry (*log_entry)(ServerLogIndex index, void *context);
	void (*truncate_and_append_to_log)(ServerLogIndex at,
			const struct ServerEntry entries[], uint64_t count,
			void *context);
	void (*append_entry_to_log)(enum ServerEntryKind kind,
			RaftMsgTag tag, uint8_t payload[],
			void *context);
	void (*commit_log_entries)(ServerLogIndex up_to, void *context);

	void (*restart_timer)(void *context);

	void *context;
};

void server(struct ServerEnv env);

#endif
