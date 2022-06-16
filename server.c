#define _POSIX_C_SOURCE 200112L

#include "raft.h"
#include "server.h"

#include <setjmp.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum State {
	STATE_FOLLOWER,
	STATE_CANDIDATE,
	STATE_LEADER
};

static sigjmp_buf jmp_env;

static volatile sig_atomic_t current_state = STATE_FOLLOWER;

static void fatal_error(const char *s) {
	write(STDERR_FILENO, "server: ", 8);
	write(STDERR_FILENO, s, strlen(s));
	write(STDERR_FILENO, "\n", 1);
	_exit(EXIT_FAILURE);
}

static void timeout_handler(int sig) {
	(void)sig;
	current_state = STATE_CANDIDATE;
	siglongjmp(jmp_env, 1);
}

static void block_sigalrm(void) {
	sigset_t mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGALRM);
	if (sigprocmask(SIG_BLOCK, &mask, NULL) < 0) {
		fatal_error("block_sigalrm: sigprocmask");
	}
}

static void unblock_sigalrm(void) {
	sigset_t mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGALRM);
	if (sigprocmask(SIG_UNBLOCK, &mask, NULL) < 0) {
		fatal_error("unblock_sigalrm: sigprocmask");
	}
}

static void ignore_sigalrm(void) {
	struct sigaction sigalrm_action;
	sigemptyset(&sigalrm_action.sa_mask);
	sigalrm_action.sa_flags = 0;
	sigalrm_action.sa_handler = SIG_IGN;
	if (sigaction(SIGALRM, &sigalrm_action, NULL) < 0) {
		fatal_error("ignore_sigalrm: sigaction");
	}
}

static void unignore_sigalrm(void) {
	struct sigaction sigalrm_action;
	sigemptyset(&sigalrm_action.sa_mask);
	sigalrm_action.sa_flags = 0;
	sigalrm_action.sa_handler = timeout_handler;
	if (sigaction(SIGALRM, &sigalrm_action, NULL) < 0) {
		fatal_error("unignore_sigalrm: sigaction");
	}
}

static void unblock_sigalrm_dropping_pending(void) {
	ignore_sigalrm();
	unblock_sigalrm();
	unignore_sigalrm();
}

static bool follower_handle_want_vote(struct ServerEnv *env,
		int update_result,
		struct ServerMsg request) {
	ServerLogIndex last_log_index = env->last_log_index(env->context);
	ServerTerm last_logged_term = env->log_entry(last_log_index, env->context).term_added;
	if (update_result >= 0
			&& env->can_vote_for(request.sender_id, env->context)
			&& (request.term > last_logged_term
				|| (request.term == last_logged_term
					&& request.index >= last_log_index))) {
		env->record_vote(request.sender_id, env->context);
		env->send_to_server(request.sender_id, SERVER_MSG_GRANT_VOTE,
				0, 0, env->context);
		return true;
	} else {
		env->send_to_server(request.sender_id, SERVER_MSG_DENY_VOTE,
				0, 0, env->context);
		return false;
	}
}

static bool follower_handle_try_append(struct ServerEnv *env,
		int update_result,
		struct ServerMsg request) {
	if (update_result >= 0
			&& request.index <= env->last_log_index(env->context)
			&& request.term == env->log_entry(request.index, env->context).term_added) {
		env->truncate_and_append_to_log(request.index,
				request.entries,
				request.num_entries,
				env->context);
		env->commit_log_entries(request.commit, env->context);
		env->send_to_server(request.sender_id, SERVER_MSG_ACCEPT_APPEND,
				request.index,
				request.num_entries,
				env->context);
		return true;
	} else {
		env->send_to_server(request.sender_id, SERVER_MSG_REFUSE_APPEND,
				request.index,
				request.num_entries,
				env->context);
		return update_result >= 0;
	}
}

static bool follower_handle_msg(struct ServerEnv *env, struct ServerMsg msg) {
	int update_result = env->update_term(msg.sender_term, env->context);
	switch (msg.kind) {
	case SERVER_MSG_WANT_VOTE:
		return follower_handle_want_vote(env, update_result, msg);
	case SERVER_MSG_TRY_APPEND:
		return follower_handle_try_append(env, update_result, msg);
	case SERVER_MSG_DENY_VOTE:
	case SERVER_MSG_GRANT_VOTE:
	case SERVER_MSG_REFUSE_APPEND:
	case SERVER_MSG_ACCEPT_APPEND:
		break;
	default:
		fatal_error("follower_handle_msg: impossible");
	}

	return false;
}

static void follower(struct ServerEnv *env) {
	unignore_sigalrm();
	env->restart_timer(env->context);

	for (;;) {
		struct ServerInbox inbox = {
			.has_client_message = false,
			.has_server_message = false
		};
		(void)env->receive_messages(&inbox, TIMEOUT_NONE, env->context);

		if (inbox.has_client_message) {
			block_sigalrm();
			env->send_to_client(RAFT_MSG_NOT_LEADER,
					inbox.from_client.tag,
					NULL,
					env->context);
			unblock_sigalrm();
		}

		if (inbox.has_server_message) {
			block_sigalrm();
			if (follower_handle_msg(env, inbox.from_server)) {
				env->restart_timer(env->context);
				unblock_sigalrm_dropping_pending();
			} else {
				unblock_sigalrm();
			}
		}
	}
}

struct CandidateTally {
	uint16_t num_votes_received;
	bool heard_from[RAFT_MAX_SERVERS + 1];
};

static void candidate_solicit_votes(struct ServerEnv *env,
		const struct CandidateTally *tally) {
	uint16_t num_servers = env->num_servers(env->context);
	for (ServerId i = 1; i <= num_servers; i += 1) {
		if (!tally->heard_from[i]) {
			env->send_to_server(i, SERVER_MSG_WANT_VOTE,
					0, 0, env->context);
		}
	}
}

static enum State candidate_handle_msg(struct ServerEnv *env,
		struct CandidateTally *tally,
		struct ServerMsg msg) {
	int update_result = env->update_term(msg.sender_term, env->context);
	if (update_result > 0) {
		return STATE_FOLLOWER;
	}
	uint16_t num_servers = env->num_servers(env->context);

	switch (msg.kind) {
	case SERVER_MSG_WANT_VOTE:
		env->send_to_server(msg.sender_id, SERVER_MSG_DENY_VOTE,
				0, 0, env->context);
		break;
	case SERVER_MSG_DENY_VOTE: {
		if (update_result == 0) {
			tally->heard_from[msg.sender_id] = true;
		}
		break;
	}
	case SERVER_MSG_GRANT_VOTE: {
		if (update_result == 0 && !tally->heard_from[msg.sender_id]) {
			tally->heard_from[msg.sender_id] = true;
			tally->num_votes_received += 1;
			if (2 * tally->num_votes_received > num_servers) {
				return STATE_LEADER;
			}
		}
		break;
	}
	case SERVER_MSG_TRY_APPEND: {
		if (update_result == 0) {
			return STATE_FOLLOWER;
		} else {
			env->send_to_server(msg.sender_id,
					SERVER_MSG_REFUSE_APPEND,
					msg.index,
					msg.num_entries,
					env->context);
		}
		break;
	}
	case SERVER_MSG_REFUSE_APPEND:
	case SERVER_MSG_ACCEPT_APPEND:
		break;
	}

	return STATE_CANDIDATE;
}

static void candidate(struct ServerEnv *env) {
	env->restart_timer(env->context);
	env->advance_term_and_vote_for_self(env->context);

	ServerId my_id = env->my_id(env->context);
	uint16_t num_servers = env->num_servers(env->context);
	struct CandidateTally tally = {.num_votes_received = 1};
	for (ServerId i = 1; i <= num_servers; i += 1) {
		tally.heard_from[i] = (i == my_id);
	}

	candidate_solicit_votes(env, &tally);
	for (;;) {
		struct ServerInbox inbox = {
			.has_client_message = false,
			.has_server_message = false
		};
		while (!env->receive_messages(&inbox, TIMEOUT_VOTES, env->context)) {
			candidate_solicit_votes(env, &tally);
		}

		if (inbox.has_client_message) {
			block_sigalrm();
			env->send_to_client(RAFT_MSG_NOT_LEADER,
					inbox.from_client.tag,
					NULL,
					env->context);
			unblock_sigalrm();
		}

		if (inbox.has_server_message) {
			block_sigalrm();
			current_state = (sig_atomic_t)(candidate_handle_msg(env,
						&tally, inbox.from_server));
			if (current_state == STATE_CANDIDATE) {
				unblock_sigalrm();
			} else {
				unblock_sigalrm_dropping_pending();
				break;
			}
		}
	}
}

struct TrackedIndices {
	ServerLogIndex matched;
	ServerLogIndex next;
};

static void leader_send_appends_to_all(struct ServerEnv *env,
		const struct TrackedIndices indices[]) {
	ServerId my_id = env->my_id(env->context);
	uint16_t num_servers = env->num_servers(env->context);
	ServerLogIndex my_last_index = env->last_log_index(env->context);
	for (ServerId i = 1; i <= num_servers; i += 1) {
		if (i == my_id) {
			continue;
		}

		if (indices[i].next == 0) {
			fatal_error("leader_send_appends_to_all: TrackedIndices.next hit zero");
		}
		if (indices[i].next > my_last_index + 1) {
			fatal_error("leader_send_appends_to_all: averted overflow on subtraction");
		}
		uint64_t num_send = my_last_index + 1 - indices[i].next;
		if (num_send > SERVER_MAX_APPEND_ENTRIES) {
			num_send = SERVER_MAX_APPEND_ENTRIES;
		}
		env->send_to_server(i, SERVER_MSG_TRY_APPEND,
				indices[i].next - 1,
				num_send,
				env->context);
	}
}

static void leader_handle_client(struct ServerEnv *env,
		const struct TrackedIndices indices[],
		struct RaftSubmission msg) {
	env->append_entry_to_log(SERVER_ENTRY_NORMAL, msg.tag, msg.payload,
			env->context);
	leader_send_appends_to_all(env, indices);
}

static void leader_commit_newly_replicated(struct ServerEnv *env,
		const struct TrackedIndices indices[],
		ServerLogIndex start) {
	ServerTerm current_term = env->current_term(env->context);
	ServerLogIndex base = env->committed_index(env->context);
	uint16_t num_servers = env->num_servers(env->context);
	for (ServerLogIndex j = start;
			j > base && env->log_entry(j, env->context).term_added == current_term;
			j -= 1) {
		uint16_t num_replicas = 0;
		for (ServerId i = 1; i <= num_servers; i += 1) {
			if (indices[i].matched >= j) {
				num_replicas += 1;
			}
		}
		if (2 * num_replicas > num_servers) {
			env->commit_log_entries(j, env->context);
			break;
		}
	}
}

static enum State leader_handle_msg(struct ServerEnv *env,
		struct TrackedIndices indices[],
		struct ServerMsg msg) {
	int update_result = env->update_term(msg.sender_term, env->context);
	if (update_result > 0) {
		return STATE_FOLLOWER;
	}

	switch (msg.kind) {
	case SERVER_MSG_WANT_VOTE:
		env->send_to_server(msg.sender_id, SERVER_MSG_DENY_VOTE,
				0, 0, env->context);
		break;
	case SERVER_MSG_TRY_APPEND:
		env->send_to_server(msg.sender_id, SERVER_MSG_REFUSE_APPEND,
				msg.index,
				msg.num_entries,
				env->context);
		break;
	case SERVER_MSG_REFUSE_APPEND: {
		if (update_result == 0) {
			ServerLogIndex implied_next = msg.index;
			if (implied_next < indices[msg.sender_id].next) {
				indices[msg.sender_id].next = implied_next;
			}
		}
		break;
	}
	case SERVER_MSG_ACCEPT_APPEND: {
		if (update_result == 0) {
			ServerLogIndex implied_matched = msg.index + msg.num_entries;
			if (implied_matched > indices[msg.sender_id].matched) {
				indices[msg.sender_id].matched = implied_matched;
			}
			ServerLogIndex implied_next = implied_matched + 1;
			if (implied_next > indices[msg.sender_id].next) {
				indices[msg.sender_id].next = implied_next;
			}

			leader_commit_newly_replicated(env,
					indices,
					indices[msg.sender_id].matched);
		}
		break;
	}
	case SERVER_MSG_DENY_VOTE:
	case SERVER_MSG_GRANT_VOTE:
		break;
	}

	return STATE_LEADER;
}

static void leader(struct ServerEnv *env) {
	ignore_sigalrm();
	env->send_to_client(RAFT_MSG_BECAME_LEADER, 0, NULL, env->context);
	env->append_entry_to_log(SERVER_ENTRY_NOP, 0, NULL, env->context);

	struct TrackedIndices tracked_indices[RAFT_MAX_SERVERS + 1];
	ServerLogIndex top = env->last_log_index(env->context);
	const struct TrackedIndices initial_indices = {.matched = 0, .next = top + 1};
	for (ServerId i = 1; i <= env->num_servers(env->context); i += 1) {
		tracked_indices[i] = initial_indices;
	}
	tracked_indices[env->my_id(env->context)].matched = top;

	for (;;) {
		struct ServerInbox inbox = {
			.has_client_message = false,
			.has_server_message = false
		};
		while (!env->receive_messages(&inbox, TIMEOUT_HEARTBEATS,
					env->context)) {
			leader_send_appends_to_all(env, tracked_indices);
		}

		if (inbox.has_client_message) {
			leader_handle_client(env,
					tracked_indices,
					inbox.from_client);
		}

		if (inbox.has_server_message) {
			if ((current_state = (sig_atomic_t)(leader_handle_msg(env,
						tracked_indices,
						inbox.from_server)))
					!= STATE_LEADER) {
				break;
			}
		}
	}
}

void server(struct ServerEnv env) {
	sigsetjmp(jmp_env, 1);
	for (;;) {
		switch (current_state) {
		case STATE_FOLLOWER:
			follower(&env);
			break;
		case STATE_CANDIDATE:
			candidate(&env);
			break;
		case STATE_LEADER:
			leader(&env);
			break;
		}
	}
}
