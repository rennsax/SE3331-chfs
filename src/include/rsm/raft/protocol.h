#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"
#include "type_config.h"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    RaftTermNumber term;
    RaftNodeId candidate_id;
    RaftLogIndex last_log_index;
    RaftTermNumber last_log_term;

    MSGPACK_DEFINE(term, candidate_id, last_log_term, last_log_term);
};

struct RequestVoteReply {
    RaftTermNumber term;
    bool vote_granted;

    MSGPACK_DEFINE(term, vote_granted);
};

template <typename _Command> struct RaftLogEntry {
    using StateMachineCommand = _Command;
    RaftTermNumber received_term;
    StateMachineCommand command;

    MSGPACK_DEFINE(received_term, command);
};

template <typename Command> struct AppendEntriesArgs {
    RaftTermNumber term;
    RaftNodeId leader_id;
    RaftLogIndex prev_log_index;

    RaftTermNumber prev_log_term;
    std::vector<RaftLogEntry<Command>> entries;

    MSGPACK_DEFINE(term, leader_id, prev_log_index, prev_log_term, entries);
};

struct RpcAppendEntriesArgs {
    RaftTermNumber term;
    bool success;

    MSGPACK_DEFINE(term, success);
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(
    const AppendEntriesArgs<Command> &arg) {
    /* Lab3: Your code here */
    return RpcAppendEntriesArgs();
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(
    const RpcAppendEntriesArgs &rpc_arg) {
    /* Lab3: Your code here */
    return AppendEntriesArgs<Command>();
}

struct AppendEntriesReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(

    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(

    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(

    )
};

} /* namespace chfs */