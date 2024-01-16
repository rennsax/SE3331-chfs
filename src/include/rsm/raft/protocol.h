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

    MSGPACK_DEFINE(term, candidate_id, last_log_index, last_log_term);
};

struct RequestVoteReply {
    RaftTermNumber term;
    bool vote_granted;

    MSGPACK_DEFINE(term, vote_granted);
};

template <typename Command> struct AppendEntriesArgs {
    RaftTermNumber term;
    RaftNodeId leader_id;
    RaftLogIndex prev_log_index;

    RaftTermNumber prev_log_term;
    std::vector<RaftLogEntry<Command>> entries;
    RaftLogIndex leader_commit;
};

struct RpcAppendEntriesArgs {
    RaftTermNumber term;
    RaftNodeId leader_id;
    RaftLogIndex prev_log_index;

    RaftTermNumber prev_log_term;
    std::vector<u8> entries;
    RaftLogIndex leader_commit;

    MSGPACK_DEFINE(term, leader_id, prev_log_index, prev_log_term, entries,
                   leader_commit);
};
template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(
    const RpcAppendEntriesArgs &rpc_arg);

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(
    const AppendEntriesArgs<Command> &arg) {
    std::vector<RaftLogEntry<Command>> original_entries = arg.entries;
    auto marshalled = std::vector<u8>{};
    if (!original_entries.empty()) {
        marshalled.reserve(original_entries.size() *
                           original_entries.at(0).size());

        for (const auto &entry : original_entries) {
            auto serialized = entry.serialize();
            marshalled.insert(end(marshalled), begin(serialized),
                              end(serialized));
        }
    }

    auto res = RpcAppendEntriesArgs{arg.term,           arg.leader_id,
                                    arg.prev_log_index, arg.prev_log_term,
                                    marshalled,         arg.leader_commit};
    auto original = transform_rpc_append_entries_args<Command>(res);
    assert(original.term == arg.term && original.leader_id == arg.leader_id &&
           original.prev_log_index == arg.prev_log_index &&
           original.prev_log_term == arg.prev_log_term &&
           original.leader_commit == arg.leader_commit);
    return res;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(
    const RpcAppendEntriesArgs &rpc_arg) {

    auto marshalled = rpc_arg.entries;
    std::vector<RaftLogEntry<Command>> entries{};

    if (!marshalled.empty()) {
        const auto entry_size = RaftLogEntry<Command>{}.size();
        auto entries_cnt = marshalled.size() / entry_size;
        entries.reserve(entries_cnt);
        for (auto i = 0; i < entries_cnt; i++) {
            auto begin_it = begin(marshalled) + i * entry_size;
            auto end_it = begin_it + entry_size;
            auto entry = RaftLogEntry<Command>{};
            entry.deserialize(std::vector<u8>(begin_it, end_it));
            entries.push_back(entry);
        }
    }

    return AppendEntriesArgs<Command>{
        rpc_arg.term,          rpc_arg.leader_id, rpc_arg.prev_log_index,
        rpc_arg.prev_log_term, entries,           rpc_arg.leader_commit};
}

struct AppendEntriesReply {
    RaftTermNumber term;
    bool success; // Whether there is log inconsistency

    MSGPACK_DEFINE(term, success);
};

struct InstallSnapshotArgs {
    RaftTermNumber term;
    RaftNodeId leader_id;
    RaftLogIndex last_included_index;
    RaftTermNumber last_included_term;

    std::size_t offset;
    std::vector<u8> data;

    bool done;

    MSGPACK_DEFINE(term, leader_id, last_included_index, last_included_term,
                   offset, data, done);
};

struct InstallSnapshotReply {
    RaftTermNumber term;

    MSGPACK_DEFINE(term);
};

} /* namespace chfs */