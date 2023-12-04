#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <set>
#include <stdarg.h>
#include <thread>
#include <unistd.h>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "type_config.h"
#include "utils/thread_pool.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    RaftNodeId node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command> class RaftNode {

#define RAFT_LOG(fmt, args...)                                                 \
    do {                                                                       \
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(      \
                       std::chrono::system_clock::now().time_since_epoch())    \
                       .count();                                               \
        char buf[512];                                                         \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now,   \
                __FILE__, __LINE__, my_id, current_term, role, ##args);        \
        thread_pool->enqueue([=]() { std::cerr << buf; });                     \
    } while (0);

public:
    RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /*
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered
     * before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;

    /* Returns whether this node is the leader, you should also return the
     * current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /*
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends
     * the log, false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size)
        -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;

    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                   const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target,
                                     const AppendEntriesArgs<Command> arg,
                                     const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target,
                                       const InstallSnapshotArgs arg,
                                       const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    /* Data structures */
    bool network_stat; /* for test */

    std::mutex mtx; /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>>
        log_storage;                     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                            raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer>
        rpc_server; /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>>
        rpc_clients_map; /* RPC clients of all raft nodes including this node.
                          */

    /**
     * @brief Configuration for all nodes.
     *
     * We can get the total number of nodes in the cluster with this member.
     */
    std::vector<RaftNodeConfig> node_configs; /*  */
    RaftNodeId my_id; /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    RaftNodeId leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    const RaftNodeId majority_cnt;

    // Persistent state on all servers
    RaftTermNumber current_term = 0;
    RaftNodeId voted_for = KRaftNilNodeId;
    std::vector<RaftLogEntry<Command>> log{};

    // Volatile state on all servers
    RaftLogIndex commit_index = 0, last_applied = 0;

    // Volatile state on leaders
    /// Both of the two lists are of size @c node_configs.size()
    std::unique_ptr<std::vector<RaftLogIndex>> next_index{}, match_index{};

    // Volatile state on candidates
    std::set<RaftNodeId> granted_from{};

    /**
     * @brief Handle discover new term in the cluster.
     *
     * We then convert the role to follower, update the term number, make
     * votedFor @c nil.
     *
     * A new term can be discovered either from a candidate or the leader.
     * If it's from a candidate, the @c nil votedFor is possibly transient,
     * since it may soon vote for the candidate (but it may also denies);
     * If it's from the leader (AppendEntries RPC), votedFor may maintain as @c
     * nil for some time (at least the whole term).
     *
     * @attention Not thread-safe.
     *
     * @param newTerm The new term discovered from other servers.
     */
    void increase_term_(RaftTermNumber newTerm) noexcept {
        this->role = RaftRole::Follower;
        this->current_term = newTerm;
        this->voted_for = KRaftNilNodeId;
    }
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true), node_configs(configs), my_id(node_id), stopped(true),
      role(RaftRole::Follower), leader_id(KRaftNilNodeId),
      majority_cnt(static_cast<RaftNodeId>(configs.size()) / 2 + 1),
      current_term(0) {
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server =
        std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                     [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED,
                     [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                     [this](std::vector<u8> data, int cmd_size) {
                         return this->new_command(data, cmd_size);
                     });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                     [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                     [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
        return this->request_vote(arg);
    });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
        return this->append_entries(arg);
    });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT,
                     [this](InstallSnapshotArgs arg) {
                         return this->install_snapshot(arg);
                     });

    /* Lab3: Your code here */

    rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
    /* Lab3: Your code here */

    background_election =
        std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping =
        std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit =
        std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply =
        std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
    /* Lab3: Your code here */
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
    /* Lab3: Your code here */
    return std::make_tuple(false, -1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {
    /* Lab3: Your code here */
    return std::make_tuple(false, -1, -1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
    /* Lab3: Your code here */
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
    /* Lab3: Your code here */
    return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
    -> RequestVoteReply {

    auto produce_reply =
        [this, cand_id = args.candidate_id](bool voted) -> RequestVoteReply {
        if (voted) {
            this->voted_for = cand_id;
        }
        return {this->current_term, voted};
    };

    RaftProducer<RequestVoteReply, decltype(produce_reply)> reply_producer{
        this->mtx,
        std::move(produce_reply),
        {/* TODO cleanup */},
    };

    if (args.term < this->current_term) {
        // Find stale term number, immediately reject.
        return reply_producer.get(false);

    } else if (args.term > this->current_term) {
        // Update the term number (logical clock).
        this->increase_term_(args.term);
    }

    assert(this->current_term >= args.term);

    // The voter promises to not vote for different candidates in one term.
    if (this->voted_for != KRaftNilNodeId &&
        this->voted_for != args.candidate_id) {
        return reply_producer.get(false);
    }

    // Check the candidate is "up-to-date" from the voter's aspect.

    if (this->log.empty()) {
        return reply_producer.get(true);
    }

    if (auto my_last_log_term = this->log.back().received_term;

        my_last_log_term < args.last_log_term ||
        (my_last_log_term == args.last_log_term &&
         this->log.size() <= args.last_log_index)) {

        return reply_producer.get(true);
    }

    return reply_producer.get(false);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    RaftNodeId target, const RequestVoteArgs arg,
    const RequestVoteReply reply) {

    RaftRAII lock{
        this->mtx,
        {/* TODO cleanup */},
    };

    // Discover new term: give up the election.
    if (this->current_term < reply.term) {
        this->increase_term_(reply.term);
        // Fall to a follower.
        this->role = RaftRole::Follower;
        return;
    }

    // The candidate may have already fallen to a follower.
    if (this->role != RaftRole::Candidate) {
        return;
    }

    if (reply.vote_granted) {
        this->granted_from.insert(target);
        if (this->granted_from.size() >= this->majority_cnt) {
        }
    } else {
        this->granted_from.erase(target); // maybe here
    }

    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
    /* Lab3: Your code here */
    return AppendEntriesReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
    /* Lab3: Your code here */
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
    -> InstallSnapshotReply {
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
    /* Lab3: Your code here */
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id,
                                                        RequestVoteArgs arg) {
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr ||
        rpc_clients_map[target_id]->get_connection_state() !=
            rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg,
                                  res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    int target_id, AppendEntriesArgs<Command> arg) {
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr ||
        rpc_clients_map[target_id]->get_connection_state() !=
            rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg,
                                    res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    int target_id, InstallSnapshotArgs arg) {
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr ||
        rpc_clients_map[target_id]->get_connection_state() !=
            rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_install_snapshot_reply(target_id, arg,
                                      res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    // while (true) {
    //     {
    //         if (is_stopped()) {
    //             return;
    //         }
    //         /* Lab3: Your code here */
    //     }
    // }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    // while (true) {
    //     {
    //         if (is_stopped()) {
    //             return;
    //         }
    //         /* Lab3: Your code here */
    //     }
    // }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    // while (true) {
    //     {
    //         if (is_stopped()) {
    //             return;
    //         }
    //         /* Lab3: Your code here */
    //     }
    // }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    // while (true) {
    //     {
    //         if (is_stopped()) {
    //             return;
    //         }
    //         /* Lab3: Your code here */
    //     }
    // }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool> &network_availability) {
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client : rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network : network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config : node_configs) {
                if (config.node_id == node_id)
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(
                target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client : rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client : rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }

    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot();
}

} // namespace chfs