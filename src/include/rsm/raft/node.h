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
#include "rsm/raft/timer.h"
#include "rsm/state_machine.h"
#include "type_config.h"
#include "utils/thread_pool.h"

namespace chfs {

enum class RaftRole {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
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
    } while (0)

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

    std::atomic_bool stopped = true;

    RaftRole role = RaftRole::Follower;
    RaftNodeId leader_id =
        KRaftNilNodeId; // So the follower can forward requests.

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    const RaftNodeId majority_cnt;

    // Persistent state on all servers
    RaftTermNumber current_term = KRaftFallbackTermNumber;
    RaftNodeId voted_for = KRaftNilNodeId;
    /**
     * Log entries start from index 1.
     */
    std::vector<RaftLogEntry<Command>> log;

    // Volatile state on all servers
    RaftLogIndex commit_index = KRaftDefaultLogIndex;
    RaftLogIndex last_applied = KRaftDefaultLogIndex;

    // Volatile state on leaders
    /// Both of the two lists are of size @c node_configs.size()
    std::unique_ptr<std::vector<RaftLogIndex>> next_index{}, match_index{};

    // Volatile state on candidates
    RaftNodeId vote_granted_cnt = 0;

    std::unique_ptr<Timer> election_timer{};

    std::condition_variable state_cv{};

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
    void expire_term(RaftTermNumber newTerm) noexcept {
        if (this->role == RaftRole::Leader) {
            assert(next_index.get());
            assert(match_index.get());
            next_index.reset();
            match_index.reset();
        }
        role = RaftRole::Follower;
        current_term = newTerm;
        voted_for = KRaftNilNodeId;
        leader_id = KRaftNilNodeId;
        persist();
    }

    /**
     * @brief A candidate wins the election and becomes the leader.
     *
     */
    void win_election() {
        RAFT_LOG("[election] win the election");
        this->role = RaftRole::Leader;
        this->leader_id = this->my_id;
        assert(this->next_index.get() == nullptr);
        assert(this->match_index.get() == nullptr);
        auto N = this->node_configs.size();
        // Initialize the leader's data structure.
        this->next_index = std::make_unique<std::vector<RaftLogIndex>>(
            N, this->get_last_log_index() + 1);
        this->match_index = std::make_unique<std::vector<RaftLogIndex>>(N);
        do_broadcast_heartbeat(); // initial empty RPCs (heartbeat)
    }

    RaftLogIndex get_log_length() const noexcept {
        return this->log.size();
    }

    RaftLogIndex get_last_log_index() const noexcept {
        // In Raft consensus algorithm, log index begins with 1.
        return this->log.size();
    }

    /**
     * @brief Get the term number of the last log entry.
     *
     * If there is no log entry at all, return 0 since it's less than any valid
     * term number (which begins with 1).
     *
     * @return RaftTermNumber
     */
    RaftTermNumber get_last_log_term() const noexcept {
        if (this->log.empty()) {
            return 0;
        }
        return this->log.back().term;
    }

    /**
     * @brief Get the last log entry.
     *
     * @attention Raft log index begins with 1.
     *
     * @param idx Log entry index.
     * @return std::optional<RaftLogEntry<Command>> If not presented, the index
     * is out of bound.
     */
    std::optional<RaftLogEntry<Command>> get_log_entry(
        RaftLogIndex idx) const noexcept {
        if (idx < static_cast<RaftLogIndex>(1) ||
            idx > this->get_last_log_index()) {
            return {};
        }
        return this->log[idx - 1];
    }

    void reset_election_timer() {
        // RAFT_LOG("[common] reset timeout");
        election_timer = make_election_timer();
        election_timer->start();
    }

    void do_start_new_election() {
        // RAFT_LOG("[election] start new election");
        role = RaftRole::Candidate;
        current_term += 1;
        // Vote for self
        vote_granted_cnt = 1;
        voted_for = my_id;
        persist();

        reset_election_timer();
        for (RaftNodeId i = 0; i < node_configs.size(); i++) {
            if (i == my_id) {
                continue;
            }
            do_send_request_vote_rpc(i);
        }
    }

    void do_broadcast_heartbeat() {
        // Only leader can broadcast.
        assert(this->role == RaftRole::Leader);
        // RAFT_LOG("[heartbeat] broadcast heartbeat");

        for (RaftNodeId client_id = 0; client_id < node_configs.size();
             client_id++) {
            if (client_id == my_id) {
                continue;
            }
            auto prev_log_index = next_index->at(client_id) - 1;
            auto prev_log_term = get_log_entry(prev_log_index)->term;
            // Attention: the args of heartbeat RPC should be thought twice!
            auto heartbeat_rpc_args =
                AppendEntriesArgs<Command>{current_term,  my_id, prev_log_index,
                                           prev_log_term, {},    commit_index};
            // RAFT_LOG("[heartbeat] send heartbeat to node %d", client_id);
            thread_pool->enqueue(
                &RaftNode<StateMachine, Command>::send_append_entries, this,
                client_id, heartbeat_rpc_args);
        }
    }

    /**
     * Leader propagate log entries to followers.
     * Therefore, `entries` in the args should be non-empty.
     * For heartbeat, see #do_broadcast_heartbeat.
     *
     */
    void do_send_append_entries_rpc(RaftNodeId follower_id) {
        assert(follower_id != my_id);
        assert(this->role == RaftRole::Leader);
        assert(next_index->at(follower_id) >= 1);

        auto prev_log_index = next_index->at(follower_id) - 1;
        auto prev_log_term = get_log_entry(prev_log_index)->term;
        // If this function is called, we have
        // get_last_log_index() > prev_log_index.
        // Send multiple entries at once for efficiency.
        std::vector<RaftLogEntry<Command>> entries{};
        for (RaftLogIndex i = next_index->at(follower_id);
             i <= get_last_log_index(); i++) {
            entries.push_back(get_log_entry(i).value());
        }

        thread_pool->enqueue(
            &RaftNode<StateMachine, Command>::send_append_entries, this,
            follower_id,
            AppendEntriesArgs<Command>{current_term, my_id, prev_log_index,
                                       prev_log_term, entries, commit_index});
    }

    void do_send_request_vote_rpc(RaftNodeId follower_id) {
        assert(follower_id != my_id);
        assert(this->role == RaftRole::Candidate);
        // RAFT_LOG("[election] send RequestVote RPC to node %d", follower_id);
        thread_pool->enqueue(
            &RaftNode<StateMachine, Command>::send_request_vote, this,
            follower_id,
            RequestVoteArgs{current_term, my_id, get_last_log_index(),
                            get_last_log_term()});
    }

    std::tuple<bool, int, int> do_forward_command(
        const std::vector<u8> &cmd_data) {
        assert(leader_id != my_id);

        // During election
        if (leader_id == KRaftNilNodeId) {
            return {false, KRaftFallbackTermNumber, KRaftDefaultLogIndex};
        }
        RAFT_LOG("[command] forward command to leader %d", leader_id);
        // Synchronized write
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[leader_id] == nullptr ||
            rpc_clients_map[leader_id]->get_connection_state() !=
                rpc::client::connection_state::connected) {
            return {false, KRaftFallbackTermNumber, KRaftDefaultLogIndex};
        }

        auto res = rpc_clients_map[leader_id]->call(RAFT_RPC_NEW_COMMEND,
                                                    cmd_data, cmd_data.size());
        clients_lock.unlock();
        if (res.is_ok()) {
            return res.unwrap()->as<std::tuple<bool, int, int>>();
        } else {
            return {false, KRaftFallbackTermNumber, KRaftDefaultLogIndex};
        }
    }

    /**
     * @brief Leader tries to apply the new command (log) to the state machine.
     *
     * The leader need to append the new command to its own log, and then
     * propagate the log entry to the followers. Until majority of the servers
     * (including itself) have stored the new log entry, the log entry is
     * committed.
     *
     */
    std::tuple<bool, int, int> do_apply_to_state_machine(
        const std::vector<u8> &cmd_data, std::unique_lock<std::mutex> &lock) {
        assert(role == RaftRole::Leader);
        Command command{};
        command.deserialize(cmd_data, cmd_data.size());
        RAFT_LOG("[command] leader appends command %d at %d", command.value,
                 get_last_log_index() + 1);
        log.push_back({current_term, std::move(command)});
        persist();
        match_index->at(my_id) = get_last_log_index();
        auto new_log_index = get_last_log_index();

        state_cv.wait_for(lock, KRaftRequestTimeout, [this, new_log_index]() {
            // RAFT_LOG("[chore] last_applied %d, new_log_index %d",
            // last_applied,
            //          new_log_index);
            return last_applied >= new_log_index || role != RaftRole::Leader;
        });
        if (last_applied < new_log_index && role != RaftRole::Leader) {
            // While requesting, the leader has fallen.
            // Since new term begins, this request must be invalid now.
            return {false, KRaftFallbackTermNumber, KRaftDefaultLogIndex};
        }

        if (last_applied < new_log_index) {
            RAFT_LOG("[command] apply command timeout");
            // return {false, KRaftFallbackTermNumber, KRaftDefaultLogIndex};
            return {true, current_term, new_log_index};
        }
        return {true, current_term, new_log_index};
    }

    void persist() {
        assert(log_storage);
        log_storage->persist(current_term, voted_for,
                             std::vector<RaftLogEntry<Command>>(
                                 begin(log), begin(log) + commit_index));
    }

    void do_append_leader_entries(
        RaftLogIndex prev_log_index,
        const std::vector<RaftLogEntry<Command>> &new_entries) {
        // If an existing entry conflicts with a new one (same index but
        // different terms), delete the existing entry and all that follow it.

        // auto index_new_entry = args.prev_log_index + 1;
        // auto maybe_entry = this->get_log_entry(index_new_entry);
        // auto expected_term = args.entries[0].term;
        // if (maybe_entry.has_value() && maybe_entry->term != expected_term) {
        //     RAFT_LOG("[AppendEntries] truncate log from index %d",
        //              index_new_entry);
        //     log.resize(args.prev_log_index);
        // }

        // Append new entries not already in the log.
        std::stringstream ss_prev_log{}, ss_new_log{};

        auto previous_log_size = get_last_log_index();
        for (RaftLogIndex i = 1; i <= previous_log_size; ++i) {
            auto log_entry = get_log_entry(i);
            ss_prev_log << "(" << log_entry->term << ", "
                        << log_entry->command.value << ") ";
        }
        // Attention: cannot truncate log entries that are already committed!!
        log.resize(std::max(prev_log_index, commit_index));
        RAFT_LOG("[AppendEntries] after resizing: %d", get_last_log_index());

        log.insert(end(log), begin(new_entries), end(new_entries));
        for (RaftLogIndex i = 1; i <= get_last_log_index(); ++i) {
            auto log_entry = get_log_entry(i);
            ss_new_log << "(" << log_entry->term << ", "
                       << log_entry->command.value << ") ";
        }
        RAFT_LOG("[AppendEntries] log changed: %s -> %s",
                 ss_prev_log.str().c_str(), ss_new_log.str().c_str());
    }
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true), state(std::make_unique<StateMachine>()),
      node_configs(configs), my_id(node_id),
      majority_cnt(static_cast<RaftNodeId>(configs.size()) / 2 + 1) {
    std::lock_guard<std::mutex> lock{this->mtx};
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

    thread_pool = std::make_unique<ThreadPool>(4);

    log_storage =
        std::make_unique<RaftLog<Command>>(std::make_shared<BlockManager>(
            "/tmp/raft_log/node-" + std::to_string(my_id) + ".log"));
    log_storage->read_persisted(current_term, voted_for, log);

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

    std::lock_guard<std::mutex> lock{this->mtx};
    std::lock_guard<std::mutex> lock_clients{this->clients_mtx};

    std::map<int, bool> node_network_available{};
    for (auto config : node_configs) {
        rpc_clients_map[config.node_id] =
            std::make_unique<RpcClient>(config.ip_address, config.port, true);
    }

    stopped = false;
    reset_election_timer();

    background_election =
        std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping =
        std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit =
        std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply =
        std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    RAFT_LOG("[common] start node");
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
    std::lock_guard<std::mutex> lock{this->mtx};
    stopped = true;
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    thread_pool.reset();
    if (role == RaftRole::Leader) {
        assert(next_index.get());
        assert(match_index.get());
        next_index.reset();
        match_index.reset();
    }
    role = RaftRole::Follower;
    commit_index = KRaftDefaultLogIndex;
    last_applied = KRaftDefaultLogIndex;
    leader_id = KRaftNilNodeId;
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
    std::lock_guard<std::mutex> lock{this->mtx};
    assert((leader_id == my_id) == (role == RaftRole::Leader));
    return {this->role == RaftRole::Leader, this->current_term};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {

    if (cmd_data.size() != cmd_size) {
        RAFT_LOG("[command] invalid command size %zu versus %d",
                 cmd_data.size(), cmd_size);
        return std::make_tuple(false, KRaftFallbackTermNumber,
                               KRaftDefaultLogIndex);
    }

    std::unique_lock<std::mutex> lock{this->mtx};

    if (role == RaftRole::Leader) {
        auto res = do_apply_to_state_machine(cmd_data, lock);
        RAFT_LOG("[command] leader responds with (%d, %d, %d)",
                 std::get<0>(res), std::get<1>(res), std::get<2>(res));
        return res;
    } else {
        return {false, KRaftFallbackTermNumber, KRaftDefaultLogIndex};
        // return do_forward_command(cmd_data);
    }
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

    std::lock_guard<std::mutex> lock{this->mtx};

    if (args.term < current_term) {
        // Find stale term number, immediately reject.
        RAFT_LOG("[vote] reject %d for its stale term number",
                 args.candidate_id);
        return {current_term, false};
    }
    reset_election_timer();
    if (args.term > current_term) {
        // Update the term number (logical clock).
        expire_term(args.term);
    }

    if (this->role == RaftRole::Leader) {
        // I'm the leader!
        RAFT_LOG("[vote] reject %d because I'm the leader!", args.candidate_id);
        return {current_term, false};
    }

    // The voter promises to not vote for different candidates in one term.
#ifndef NDEBUG
    // TODO can a voter previously votes for a candidate c, but denies it in
    // the same term?
    // if (this->voted_for != KRaftNilNodeId) {
    // RAFT_LOG("[vote] duplicated vote for the same term");
    // }
#endif

    if (this->voted_for != KRaftNilNodeId &&
        this->voted_for != args.candidate_id) {
        RAFT_LOG("[vote] reject %d since already voted for %d",
                 args.candidate_id, this->voted_for);
        return {current_term, false};
    }

    // Check the candidate is "up-to-date" from the voter's aspect.

    auto my_last_log_term = this->get_last_log_term();
    auto my_last_log_index = this->get_last_log_index();

    if (my_last_log_term > args.last_log_term) {
        RAFT_LOG(
            "[vote] reject %d because we have larger lastLogTerm: %d vs %d",
            args.candidate_id, my_last_log_term, args.last_log_term);
        return {current_term, false};
    }

    if (my_last_log_term == args.last_log_term &&
        my_last_log_index > args.last_log_index) {
        RAFT_LOG(
            "[vote] reject %d because we have larger lastLogIndex: %d vs %d",
            args.candidate_id, my_last_log_index, args.last_log_index);
        return {current_term, false};
    }

    RAFT_LOG("[vote] vote for %d", args.candidate_id);
    voted_for = args.candidate_id;
    persist();
    return {current_term, true};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    RaftNodeId target, const RequestVoteArgs arg,
    const RequestVoteReply reply) {

    std::lock_guard<std::mutex> lock{this->mtx};

    // Discover new term: give up the election.
    if (this->current_term < reply.term) {
        RAFT_LOG("[election] discover new term %d, fallback to follower",
                 reply.term);
        expire_term(reply.term);
        return;
    }

    // The candidate may have already fallen to a follower or become a leader
    if (this->role != RaftRole::Candidate) {
        return;
    }

    if (reply.vote_granted) {
        this->vote_granted_cnt++;
        if (this->vote_granted_cnt >= this->majority_cnt) {
            win_election();
        }
    }

    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {

    AppendEntriesArgs<Command> args =
        transform_rpc_append_entries_args<Command>(rpc_arg);

    std::lock_guard<std::mutex> lock{this->mtx};
    RAFT_LOG("[AppendEntries] receive from %d, prev_log_index %d, "
             "prev_log_term %d, entry (%d, %d), commit_index %d",
             args.leader_id, args.prev_log_index, args.prev_log_term,
             args.entries.empty() ? -1 : args.entries[0].term,
             args.entries.empty() ? -1 : args.entries[0].command.value,
             args.leader_commit);
    // Common operations
    // The leader has less term number, reject it!
    if (args.term < current_term) {
        return AppendEntriesReply{current_term, false};
    }
    reset_election_timer();
    leader_id = args.leader_id;
    if (args.term > current_term) {
        expire_term(args.term);
    }
    if (role == RaftRole::Candidate) {
        RAFT_LOG("[election] candidate receive AppendEntries RPC from %d, "
                 "fallback to "
                 "follower",
                 args.leader_id);
        role = RaftRole::Follower;
    }

    // if (is_heartbeat) {
    //     return AppendEntriesReply{current_term, true};
    // }

    // If log inconsistency occurs...
    if (args.prev_log_index != 0) {
        auto maybe_entry = this->get_log_entry(args.prev_log_index);
        if (!maybe_entry.has_value()) {
            RAFT_LOG("[AppendEntries] find log inconsistency at index %d: no "
                     "such log entry",
                     args.prev_log_index);
            return AppendEntriesReply{current_term, false};
        }
        if (maybe_entry->term != args.prev_log_term) {
            RAFT_LOG("[AppendEntries] find log inconsistency at index %d: "
                     "unmatched term number",
                     args.prev_log_index);
            // The leader need to decrease "next_index"
            return AppendEntriesReply{current_term, false};
        }
    }

    // Replicate leader's log entries to the follower.
    // Even if args.entries is empty.
    do_append_leader_entries(args.prev_log_index, args.entries);

    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if (args.leader_commit > commit_index) {
        auto previous_commit_index = commit_index;
        commit_index = std::min(args.leader_commit, get_last_log_index());
        RAFT_LOG("[AppendEntries] commitIndex: %d -> %d", previous_commit_index,
                 commit_index);
    }

    return AppendEntriesReply{current_term, true};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {

    std::lock_guard<std::mutex> lock{this->mtx};
    if (reply.term > current_term) {
        RAFT_LOG("[heartbeat] discover new term %d, fallback to follower",
                 reply.term);
        expire_term(reply.term);
        return;
    }
    if (role != RaftRole::Leader) {
        // The leader may have fallen to a follower.
        return;
    }
    if (reply.success) {

        if (!arg.entries.empty()) {
            next_index->at(node_id) =
                arg.prev_log_index + arg.entries.size() + 1;
            match_index->at(node_id) = arg.prev_log_index + arg.entries.size();

            // RAFT_LOG("[log] update next index of node %d to %d", node_id,
            //          next_index->at(node_id));
            // If there exists an N such that N > commitIndex, a majority of
            // matchIndex[i] >= N, and log[N].term == currentTerm:
            // set commitIndex = N
            for (RaftLogIndex i = get_last_log_index(); i > commit_index; i--) {
                if (std::count_if(match_index->begin(), match_index->end(),
                                  [i](RaftLogIndex idx) { return idx >= i; }) >=
                        majority_cnt &&
                    get_log_entry(i)->term == current_term) {
                    RAFT_LOG("[log] commitIndex: %d -> %d", commit_index, i);
                    commit_index = i;
                    break;
                }
            }
        }

    } else {
        next_index->at(node_id) = next_index->at(node_id) - 1;
        // RAFT_LOG("[log] update next index of node %d to %d", node_id,
        //          next_index->at(node_id));
        // Retry sending: let #run_background_commit do the work.
    }
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
    // Periodically check the liveness of the leader.

    /**
     * Does the following stuff:
     * 1. If the server is the leader, do nothing.
     * 2. If the server is the candidate, check the election timeout.
     *    - If the election timeout is not expired, do nothing.
     *    - Otherwise, start a new election.
     * 3. If the server is the follower, check the heartbeat timeout.
     *    - If the heartbeat timeout is not expired, do nothing.
     *    - Otherwise, becomes a candidate and start a new election.
     *
     */

    while (true) {
        if (is_stopped()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock{this->mtx};
            if (this->role != RaftRole::Leader) {
                assert(election_timer);
                if (election_timer->is_timeout()) {
                    do_start_new_election();
                }
            }
        }

        std::this_thread::sleep_for(KRaftThreadSleepingSlice);
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodically send logs to the follower.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) {
            return;
        }
        {
            std::unique_lock<std::mutex> lock{this->mtx};
            if (role == RaftRole::Leader) {
                // If last log index >= nextIndex for a follower: send
                // AppendEntries RPC with log entries starting at nextIndex
                auto last_log_index = get_last_log_index();
                for (RaftNodeId i = 0; i < node_configs.size(); i++) {
                    if (i == my_id) {
                        continue;
                    }
                    if (last_log_index >= next_index->at(i)) {
                        do_send_append_entries_rpc(i);
                    }
                }
            } else {
                // If the server is not the leader now, notify threads waiting
                // for state_cv in case there are pending writes.
                lock.unlock();
                state_cv.notify_all();
            }
        }
        std::this_thread::sleep_for(KRaftThreadSleepingSlice);
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodically apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        if (is_stopped()) {
            return;
        }

        {
            std::lock_guard<std::mutex> lock{this->mtx};

            if (commit_index > last_applied) {
                for (RaftLogIndex i = last_applied + 1; i <= commit_index;
                     ++i) {
                    auto entry = get_log_entry(i);
                    assert(entry.has_value());
                    state->apply_log(entry->command);
                }
                RAFT_LOG("[sm] apply logs from %d to %d", last_applied + 1,
                         commit_index);
                last_applied = commit_index;
            }
        }
        state_cv.notify_all();
        std::this_thread::sleep_for(KRaftThreadSleepingSlice);
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodically send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) {
            return;
        }
        {
            std::unique_lock<std::mutex> lock{this->mtx};
            if (this->role == RaftRole::Leader) {
                do_broadcast_heartbeat();
            }
        }
        std::this_thread::sleep_for(KRaftHeartbeatInterval);
    }

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