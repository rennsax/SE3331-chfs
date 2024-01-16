#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include "rpc/msgpack.hpp"
#include "type_config.h"
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace chfs {
template <typename _Command> struct RaftLogEntry {
    using StateMachineCommand = _Command;
    RaftTermNumber term;
    StateMachineCommand command;

    std::size_t size() const noexcept {
        return command.size() + sizeof(RaftTermNumber);
    }

    std::vector<u8> serialize() const noexcept {
        std::vector<u8> buf{};
        buf.reserve(size());
        buf.push_back((term >> 24) & 0xff);
        buf.push_back((term >> 16) & 0xff);
        buf.push_back((term >> 8) & 0xff);
        buf.push_back(term & 0xff);
        auto command_buf = command.serialize(command.size());
        buf.insert(end(buf), begin(command_buf), end(command_buf));

        return buf;
    }

    void deserialize(const std::vector<u8> &data) {
        assert(data.size() == size());
        term = (data[0] & 0xff) << 24;
        term |= (data[1] & 0xff) << 16;
        term |= (data[2] & 0xff) << 8;
        term |= data[3] & 0xff;
        auto command_buf =
            std::vector<u8>(begin(data) + sizeof(RaftTermNumber), end(data));
        command.deserialize(command_buf, command_buf.size());
    }

    MSGPACK_DEFINE(term, command);
};

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command> class RaftLog {
    struct RaftLogMetaData {
        bool is_modified;            // 8
        RaftTermNumber current_term; // 8
        RaftNodeId voted_for;        // 8
        RaftLogIndex log_cnt;        // 8
    };

public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    void persist(RaftTermNumber current_term, RaftNodeId voted_for,
                 const std::vector<RaftLogEntry<Command>> &entries) {
        std::lock_guard<std::mutex> lock{mtx};
        RaftLogMetaData metadata = read_metadata_();
        metadata.is_modified = true;
        metadata.current_term = current_term;
        metadata.voted_for = voted_for;
        metadata.log_cnt = entries.size();
        persist_metadata_(metadata);
        persist_log_entries_(entries);
    }

    void read_persisted(RaftTermNumber &current_term, RaftNodeId &voted_for,
                        RaftLogIndex &commit_index,
                        std::vector<RaftLogEntry<Command>> &entries) {
        std::lock_guard<std::mutex> lock{mtx};
        auto metadata = read_metadata_();
        if (!metadata.is_modified) {
            return;
        }
        current_term = metadata.current_term;
        voted_for = metadata.voted_for;
        commit_index = metadata.log_cnt;

        entries.clear();
        entries.reserve(metadata.log_cnt);
        std::vector<u8> block_data(bm_->block_size());
        for (RaftLogIndex i = 1; i <= metadata.log_cnt; ++i) {
            bm_->read_block(i, block_data.data());
            RaftLogEntry<Command> entry{};
            entry.deserialize(std::vector<u8>(
                begin(block_data), begin(block_data) + entry.size()));
            entries.push_back(entry);
        }
    }

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;

    static constexpr uint64_t KMagicNumber = 0x7f7f7f7f7f7f7f7f;

    void persist_log_entries_(
        const std::vector<RaftLogEntry<Command>> &entries) {
        for (std::size_t i = 0; i < entries.size(); ++i) {
            auto to_write = entries[i].serialize();
            bm_->write_partial_block(i + 1, to_write.data(), 0,
                                     to_write.size());
        }
    }

    // Not thread-safe
    RaftLogMetaData read_metadata_() {
        std::vector<u8> block_data(bm_->block_size());
        bm_->read_block(0, block_data.data());

        uint64_t magic_number = 0;
        RaftTermNumber current_term = KRaftFallbackTermNumber;
        RaftNodeId voted_for = KRaftNilNodeId;
        RaftLogIndex log_cnt = 0;
        std::memcpy(&magic_number, block_data.data(), sizeof(uint64_t));
        if (magic_number != KMagicNumber) {
            return {false, current_term, voted_for, log_cnt};
        }
        std::memcpy(&current_term, block_data.data() + 8,
                    sizeof(RaftTermNumber));
        std::memcpy(&voted_for, block_data.data() + 16, sizeof(RaftNodeId));
        std::memcpy(&log_cnt, block_data.data() + 24, sizeof(RaftLogIndex));
        return {true, current_term, voted_for, log_cnt};
    }

    void persist_metadata_(const RaftLogMetaData &metadata) {
        uint64_t magic_number = metadata.is_modified ? KMagicNumber : 0;
        std::vector<u8> buffer(32);
        std::memcpy(buffer.data(), &magic_number, sizeof(uint64_t));
        std::memcpy(buffer.data() + 8, &metadata.current_term,
                    sizeof(RaftTermNumber));
        std::memcpy(buffer.data() + 16, &metadata.voted_for,
                    sizeof(RaftNodeId));
        std::memcpy(buffer.data() + 24, &metadata.log_cnt,
                    sizeof(RaftLogIndex));
        bm_->write_partial_block(0, buffer.data(), 0, 32);
    }
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
    : bm_(std::move(bm)) {
}

template <typename Command> RaftLog<Command>::~RaftLog() {
    bm_->flush();
}

} /* namespace chfs */
