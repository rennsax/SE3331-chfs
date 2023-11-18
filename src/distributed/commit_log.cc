#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {

template <typename T>
T read_next(std::shared_ptr<BlockManager> bm, usize &offset) {
  auto buffer = bm->contiguous_read_(offset, sizeof(T));
  offset += sizeof(T);
  T res = *reinterpret_cast<T *>(buffer.data());
  return res;
}
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm),
      cur_block_(KDefaultBlockCnt - KLogBlockCnt), cur_offset_(0) {
  this->bm_->contiguous_write_(cur_block_, cur_block_,
                               std::vector<u8>(sizeof(txn_id_t), 0));
}

CommitLog::~CommitLog() {
}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  return this->parse_log_().size();
}

auto CommitLog::parse_log_() const -> std::vector<LogEntry> {
  std::vector<LogEntry> res{};
  usize offset = (KDefaultBlockCnt - KLogBlockCnt) * DiskBlockSize;

  while (true) {
    auto tid = read_next<txn_id_t>(this->bm_, offset);
    if (tid == 0) {
      break;
    }
    std::vector<BlockOperation> ops{};
    while (true) {
      auto bid = read_next<block_id_t>(this->bm_, offset);
      if (bid == 0) {
        break;
      }
      auto data = this->bm_->contiguous_read_(offset, DiskBlockSize);
      offset += DiskBlockSize;
      ops.push_back(BlockOperation{bid, data});
    }
    res.push_back(LogEntry{tid, ops});
  }
  return res;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
  auto entry_sz = 20 + 4104 * ops.size();
  std::vector<u8> log_entry(entry_sz);
  auto data_ptr = log_entry.data();
  std::memcpy(data_ptr, &txn_id, sizeof(txn_id_t));
  data_ptr += sizeof(txn_id_t);
  for (auto const &op : ops) {
    *reinterpret_cast<u64 *>(data_ptr) = op->block_id_;
    data_ptr += sizeof(u64);
    std::memcpy(data_ptr, op->new_block_state_.data(), DiskBlockSize);
    data_ptr += DiskBlockSize;
  }
  *reinterpret_cast<u64 *>(data_ptr) = 0;
  CHFS_VERIFY(data_ptr + sizeof(u64) - log_entry.data() == entry_sz,
              "the log entry size mismatch");
  // Since I doesn't implement overriding the beginning,
  // the new cursor can be calculated here, not by the write function.
  auto cursor_new =
      this->bm_->contiguous_write_(cur_block_, cur_offset_, log_entry);
  cur_block_ = std::get<0>(cursor_new);
  cur_offset_ = std::get<1>(cursor_new);
  // 32-bit 0, which denotes the end of redo-log
  this->bm_->contiguous_write_(cur_block_, cur_offset_,
                               std::vector<u8>(sizeof(txn_id_t), 0));
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  if (!is_checkpoint_enabled_) {
    return;
  }
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  auto log_entries = this->parse_log_();
  for (auto const &entry : log_entries) {
    for (auto const &op : entry.ops) {
      this->bm_->write_block(op.block_id_, op.new_block_state_.data());
    }
  }
}

}; // namespace chfs