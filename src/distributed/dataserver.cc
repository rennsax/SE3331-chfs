#include "distributed/dataserver.h"
#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  auto version_block_cnt =
      (bm->total_blocks() * sizeof(version_t) + bm->total_blocks() - 1) /
      bm->block_size();

  CHFS_VERIFY(version_block_cnt >= 1, "no version block?");
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_block_cnt, false);
  } else {
    // We need to reserve some blocks for storing the version of each block

    for (usize i = 0; i < version_block_cnt; ++i) {
      bm->zero_block(i); // All version numbers are initially 0.
    }

    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, version_block_cnt, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() {
  server_.reset();
}

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  if (this->read_version(block_id) != version) {
    return {};
  }
  auto bm = this->block_allocator_->bm;
  std::vector<u8> buffer(bm->block_size()), res(len);
  if (auto read_res = bm->read_block(block_id, buffer.data());
      read_res.is_err()) {
    return {};
  }
  std::memcpy(res.data(), buffer.data() + offset, len);
  return res;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  auto write_res = this->block_allocator_->bm->write_partial_block(
      block_id, buffer.data(), offset, buffer.size());
  if (write_res.is_err()) {
    return false;
  }
  // FIXME writing does not increase the version id?
  // this->increase_version(block_id);
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto alloc_res = this->block_allocator_->allocate();

  if (alloc_res.is_err()) {
    return {}; // TODO error handling?
  }

  auto alloc_bid = alloc_res.unwrap();
  auto cur_version = this->increase_version(alloc_bid);

  return {alloc_bid, cur_version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  if (auto res = this->block_allocator_->deallocate(block_id); res.is_err()) {
    return false;
  }
  this->increase_version(block_id);

  return true;
}

version_t DataServer::read_version(block_id_t block_id) const {
  auto version_offset = block_id * sizeof(version_t); // in byte
  auto version_bid = version_offset / this->block_allocator_->bm->block_size();
  auto version_offset_in_block =
      version_offset % this->block_allocator_->bm->block_size();
  std::vector<u8> buffer(this->block_allocator_->bm->block_size());
  this->block_allocator_->bm->read_block(version_bid, buffer.data());
  auto previous_version =
      *reinterpret_cast<version_t *>(buffer.data() + version_offset_in_block);
  return previous_version;
}

version_t DataServer::increase_version(block_id_t block_id) {
  auto version_offset = block_id * sizeof(version_t); // in byte
  auto version_bid = version_offset / this->block_allocator_->bm->block_size();
  auto version_offset_in_block =
      version_offset % this->block_allocator_->bm->block_size();
  std::vector<u8> buffer(this->block_allocator_->bm->block_size());
  this->block_allocator_->bm->read_block(version_bid, buffer.data());
  auto previous_version =
      *reinterpret_cast<version_t *>(buffer.data() + version_offset_in_block);
  auto cur_version = previous_version + 1;
  *reinterpret_cast<version_t *>(buffer.data() + version_offset_in_block) =
      cur_version;
  this->block_allocator_->bm->write_block(version_bid, buffer.data());

  return cur_version;
}
} // namespace chfs