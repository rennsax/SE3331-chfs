#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  static constexpr auto inode_type_cast = [](u8 type) -> InodeType {
    switch (type) {
    case RegularFileType:
      return InodeType::FILE;
    case DirectoryType:
      return InodeType::Directory;
    default:
      return InodeType::Unknown;
    }
  };
  auto mk_res = this->operation_->mk_helper_metadata_server(
      parent, name.c_str(), inode_type_cast(type));
  if (mk_res.is_err()) {
    return 0;
  }
  return mk_res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {

  auto to_unlink = this->lookup(parent, name);
  if (to_unlink == 0) {
    return false;
  }
  // type or file?
  auto type_res = this->operation_->gettype(to_unlink);
  if (type_res.is_err()) {
    return false;
  }
  auto inode_type = type_res.unwrap();
  if (inode_type == InodeType::Directory) {
    auto dir_entities = this->readdir(to_unlink);
    if (!dir_entities.empty()) {
      // Not empty
      return false;
    }
    if (auto rm_res = this->operation_->unlink(parent, name.c_str());
        rm_res.is_err()) {
      return false;
    }
  } else if (inode_type == InodeType::FILE) {
    if (auto rm_res =
            this->operation_->unlink_regular_file(parent, name.c_str());
        rm_res.is_err()) {
      return false;
    }
  } else {
    return false;
  }
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {

  auto lookup_res = this->operation_->lookup(parent, name.c_str());

  return lookup_res.is_err() ? 0 : lookup_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
#ifndef NDEBUG
  auto type_res = this->operation_->gettype(id);
  CHFS_ASSERT(!type_res.is_err() && type_res.unwrap() == InodeType::FILE,
              "wrong file type");
#endif
  auto read_res = this->operation_->read_regular_node(id);
  if (read_res.is_err()) {
    return {};
  }
  auto block_entities = read_res.unwrap();
  std::vector<BlockInfo> res(block_entities.size());

  std::transform(begin(block_entities), end(block_entities), begin(res),
                 [](const RegularInode::BlockEntity &entity) -> BlockInfo {
                   return {entity.bid, entity.mid, entity.vid};
                 });
  return res;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // Check if the inode is a regular inode. If not, return error.
  if (auto type_res = this->operation_->gettype(id);
      type_res.is_err() || type_res.unwrap() != InodeType::FILE) {
    return {};
  }
  // Pick a random slave to allocate the block.
  auto slave_id = this->generator.rand(1, this->num_data_servers);
  auto alloc_response = this->clients_.at(slave_id)->call("alloc_block");
  if (alloc_response.is_err()) {
    return {};
  }
  auto [bid, vid] =
      alloc_response.unwrap()->as<std::pair<block_id_t, version_t>>();
  this->operation_->append_block_to_regular_inode(id, {bid, vid, slave_id});
  return {bid, slave_id, vid};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  const auto rm_callback =
      [=](const RegularInode::BlockEntity &entity) -> bool {
    return entity.bid == block_id && entity.mid == machine_id;
  };

  auto rm_res =
      this->operation_->delete_block_from_regular_inode(id, rm_callback);

  if (rm_res.is_err()) {
    // Error may occur because the inode isn't a regular inode.
    return false;
  }
  CHFS_VERIFY(rm_res.unwrap() <= 1, "delete more than one block entities");

  if (rm_res.unwrap() == 0) {
    // The block to delete isn't shown up in the inode.
    return false;
  }

  // Free the block from the slave server.
  if (auto res = this->clients_.at(machine_id)->call("free_block", block_id);
      res.is_err() || !res.unwrap()->as<bool>()) {
    return false;
  }
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  std::list<DirectoryEntry> entities{};
  if (auto read_res =
          ::chfs::read_directory(this->operation_.get(), node, entities);
      read_res.is_err()) {
    return {};
  }
  std::vector<std::pair<std::string, inode_id_t>> res(entities.size());

  std::transform(
      begin(entities), end(entities), begin(res),
      [](DirectoryEntry &entity) -> std::pair<std::string, inode_id_t> {
        return {std::move(entity.name), entity.id};
      });

  return res;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {

  static constexpr auto inode_type_cast = [](InodeType type) -> u8 {
    switch (type) {
    case InodeType::FILE:
      return RegularFileType;
    case InodeType::Directory:
      return DirectoryType;
    case InodeType::Unknown:
    default:
      return 0;
    }
  };
  auto res = this->operation_->get_type_attr(id);
  if (res.is_err()) {
    return {};
  }
  auto [inode_type, attr] = res.unwrap();
  return std::make_tuple(attr.size, attr.atime, attr.mtime, attr.ctime,
                         inode_type_cast(inode_type));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs