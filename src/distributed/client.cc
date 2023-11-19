#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {
}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto res = this->metadata_server_->call("mknode", static_cast<u8>(type),
                                          parent, name);
  if (res.is_err()) {
    return res.unwrap_error();
  }
  auto inode_id = res.unwrap()->as<inode_id_t>();
  if (inode_id == 0) {
    return ErrorType::INVALID;
  }
  return inode_id;
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto res = this->metadata_server_->call("unlink", parent, name);
  if (res.is_err()) {
    return res.unwrap_error();
  }
  auto unlink_ok = res.unwrap()->as<bool>();
  return unlink_ok ? KNullOk : ErrorType::INVALID;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto res = this->metadata_server_->call("lookup", parent, name);
  if (res.is_err()) {
    return res.unwrap_error();
  }
  auto inode_id = res.unwrap()->as<inode_id_t>();
  if (inode_id == 0) {
    return ErrorType::NotExist;
  }
  return inode_id;
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto res = this->metadata_server_->call("readdir", id);
  if (res.is_err()) {
    return res.unwrap_error();
  }
  auto dir_entities =
      res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return dir_entities;
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  static constexpr auto inode_type_cast = [](u8 type) -> InodeType {
    if (type == RegularFileType) {
      return InodeType::FILE;
    }
    if (type == DirectoryType) {
      return InodeType::Directory;
    }
    return InodeType::Unknown;
  };

  auto res = this->metadata_server_->call("get_type_attr", id);
  if (res.is_err()) {
    return res.unwrap_error();
  }
  auto [size, atime, mtime, ctime, type] =
      res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  return std::make_pair(inode_type_cast(type),
                        FileAttr{atime, mtime, ctime, size});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // 1. Ask the metadata server for all the blocks on data servers
  auto blocks_map_res = this->get_block_map_(id);
  if (blocks_map_res.is_err()) {
    return blocks_map_res.unwrap_error();
  }
  auto blocks_map = blocks_map_res.unwrap();

  std::vector<u8> buffer(size);
  u8 *cur_pos = buffer.data();

  // Error: the file doesn't have so much data
  if (offset + size > blocks_map.size() * DiskBlockSize) {
    return ErrorType::INVALID;
  }

  // 2. For each block, ask for the corresponded data server.
  usize first_block = offset / DiskBlockSize;
  usize first_offset = offset % DiskBlockSize;
  // Only read one block
  if (DiskBlockSize - first_offset >= size) {
    auto read_res = this->read_from_data_server_(
        cur_pos, blocks_map.at(first_block), first_offset, size);
    if (read_res.is_err()) {
      return read_res.unwrap_error();
    }
    return buffer;
  }
  // Read more than one block
  usize last_block = (offset + size - 1) / DiskBlockSize;
  usize last_offset = (offset + size) % DiskBlockSize;

  // Read the first block
  {
    usize read_size = DiskBlockSize - first_offset;
    auto read_res = this->read_from_data_server_(
        cur_pos, blocks_map.at(first_block), first_offset, read_size);
    if (read_res.is_err()) {
      return read_res.unwrap_error();
    }
    cur_pos += read_size;
  }

  // Read full blocks
  for (usize i = first_block + 1; i < last_block; ++i) {
    auto read_res = this->read_from_data_server_(cur_pos, blocks_map.at(i));
    if (read_res.is_err()) {
      return read_res.unwrap_error();
    }
    cur_pos += DiskBlockSize;
  }
  // Read the final block
  {
    auto read_res = this->read_from_data_server_(
        cur_pos, blocks_map.at(last_block), 0, last_offset);
    if (read_res.is_err()) {
      return read_res.unwrap_error();
    }
    // cur_pos += last_offset;
  }
  CHFS_VERIFY(cur_pos + last_offset - buffer.data(), buffer.size());

  return ChfsResult<std::vector<u8>>(buffer);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // 1. Ask the metadata server for all the blocks on data servers
  auto blocks_map_res = this->get_block_map_(id);
  if (blocks_map_res.is_err()) {
    return blocks_map_res.unwrap_error();
  }
  auto blocks_map = blocks_map_res.unwrap();

  // The client determine whether to allocate new block.
  auto minimal_block_cnt =
      (offset + data.size() + DiskBlockSize - 1) / DiskBlockSize;
  while (blocks_map.size() < minimal_block_cnt) {
    auto alloc_res = this->metadata_server_->call("alloc_block", id);
    if (alloc_res.is_err()) {
      return alloc_res.unwrap_error();
    }
    auto allocated_block_info = alloc_res.unwrap()->as<BlockInfo>();
    blocks_map.push_back(allocated_block_info);
  }
  // TODO: I suppose the client cannot truncate the file, so no need to free
  //       blocks.

  // 2. Write the data to corresponded blocks.
  auto begin_block = offset / DiskBlockSize;
  auto begin_block_offset = offset % DiskBlockSize;
  usize already_write = 0;

  // Write the first block
  {
    auto [bid, mid, vid] = blocks_map.at(begin_block);
    std::vector<u8> to_write(
        std::min(static_cast<std::size_t>(DiskBlockSize - begin_block_offset),
                 data.size()));
    std::memcpy(to_write.data(), data.data(), to_write.size());
    auto write_res =
        this->write_to_data_server_(mid, bid, begin_block_offset, to_write);
    if (write_res.is_err()) {
      return write_res.unwrap_error();
    }
    if (!write_res.unwrap()) {
      return ErrorType::INVALID;
    }
    already_write += to_write.size();
  }
  // Write other blocks
  for (auto cur_block = begin_block + 1;
       already_write < data.size() &&
       cur_block < blocks_map.size() /* always true */;
       ++cur_block) {

    auto [bid, mid, vid] = blocks_map.at(cur_block);
    std::vector<u8> to_write(std::min(static_cast<std::size_t>(DiskBlockSize),
                                      data.size() - already_write));
    std::memcpy(to_write.data(), data.data() + already_write, to_write.size());
    auto write_res = this->write_to_data_server_(mid, bid, 0, to_write);
    if (write_res.is_err()) {
      return write_res.unwrap_error();
    }
    if (!write_res.unwrap()) {
      return ErrorType::INVALID;
    }
    already_write += to_write.size();
  }
  CHFS_VERIFY(already_write == data.size(), "the size mismatch");

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto res = this->metadata_server_->call("free_block", id, block_id, mac_id);
  if (res.is_err()) {
    return res.unwrap_error();
  }
  if (!res.unwrap()->as<bool>()) {
    return ErrorType::INVALID;
  }
  return KNullOk;
}

ChfsNullResult ChfsClient::read_from_data_server_(u8 *target,
                                                  BlockInfo block_info) const {
  return this->read_from_data_server_(target, block_info, 0, DiskBlockSize);
}

ChfsNullResult ChfsClient::read_from_data_server_(u8 *target,
                                                  BlockInfo block_info,
                                                  usize offset,
                                                  usize len) const {
  auto [bid, mid, vid] = block_info;
  auto read_res =
      this->data_servers_.at(mid)->call("read_data", bid, offset, len, vid);
  if (read_res.is_err()) {
    return read_res.unwrap_error();
  }
  auto read_data = read_res.unwrap()->as<std::vector<u8>>();
  /**
   * Invalid read. May occur when
   *    1. the block doesn't exist,
   *    2. the version id is stale.
   */
  if (read_data.empty()) {
    return ErrorType::INVALID;
  }
  std::memcpy(target, read_data.data(), len);
  return KNullOk;
}

ChfsResult<std::vector<BlockInfo>> ChfsClient::get_block_map_(inode_id_t id) {
  auto blocks_response = this->metadata_server_->call("get_block_map", id);
  if (blocks_response.is_err()) {
    return blocks_response.unwrap_error();
  }
  auto blocks_map = blocks_response.unwrap()->as<std::vector<BlockInfo>>();
  // The block map can be empty, which means the file contains no data.
  return blocks_map;
}
ChfsResult<bool> ChfsClient::write_to_data_server_(
    mac_id_t mid, block_id_t bid, const std::vector<u8> &data) {
  return this->write_to_data_server_(mid, bid, 0, data);
}

ChfsResult<bool> ChfsClient::write_to_data_server_(
    mac_id_t mid, block_id_t bid, usize offset, const std::vector<u8> &data) {
  auto write_res =
      this->data_servers_.at(mid)->call("write_data", bid, offset, data);
  if (write_res.is_err()) {
    return write_res.unwrap_error();
  }
  return write_res.unwrap()->as<bool>();
}

} // namespace chfs