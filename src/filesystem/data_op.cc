#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {

  // 1. Allocate a block for the inode.
  auto block_id_res = this->block_allocator_->allocate();
  if (block_id_res.is_err()) {
    return block_id_res.unwrap_error();
  }
  auto block_id = block_id_res.unwrap();
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  auto res = this->inode_manager_->allocate_inode(type, block_id);
  if (res.is_err()) {
    return res.unwrap_error();
  }

  return res.unwrap();
}

ChfsResult<inode_id_t> FileOperation::alloc_metadata_server_inode(
    InodeType type) {
  // 1. Allocate a block for the inode.
  auto block_id_res = this->block_allocator_->allocate();
  if (block_id_res.is_err()) {
    return block_id_res.unwrap_error();
  }
  auto block_id = block_id_res.unwrap();
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  if (type == InodeType::FILE) {
    return this->inode_manager_->allocate_regular_inode(block_id);
  }
  return this->inode_manager_->allocate_inode(type, block_id);
}

auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
  return this->inode_manager_->get_attr(id);
}

auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  return this->inode_manager_->get_type_attr(id);
}

auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
  return this->inode_manager_->get_type(id);
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
  return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                     u64 offset) -> ChfsResult<u64> {
  auto read_res = this->read_file(id);
  if (read_res.is_err()) {
    return ChfsResult<u64>(read_res.unwrap_error());
  }

  auto content = read_res.unwrap();
  if (offset + sz > content.size()) {
    content.resize(offset + sz);
  }
  memcpy(content.data() + offset, data, sz);

  auto write_res = this->write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<u64>(write_res.unwrap_error());
  }
  return ChfsResult<u64>(sz);
}

ChfsNullResult FileOperation::append_block_to_regular_inode(
    inode_id_t id, const RegularInode::BlockEntity &block) {

  auto get_regular_inode_res = this->get_regular_inode(id);
  if (get_regular_inode_res.is_err()) {
    return get_regular_inode_res.unwrap_error();
  }
  auto [inode_bid, inode] = get_regular_inode_res.unwrap();

  std::vector<u8> buffer(this->block_manager_->block_size());

  inode.blocks.push_back(block);
  inode.inner_attr.mtime = time(NULL);
  inode.nblocks++;
  inode.flush_to_buffer(buffer.data());
  if (auto res = this->block_manager_->write_block(inode_bid, buffer.data());
      res.is_err()) {
    return res.unwrap_error();
  }
  return KNullOk;
}

ChfsResult<u32> FileOperation::delete_block_from_regular_inode(
    inode_id_t id,
    std::function<bool(const RegularInode::BlockEntity &)> predicate) {
  auto get_regular_inode_res = this->get_regular_inode(id);
  if (get_regular_inode_res.is_err()) {
    return get_regular_inode_res.unwrap_error();
  }
  auto [inode_bid, inode] = get_regular_inode_res.unwrap();

  std::vector<u8> buffer(this->block_manager_->block_size());
  // Remove block entities with the predicate.
  auto after_rm_end =
      std::remove_if(begin(inode.blocks), end(inode.blocks), predicate);
  inode.blocks.erase(after_rm_end, inode.blocks.end());

  // Adjust the some metadata.
  auto rm_cnt = inode.nblocks - inode.blocks.size();
  inode.nblocks -= rm_cnt;
  inode.inner_attr.mtime = time(NULL);

  // Write into the disk.
  inode.flush_to_buffer(buffer.data());
  if (auto res = this->block_manager_->write_block(inode_bid, buffer.data());
      res.is_err()) {
    return res.unwrap_error();
  }
  return rm_cnt;
}

// {Your code here}
auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
    -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inlined_blocks_num = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  } else {
    inlined_blocks_num = inode_p->get_direct_block_num();
  }

  if (content.size() > inode_p->max_file_sz_supported()) {
    std::cerr << "file size too large: " << content.size() << " vs. "
              << inode_p->max_file_sz_supported() << std::endl;
    error_code = ErrorType::OUT_OF_RESOURCE;
    goto err_ret;
  }

  // If the indirect block exists, read it
  if (auto indirect_bid = (*inode_p)[inode_p->nblocks - 1];
      indirect_bid != KInvalidBlockID) {
    indirect_block.resize(inode_p->get_size() -
                          block_size * inlined_blocks_num);
    this->block_manager_->read_block(indirect_bid, indirect_block.data());
  }

  // 2. make sure whether we need to allocate more blocks
  original_file_sz = inode_p->get_size();
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(content.size(), block_size);

  if (new_block_num > old_block_num) {
    // If we need to allocate more blocks.
    if (!inode_p->is_direct_block(new_block_num - 1)) {
      // Allocate indirect block
      const auto indirect_bid_res =
          inode_p->get_or_insert_indirect_block(this->block_allocator_);
      if (indirect_bid_res.is_err()) {
        error_code = indirect_bid_res.unwrap_error();
        goto err_ret;
      }
    }
    for (usize idx = old_block_num; idx < new_block_num; ++idx) {

      // 1. Allocate a block.
      const auto block_id_res = this->block_allocator_->allocate();
      if (block_id_res.is_err()) {
        error_code = inode_res.unwrap_error();
        goto err_ret;
      }
      const auto bid = block_id_res.unwrap();
      // 2. Fill the allocated block id to the inode.
      //    You should pay attention to the case of indirect block.
      //    You may use function `get_or_insert_indirect_block`
      //    in the case of indirect block.
      if (inode_p->is_direct_block(idx)) {
        (*inode_p)[idx] = bid;
      } else {
        auto offset = idx - inlined_blocks_num;
        CHFS_ASSERT((*inode_p)[inlined_blocks_num] != KInvalidBlockID,
                    "Unexpected unallocated indirect block");
        if (auto least_size = (offset + 1) * sizeof(block_id_t);
            least_size > indirect_block.size()) {
          indirect_block.resize(least_size);
        }
        reinterpret_cast<block_id_t *>(indirect_block.data())[offset] = bid;
      }
    }

  } else {
    // We need to free the extra blocks.
    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      if (inode_p->is_direct_block(idx)) {
        auto &bid = (*inode_p)[idx];
        CHFS_ASSERT(bid != KInvalidBlockID, "Unexpected invalid block id");
        if (auto res = this->block_allocator_->deallocate(bid); res.is_err()) {
          error_code = res.unwrap_error();
          goto err_ret;
        }
        bid = KInvalidBlockID;
      } else {
        const auto block_offset = idx - (inlined_blocks_num);
        auto &bid =
            reinterpret_cast<block_id_t *>(indirect_block.data())[block_offset];
        CHFS_ASSERT(bid != KInvalidBlockID, "Unexpected invalid block id");
        if (auto res = this->block_allocator_->deallocate(bid); res.is_err()) {
          error_code = res.unwrap_error();
          goto err_ret;
        }
        bid = KInvalidBlockID;
      }
    }

    // If there are no more indirect blocks.
    if (old_block_num > inlined_blocks_num &&
        new_block_num <= inlined_blocks_num && true) {

      auto res =
          this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
      if (res.is_err()) {
        error_code = res.unwrap_error();
        goto err_ret;
      }
      indirect_block.clear();
      inode_p->invalid_indirect_block_id();
    }
  }

  // 3. write the contents
  inode_p->inner_attr.size = content.size();
  inode_p->inner_attr.mtime = time(0);

  {
    auto block_idx = 0;
    u64 write_sz = 0;

    while (write_sz < content.size()) {
      auto sz = ((content.size() - write_sz) > block_size)
                    ? block_size
                    : (content.size() - write_sz);
      std::vector<u8> buffer(block_size);
      memcpy(buffer.data(), content.data() + write_sz, sz);

      block_id_t bid = KInvalidBlockID;
      if (inode_p->is_direct_block(block_idx)) {
        bid = (*inode_p)[block_idx];
      } else {
        bid = reinterpret_cast<block_id_t *>(
            indirect_block.data())[block_idx - inlined_blocks_num];
      }
      CHFS_ASSERT(bid != KInvalidBlockID, "Unexpected invalid block");

      this->block_manager_->write_block(bid, buffer.data());

      write_sz += sz;
      block_idx += 1;
    }
  }

  // finally, update the inode
  {
    // TODO why set all the time
    inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {
      error_code = write_res.unwrap_error();
      goto err_ret;
    }
    if (indirect_block.size() != 0) {
      write_res =
          inode_p->write_indirect_block(this->block_manager_, indirect_block);
      if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
    }
  }

  return KNullOk;

err_ret:
  // std::cerr << "write file return error: " << (int)error_code << std::endl;
  return ChfsNullResult(error_code);
}

// {Your code here}
auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
  auto error_code = ErrorType::DONE;
  std::vector<u8> content;

  const auto block_size = this->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  u64 file_sz = 0;
  u64 read_sz = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  }

  file_sz = inode_p->get_size();
  content.reserve(file_sz);

  // If the indirect block exists, read it
  if (auto indirect_bid = (*inode_p)[inode_p->nblocks - 1];
      indirect_bid != KInvalidBlockID) {
    this->block_manager_->read_block(indirect_bid, indirect_block.data());
  }

  // Now read the file
  while (read_sz < file_sz) {
    auto sz = ((inode_p->get_size() - read_sz) > block_size)
                  ? block_size
                  : (inode_p->get_size() - read_sz);
    std::vector<u8> buffer(block_size);

    // Get current block id.
    auto idx = read_sz / block_size;
    block_id_t bid = KInvalidBlockID;
    if (inode_p->is_direct_block(idx)) {
      bid = (*inode_p)[idx];
    } else {
      bid = reinterpret_cast<block_id_t *>(
          indirect_block.data())[idx - inode_p->get_direct_block_num()];
    }
    CHFS_ASSERT(bid != KInvalidBlockID, "Unexpected invalid block");
    this->block_manager_->read_block(bid, buffer.data());

    content.resize(content.size() + sz);
    memcpy(content.data() + read_sz, buffer.data(), sz);
    // std::copy(buffer.cbegin(), buffer.cbegin() + sz, content.begin() +
    // read_sz);

    read_sz += sz;
  }
  inode_p->inner_attr.set_mtime(time(nullptr));

  CHFS_ASSERT(content.size() == file_sz, "The size of content doesn't match");
  return content;

err_ret:
  return ChfsResult<std::vector<u8>>(error_code);
}

ChfsResult<std::vector<RegularInode::BlockEntity>> FileOperation::
    read_regular_node(inode_id_t id) {
  auto get_res = this->get_regular_inode(id);
  if (get_res.is_err()) {
    return get_res.unwrap_error();
  }
  return get_res.unwrap().second.blocks;
}

auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
  auto res = read_file(id);
  if (res.is_err()) {
    return res;
  }

  auto content = res.unwrap();
  return ChfsResult<std::vector<u8>>(
      std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
}

auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
  auto attr_res = this->getattr(id);
  if (attr_res.is_err()) {
    return ChfsResult<FileAttr>(attr_res.unwrap_error());
  }

  auto attr = attr_res.unwrap();
  auto file_content = this->read_file(id);
  if (file_content.is_err()) {
    return ChfsResult<FileAttr>(file_content.unwrap_error());
  }

  auto content = file_content.unwrap();

  if (content.size() != sz) {
    content.resize(sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err()) {
      return ChfsResult<FileAttr>(write_res.unwrap_error());
    }
  }

  attr.size = sz;
  return ChfsResult<FileAttr>(attr);
}

ChfsResult<std::pair<block_id_t, RegularInode>> FileOperation::
    get_regular_inode(inode_id_t id) {
  std::vector<u8> buffer(this->block_manager_->block_size());
  auto inode_bid_res = this->inode_manager_->read_inode(id, buffer);
  if (inode_bid_res.is_err()) {
    return inode_bid_res.unwrap_error();
  }
  auto inode_res = RegularInode::from(buffer);
  if (inode_res.is_err()) {
    return inode_res.unwrap_error();
  }
  return std::make_pair(inode_bid_res.unwrap(), inode_res.unwrap());
}

} // namespace chfs
