#include <vector>

#include "common/bitmap.h"
#include "metadata/inode.h"
#include "metadata/manager.h"

namespace chfs {

/**
 * Transform a raw inode ID that index the table to a logic inode ID (and vice
 * verse) This prevents the inode ID with 0 to mix up with the invalid one
 */
#ifndef RAW_2_LOGIC
#define RAW_2_LOGIC(i) (i + 1)
#endif
#ifndef LOGIC_2_RAW
#define LOGIC_2_RAW(i) (i - 1)
#endif

InodeManager::InodeManager(std::shared_ptr<BlockManager> bm,
                           u64 max_inode_supported)
    : bm(bm) {
  // 1. calculate the number of bitmap blocks for the inodes
  auto inode_bits_per_block = bm->block_size() * KBitsPerByte;
  auto blocks_needed = max_inode_supported / inode_bits_per_block;

  // we align the bitmap to simplify bitmap calculations
  if (blocks_needed * inode_bits_per_block < max_inode_supported) {
    blocks_needed += 1;
  }
  this->n_bitmap_blocks = blocks_needed;

  // we may enlarge the max inode supported
  this->max_inode_supported = blocks_needed * KBitsPerByte * bm->block_size();

  // 2. initialize the inode table
  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto table_blocks = this->max_inode_supported / inode_per_block;
  if (table_blocks * inode_per_block < this->max_inode_supported) {
    table_blocks += 1;
  }
  this->n_table_blocks = table_blocks;

  // 3. clear the bitmap blocks and table blocks
  for (u64 i = 0; i < this->n_table_blocks; ++i) {
    bm->zero_block(i + 1); // 1: the super block
  }

  for (u64 i = 0; i < this->n_bitmap_blocks; ++i) {
    bm->zero_block(i + 1 + this->n_table_blocks);
  }
}

auto InodeManager::create_from_block_manager(std::shared_ptr<BlockManager> bm,
                                             u64 max_inode_supported)
    -> ChfsResult<InodeManager> {
  auto inode_bits_per_block = bm->block_size() * KBitsPerByte;
  auto n_bitmap_blocks = max_inode_supported / inode_bits_per_block;

  CHFS_VERIFY(n_bitmap_blocks * inode_bits_per_block == max_inode_supported,
              "Wrong max_inode_supported");

  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto table_blocks = max_inode_supported / inode_per_block;
  if (table_blocks * inode_per_block < max_inode_supported) {
    table_blocks += 1;
  }

  InodeManager res = {bm, max_inode_supported, table_blocks, n_bitmap_blocks};
  return ChfsResult<InodeManager>(res);
}

// { Your code here }
auto InodeManager::allocate_inode(InodeType type, block_id_t bid)
    -> ChfsResult<inode_id_t> {
  return this->allocate_inode_template<Inode>(type, bid);
}

ChfsResult<inode_id_t> InodeManager::allocate_regular_inode(block_id_t bid) {
  return this->allocate_inode_template<RegularInode>(InodeType::FILE, bid);
}

// { Your code here }
auto InodeManager::set_table(inode_id_t idx, block_id_t bid) -> ChfsNullResult {
  auto inode_table_idx = idx;
  if (inode_table_idx >= this->max_inode_supported ||
      // bid >= 0 is always valid, see FileOperation::FileOperation
      bid >= this->bm->total_blocks()) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  auto iter_res = BlockIterator::create(this->bm.get(), 1, 1 + n_table_blocks);
  if (iter_res.is_err()) {
    return ChfsNullResult(iter_res.unwrap_error());
  }
  auto inode_table_iter = iter_res.unwrap();
  inode_table_iter.next(inode_table_idx * sizeof(block_id_t));
  auto *block_id_ptr = inode_table_iter.unsafe_get_value_ptr<block_id_t>();
  *block_id_ptr = bid;
  // Remember to flush the data
  auto res = inode_table_iter.flush_cur_block();
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }

  return KNullOk;
}

// { Your code here }
auto InodeManager::get(inode_id_t id) -> ChfsResult<block_id_t> {
  block_id_t res_block_id = 0;

  auto inode_table_idx = LOGIC_2_RAW(id);
  if (inode_table_idx >= this->max_inode_supported) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto iter_res = BlockIterator::create(this->bm.get(), 1, 1 + n_table_blocks);
  if (iter_res.is_err()) {
    return ChfsResult<block_id_t>(iter_res.unwrap_error());
  }

  auto inode_table_iter = iter_res.unwrap();
  inode_table_iter.next(inode_table_idx * sizeof(block_id_t));

  res_block_id = *inode_table_iter.unsafe_get_value_ptr<block_id_t>();

  return ChfsResult<block_id_t>(res_block_id);
}

auto InodeManager::free_inode_cnt() const -> ChfsResult<u64> {
  auto iter_res = BlockIterator::create(this->bm.get(), 1 + n_table_blocks,
                                        1 + n_table_blocks + n_bitmap_blocks);

  if (iter_res.is_err()) {
    return ChfsResult<u64>(iter_res.unwrap_error());
  }

  u64 count = 0;
  for (auto iter = iter_res.unwrap(); iter.has_next();) {
    auto data = iter.unsafe_get_value_ptr<u8>();
    auto bitmap = Bitmap(data, bm->block_size());

    count += bitmap.count_zeros();

    auto iter_res = iter.next(bm->block_size());
    if (iter_res.is_err()) {
      return ChfsResult<u64>(iter_res.unwrap_error());
    }
  }
  return ChfsResult<u64>(count);
}

auto InodeManager::get_attr(inode_id_t id) -> ChfsResult<FileAttr> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode(id, buffer);
  if (res.is_err()) {
    return ChfsResult<FileAttr>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<FileAttr>(inode_p->inner_attr);
}

auto InodeManager::get_type(inode_id_t id) -> ChfsResult<InodeType> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode(id, buffer);
  if (res.is_err()) {
    return ChfsResult<InodeType>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<InodeType>(inode_p->type);
}

auto InodeManager::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode(id, buffer);
  if (res.is_err()) {
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<std::pair<InodeType, FileAttr>>(
      std::make_pair(inode_p->type, inode_p->inner_attr));
}

// Note: the buffer must be as large as block size
auto InodeManager::read_inode(inode_id_t id, std::vector<u8> &buffer)
    -> ChfsResult<block_id_t> {
  if (id >= max_inode_supported - 1) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto block_id = this->get(id);
  if (block_id.is_err()) {
    return ChfsResult<block_id_t>(block_id.unwrap_error());
  }

  if (block_id.unwrap() == KInvalidBlockID) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto res = bm->read_block(block_id.unwrap(), buffer.data());
  if (res.is_err()) {
    return ChfsResult<block_id_t>(res.unwrap_error());
  }
  return ChfsResult<block_id_t>(block_id.unwrap());
}

// {Your code}
auto InodeManager::free_inode(inode_id_t id) -> ChfsNullResult {

  // simple pre-checks
  auto inode_idx = LOGIC_2_RAW(id);
  if (inode_idx >= max_inode_supported) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  // 1. Clear the inode table entry.
  //    You may have to use macro `LOGIC_2_RAW`
  //    to get the index of inode table from `id`.
  if (auto res = this->set_table(inode_idx, 0); res.is_err()) {
    return res.unwrap_error();
  }

  // 2. Clear the inode bitmap.
  auto iter_res = BlockIterator::create(this->bm.get(), 1 + n_table_blocks,
                                        1 + n_table_blocks + n_bitmap_blocks);
  if (iter_res.is_err()) {
    return iter_res.unwrap_error();
  }
  auto bitmap_iter = iter_res.unwrap();
  if (auto res = bitmap_iter.next(inode_idx / KBitsPerByte); res.is_err()) {
    return res.unwrap_error();
  }
  Bitmap{bitmap_iter.unsafe_get_value_ptr<u8>(), 1}.clear(inode_idx %
                                                          KBitsPerByte);
  bitmap_iter.flush_cur_block();
  return KNullOk;
}

} // namespace chfs