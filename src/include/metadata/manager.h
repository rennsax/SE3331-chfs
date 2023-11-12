//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// manager.h
//
// Identification: src/include/metadata/manager.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "./inode.h"
#include "block/allocator.h"
#include "common/bitmap.h"
#include "common/config.h"
#include "common/result.h"

namespace chfs {

// inode should be larger than 0
const inode_id_t KInvalidInodeID = 0;

class FileOperation;

/**
 * The manager manages the inode.
 * This includes managing an inode table.
 *
 * At a high-level, the inode manager assumes the following layout on blocks:
 * | block 0     | block 1  ...  | block x ...             | block y ... | block
 * N  ...         |
 * | Super block | Inode Table   | Inode allocation bitmap |
 * Block allocation bitmap ... |  Other data blocks   |
 */
class InodeManager {
  friend class FileOperation;
  // We will modify the block manager
  std::shared_ptr<BlockManager> bm;
  u64 max_inode_supported;
  u64 n_table_blocks;
  u64 n_bitmap_blocks;

public:
  /**
   * Construct an InodeManager from scratch.
   * Note that it will initialize the blocks in the block manager.
   */
  InodeManager(std::shared_ptr<BlockManager> bm, u64 max_inode_supported);

  static auto to_shared_ptr(InodeManager m) -> std::shared_ptr<InodeManager> {
    return std::make_shared<InodeManager>(m);
  }

  /**
   * Construct an InodeManager from a block manager.
   * Note that it won't modify any blocks on the block manager.
   *
   * The max_inode_supported can be found in the super block.
   */
  static auto create_from_block_manager(std::shared_ptr<BlockManager> bm,
                                        u64 max_inode_supported)
      -> ChfsResult<InodeManager>;

  /**
   * Get the maximum number of inode supported.
   * The number is determined when the file system is created.
   */
  auto get_max_inode_supported() const -> u64 {
    return max_inode_supported;
  }

  /**
   * Allocate and initialize an inode with proper type
   * @param type: file type
   * @param bid: inode block ID
   */
  auto allocate_inode(InodeType type, block_id_t bid) -> ChfsResult<inode_id_t>;

  /**
   * @brief Allocate and initialize a regular inode. Only for lab2.
   *
   * @param bid
   */
  ChfsResult<inode_id_t> allocate_regular_inode(block_id_t bid);

  /**
   * Get the number of free inodes
   * @return the number of free inodes if Ok
   */
  auto free_inode_cnt() const -> ChfsResult<u64>;

  /**
   * Get the block ID of the inode
   * @param id: **logical** inode ID
   *
   * Note that we don't check whether the returned block id is valid
   */
  auto get(inode_id_t id) -> ChfsResult<block_id_t>;

  /**
   * Free the inode entry id
   */
  auto free_inode(inode_id_t id) -> ChfsNullResult;

  /**
   * Get the attribute of the inode
   */
  auto get_attr(inode_id_t id) -> ChfsResult<FileAttr>;

  /**
   * Set the type of the inode
   */
  auto get_type(inode_id_t id) -> ChfsResult<InodeType>;

  /**
   * The combined version of the above APIs
   */
  auto get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>;

  auto get_reserved_blocks() const -> usize {
    return 1 + n_table_blocks + n_bitmap_blocks;
  }

  // helper functions

  /**
   * Set the block ID of the inode
   * @param idx: **physical** inode ID
   */
  auto set_table(inode_id_t idx, block_id_t bid) -> ChfsNullResult;

private:
  /**
   * Simple constructors
   */
  InodeManager(std::shared_ptr<BlockManager> bm, u64 max_inode_supported,
               u64 ntables, u64 nbit)
      : bm(bm), max_inode_supported(max_inode_supported),
        n_table_blocks(ntables), n_bitmap_blocks(nbit) {
  }

  /**
   * Read the inode to a buffer
   * @param block_id_t: the block id that stores the inode
   */
  auto read_inode(inode_id_t id, std::vector<u8> &buffer)
      -> ChfsResult<block_id_t>;

protected:
  /**
   * @brief Template function to adapt for different kinds of inodes.
   *
   * @tparam InodeT type of inode (language-level) to allocate
   *         The type InodeT should have the following two signatures:
   *         1) InodeT::InodeT(InodeType, usize);
   *         2) InodeT::flush_to_buffer(u8*);
   *         Otherwise, the compilation fails.
   * @param type type of inode (fs-level), dir or file
   * @param bid the block id to which the inode is allocated
   */
  template <typename InodeT>
  ChfsResult<inode_id_t> allocate_inode_template(InodeType type,
                                                 block_id_t bid);
};

/**
 * Transform a raw inode ID that index the table to a logic inode ID (and vice
 * verse) This prevents the inode ID with 0 to mix up with the invalid one
 */
#define RAW_2_LOGIC(i) (i + 1)
#define LOGIC_2_RAW(i) (i - 1)

template <typename InodeT>
inline ChfsResult<inode_id_t> InodeManager::allocate_inode_template(
    InodeType type, block_id_t bid) {
  auto iter_res = BlockIterator::create(this->bm.get(), 1 + n_table_blocks,
                                        1 + n_table_blocks + n_bitmap_blocks);
  if (iter_res.is_err()) {
    return ChfsResult<inode_id_t>(iter_res.unwrap_error());
  }

  inode_id_t count = 0;

  // Find an available inode ID.
  for (auto iter = iter_res.unwrap(); iter.has_next();
       iter.next(bm->block_size()).unwrap(), count++) {
    auto data = iter.unsafe_get_value_ptr<u8>();
    auto bitmap = Bitmap(data, bm->block_size());
    auto free_idx = bitmap.find_first_free();

    if (free_idx) {
      // If there is an available inode ID.

      // Setup the bitmap.
      bitmap.set(free_idx.value());
      auto res = iter.flush_cur_block();
      if (res.is_err()) {
        return ChfsResult<inode_id_t>(res.unwrap_error());
      }

      // 1. Initialize the inode with the given type.
      std::vector<u8> buffer(this->bm->block_size());

      // Here comes the differences of specializations.
      InodeT inode{type, this->bm->block_size()};
      inode.flush_to_buffer(buffer.data());

      if (auto res = this->bm->write_block(bid, buffer.data()); res.is_err()) {
        return res.unwrap_error();
      }
      // 2. Setup the inode table.
      if (auto res = this->set_table(free_idx.value(), bid); res.is_err()) {
        return res.unwrap_error();
      }
      // 3. Return the id of the allocated inode.
      //    You may have to use the `RAW_2_LOGIC` macro
      //    to get the result inode id.
      return ChfsResult<inode_id_t>(RAW_2_LOGIC(free_idx.value()));
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
}

} // namespace chfs