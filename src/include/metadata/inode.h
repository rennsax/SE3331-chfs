//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// inode.h
//
// Identification: src/include/metadata/inode.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iterator>
#include <string.h>
#include <time.h>

#include "common/config.h"
#include "common/macros.h"

#include "block/allocator.h"
#include "block/manager.h"

namespace chfs {

// The first block is reserved as super block
// So block IDs should be larger than 0
const block_id_t KInvalidBlockID = 0;

enum class InodeType : u32 {
  Unknown = 0,
  FILE = 1,
  Directory = 2,
};

class Inode;
class FileOperation;

/**
 * Abstract the file attributes
 */
class FileAttr {
  friend class Inode;

public:
  u64 atime = 0;
  u64 mtime = 0;
  u64 ctime = 0;
  u64 size = 0;

  auto set_mtime(u64 t) {
    mtime = t;
  }

  auto set_all_time(u64 t) {
    atime = t;
    mtime = t;
    ctime = t;
  }

} __attribute__((packed));

static_assert(sizeof(FileAttr) == 32, "Unexpected FileAttr size");

class InodeIterator;
class InodeManager;

/**
 * Abstract the inode.
 * Note for the following two things:
 * 1. the inode layout should fit exactly in a single block.
 * 2. the execution of the API is **not** thread-safe
 *
 * The INode (currently)adopts a simple design:
 * - The last block is an indirect
 * - Others are the direct blocks
 *
 */
class Inode {
  friend class InodeIterator;
  friend class InodeManager;
  friend class FileOperation;

  InodeType type;
  FileAttr inner_attr;
  u32 block_size;

  // we stored the number of blocks in the inode to prevent
  // re-calculation during runtime
  u32 nblocks;
  // The actual number of blocks should be larger,
  // which is dynamically calculated based on the block size
public:
  [[maybe_unused]] block_id_t blocks[0];

public:
  /**
   * Create a new inode for a file or directory
   * @param type: the inode type
   * @param block_size: the size of the block that stored the inode
   */
  Inode(InodeType type, usize block_size)
      : type(type), inner_attr(), block_size(block_size) {
    CHFS_VERIFY(block_size > sizeof(Inode), "Block size too small");
    nblocks = (block_size - sizeof(Inode)) / sizeof(block_id_t);
    inner_attr.set_all_time(time(0));
  }

  /**
   * Get type of the inode
   */
  auto get_type() const -> InodeType {
    return type;
  }

  /**
   * Get file attr of the inode
   */
  auto get_attr() const -> FileAttr {
    return inner_attr;
  }

  /**
   * Get the file size
   */
  auto get_size() const -> u64 {
    return inner_attr.size;
  }

  /**
   * Get the number of blocks of the inode
   */
  auto get_nblocks() const -> u32 {
    return nblocks;
  }

  /**
   * Get the number of direct blocks stored in this inode
   */
  auto get_direct_block_num() const -> u32 {
    return nblocks - 1;
  }

  /**
   * Determine whether the block ID can be stored directly in the inode
   * @param idx the place of the block ID to store
   */
  auto is_direct_block(usize idx) const -> bool {
    return idx < (nblocks - 1);
  }

  /**
   * Get the maximum file size supported by the inode
   */
  auto max_file_sz_supported() const -> u64 {
    const auto max_blocks_in_block = block_size / sizeof(block_id_t);
    return static_cast<u64>(max_blocks_in_block) *
               static_cast<u64>(block_size) +
           static_cast<u64>(this->get_nblocks() - 1) *
               static_cast<u64>(block_size);
  }

  /**
   * Write the Inode data to a buffer.
   * Note that: it won't flush the blocks.
   *
   * # Warm
   * This function is only called during construction.
   *
   * @param buffer: the buffer to write to and must be in [BLCOK_SIZE]
   */
  auto flush_to_buffer(u8 *buffer) const {
    memcpy(buffer, this, sizeof(Inode));
    auto inode_p = reinterpret_cast<Inode *>(buffer);
    for (uint i = 0; i < this->nblocks; ++i) {
      inode_p->blocks[i] = KInvalidBlockID;
    }
  }

  /**
   * Write the indirect block back to the block manager
   *
   * @param bm the block manager
   */
  auto write_indirect_block(std::shared_ptr<BlockManager> &bm,
                            std::vector<u8> &buffer) -> ChfsNullResult;

  /**
   * Set the direct block ID given an index
   */
  auto set_block_direct(usize idx, block_id_t bid) {
    this->blocks[idx] = bid;
  }

  /**
   * Warning: this function is particular dangerous.
   * It won't cause memory leakage if and only if the inode is created from a
   * block. i.e.,
   * ```
   * u8 block[BLOCK_SIZE];
   * Inode *inode = (Inode *)block;
   * ```
   */
  block_id_t &operator[](size_t index) {
    if (index >= this->nblocks)
      throw std::out_of_range("Index out of range");
    return this->blocks[index];
  }

  /**
   * Get the block ID of the indirect block.
   * If the indirect block ID is not set,
   * allocate and set one at the inode.
   *
   * Note that the modification will not immediately write back to the inode
   *
   * @param allocator the block allocator
   */
  auto get_or_insert_indirect_block(std::shared_ptr<BlockAllocator> &allocator)
      -> ChfsResult<block_id_t> {
    if (this->blocks[this->nblocks - 1] == KInvalidBlockID) {
      // aha, we need to allocate one
      auto bid = allocator->allocate();
      if (bid.is_err()) {
        return ChfsResult<block_id_t>(bid.unwrap_error());
      }
      this->blocks[this->nblocks - 1] = bid.unwrap();
    }
    return ChfsResult<block_id_t>(this->blocks[this->nblocks - 1]);
  }

  auto get_indirect_block_id() -> block_id_t {
    CHFS_ASSERT(this->blocks[this->nblocks - 1] != KInvalidBlockID,
                "Indirect block not set");
    return this->blocks[this->nblocks - 1];
  }

  auto invalid_indirect_block_id() {
    this->blocks[this->nblocks - 1] = KInvalidBlockID;
  }

  auto begin() -> InodeIterator;
  auto end() -> InodeIterator;

} __attribute__((packed));

static_assert(sizeof(Inode) == sizeof(FileAttr) + sizeof(InodeType) +
                                   sizeof(u32) + sizeof(u32),
              "Unexpected Inode size");

/**
 * A helper class to iterate through the block_ids stored in the inode
 *
 * Note that the iteration of this class is **not** thread-safe.
 *
 */
class InodeIterator {
  friend class Inode;
  int cur_idx;
  Inode *inode;
  /**
   * Constructors: note that we don't check the inode
   */
  explicit InodeIterator(Inode *inode, int idx) : cur_idx(idx), inode(inode) {
  }

  InodeIterator(Inode *inode) : InodeIterator(inode, 0) {
  }

public:
  InodeIterator &operator++() {
    cur_idx++;
    return *this;
  }

  InodeIterator operator++(int) {
    InodeIterator tmp(*this);
    operator++();
    return tmp;
  }

  bool operator==(const InodeIterator &rhs) const {
    return cur_idx == rhs.cur_idx;
  }
  bool operator!=(const InodeIterator &rhs) const {
    return cur_idx != rhs.cur_idx;
  }
  block_id_t &operator*() const {
    return inode->blocks[cur_idx];
  }
};

/**
 * @brief Regular Inode in the metadata server. Only for lab2.
 *
 * A regular inode also occupies exactly one block.
 * The differences:
 *    1) the inode never points to some blocks in the metadata server,
 *    2) instead, it directly stores <block_id, mac_id> pairs, which are
 *       used to find the file data in other slave data servers.
 * Suppose we have a regular inode in a block, taking 4096 bytes,
 * then the layout of the regular inode is:
 *    type (4) + attr (8*4=32) + nblocks (4)
 *    + block_mp (8+4+4) * n_blocks <= block_size = 4096
 * We can infer that max_nblocks ~= 270.
 *
 */
struct RegularInode {
  struct _BlockEntity {
    block_id_t bid;
    version_t vid;
    mac_id_t mid;
  };
  using BlockEntity = _BlockEntity;

  // This inode implies the file type.
  // InodeType type = InodeType::FILE;
  FileAttr inner_attr;
  u32 nblocks; // number of <block_id, mac_id> pairs
  std::vector<BlockEntity> blocks;

  /**
   * @brief Construct a new RegularInode object with raw data
   *
   * @param inode_data the byte stream
   */
  RegularInode(const std::vector<u8> &inode_data) {
    const u8 *buffer_pos = inode_data.data();
    InodeType type{};
    memcpy(&type, buffer_pos, sizeof(type));
    buffer_pos += sizeof(InodeType);

    CHFS_VERIFY(type == InodeType::FILE,
                "cannot read a regular node from an inode from other type");

    memcpy(&this->inner_attr, buffer_pos, sizeof(this->inner_attr));
    buffer_pos += sizeof(this->inner_attr);
    memcpy(&this->nblocks, buffer_pos, sizeof(this->nblocks));
    buffer_pos += sizeof(this->nblocks);
    blocks.resize(this->nblocks);
    memcpy(this->blocks.data(), buffer_pos,
           this->nblocks * sizeof(BlockEntity));
  }

  RegularInode(InodeType type, usize block_size) : inner_attr{}, nblocks{0} {
    CHFS_VERIFY(type == InodeType::FILE,
                "the inode type of RegularInode must be FILE!");
    inner_attr.set_all_time(time(0));
  }

  /**
   * Dump the Inode data to a buffer.
   *
   * @param buffer: the buffer to write to and must be in [BLOCK_SIZE]
   */
  void flush_to_buffer(u8 *buffer) const {
    constexpr InodeType _type = InodeType::FILE;
    u8 *buffer_pos = buffer;
    memcpy(buffer_pos, &_type, sizeof(_type)); // 4 bytes
    buffer_pos += sizeof(_type);
    memcpy(buffer_pos, &this->inner_attr, sizeof(this->inner_attr)); // 32 bytes
    buffer_pos += sizeof(this->inner_attr);
    memcpy(buffer_pos, &this->nblocks, sizeof(this->nblocks)); // 4 bytes
    buffer_pos += sizeof(this->nblocks);
    memcpy(buffer_pos, this->blocks.data(),
           sizeof(BlockEntity) * this->nblocks);
  }
};

} // namespace chfs
