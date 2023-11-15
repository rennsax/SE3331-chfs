//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// client.h
//
// Identification: src/include/distributed/client.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/config.h"
#include "common/result.h"
#include "distributed/metadata_server.h"
#include "librpc/client.h"
#include "metadata/inode.h"

namespace chfs {

/**
 * `ChfsClient` is similar to `FileOperation` in lab1. But it's in a distributed
 * situation. It builds connection with `MetadataServer` and `DataServer` and
 * sends various requests to them. If someone wants to interact with distributed
 * Chfs, he/she should use `ChfsClient` to.
 *
 */
class ChfsClient {

public:
  enum class ServerType {
    DATA_SERVER,
    METADATA_SERVER
  };
  enum class FileType : u8 {
    REGULAR = 1,
    DIRECTORY
  }; // for passing param

  /**
   * Constructor.
   *
   * Notice that it wouldn't create connections with these servers to support
   * adding servers dynamically. Users need to explicitly call `reg_server`
   * function to build connections with other machine.
   */
  explicit ChfsClient();

  /**
   * Register a server to the client for communication. It also should be
   * called before the client is regarded as started. Once started, it wouldn't
   * allow any new connection to build.
   *
   * @param type: Whether the server is data server or metadata server.
   * @param address: The address of the server.
   * @param port: The port of the server.
   * @param reliable: Whether the network is reliable or not.
   *
   * @return: If running, the registration is failed.
   */
  auto reg_server(ServerType type, const std::string &address, u16 port,
                  bool reliable) -> ChfsNullResult;

  /**
   * Some Filesystem operations for client.
   *
   * @param type: The type of the file to be created.
   * @param parent: The parent directory of the node to be created.
   * @param name: The name of the node to be created.
   *
   * @return: The inode number of the new created node.
   */
  auto mknode(FileType type, inode_id_t parent, const std::string &name)
      -> ChfsResult<inode_id_t>;

  /**
   * It deletes an file on chfs from its parent.
   *
   * @return: Whether the operation is successful.
   */
  auto unlink(inode_id_t parent, const std::string &name) -> ChfsNullResult;

  /**
   * It looks up the directory and search for the inode number
   * of the given name.
   *
   * @param parent: The parent directory of the node to be found.
   * @param name: The name of the node to be removed.
   *
   * @return: The inode number of the node.
   */
  auto lookup(inode_id_t parent, const std::string &name)
      -> ChfsResult<inode_id_t>;

  /**
   * It reads the content of a directory.
   *
   * @param id: The inode id of the directory.
   *
   * @return: The content of the directory.
   */
  auto readdir(inode_id_t id)
      -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>;

  /**
   * It returns the type and attribute of a file.
   *
   * @param id: The inode id of the file.
   */
  auto get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>;

  /**
   * It reads some bytes from a file.
   *
   * @param id: The inode id of the file.
   * @param offset: The offset of the file to read.
   * @param size: The size of the data to read.
   *
   * @return: The content of the file.
   */
  auto read_file(inode_id_t id, usize offset, usize size)
      -> ChfsResult<std::vector<u8>>;

  /**
   * It writes some bytes into a file.
   *
   * @param id: The inode id of the file.
   * @param offset: The offset of the file to write.
   * @param data: The data to write.
   *
   * @return: Whether the operation is successful.
   */
  auto write_file(inode_id_t id, usize offset, std::vector<u8> data)
      -> ChfsNullResult;

  /**
   * It removes a block from a file.
   *
   * @param id: The inode id of the file.
   * @param block_id: The block id of the block to be removed.
   */
  auto free_file_block(inode_id_t id, block_id_t block_id, mac_id_t mac_id)
      -> ChfsNullResult;

private:
  std::map<mac_id_t, std::shared_ptr<RpcClient>> data_servers_;
  std::shared_ptr<RpcClient>
      metadata_server_; // Currently only one metadata server
  mac_id_t num_data_servers;

  /**
   * @brief Get the block map from metadata server.
   *
   * @param id the inode id. In the protocol, we assume the id must correspond
   *           to a regular inode.
   * @return a vector of block information, or an error when RPC timeout.
   */
  ChfsResult<std::vector<BlockInfo>> get_block_map_(inode_id_t id);
  /**
   * @brief Overload.
   */
  ChfsNullResult read_from_data_server_(u8 *target, BlockInfo block_info) const;
  /**
   * @brief Utility to read a block from the data server.
   *
   * @param target the address to which the data is written.
   * @param block_info consists of block id, mac id and version id, which are
   *                   to locate the block.
   * @return Error when the read fails.
   */
  ChfsNullResult read_from_data_server_(u8 *target, BlockInfo block_info,
                                        usize offset, usize len) const;

  /**
   * @brief Overload.
   *
   */
  ChfsResult<bool> write_to_data_server_(mac_id_t mid, block_id_t bid,
                                         const std::vector<u8> &data);
  /**
   * @brief Utility to write some data to the block on the data server.
   *
   * @param mid mac id
   * @param bid block id
   * @param offset where to begin the write
   * @param data the data to write. data.size() denotes how many bytes to write.
   * @return true on write ok. Error on RPC timeout.
   */
  ChfsResult<bool> write_to_data_server_(mac_id_t mid, block_id_t bid,
                                         usize offset,
                                         const std::vector<u8> &data);
};

} // namespace chfs