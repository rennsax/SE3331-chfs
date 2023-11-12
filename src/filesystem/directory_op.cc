#include <algorithm>
#include <charconv>
#include <numeric>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs_util {
struct string {
  static std::vector<std::string_view> split(std::string_view str,
                                             char delimiter = ':') {
    size_t previous = 0;
    std::vector<std::string_view> res{};
    for (auto index = str.find(delimiter); index != std::string::npos;
         previous = index + 1, index = str.find(delimiter, previous)) {
      res.push_back(str.substr(previous, index - previous));
      previous = index + 1;
    }
    res.push_back(str.substr(previous));
    return res;
  }

  template <typename InputIt>
  static std::string join(InputIt begin, InputIt end, char delimiter = ' ') {
    std::string res{};
    if (begin == end) {
      return res;
    }
    res = std::string{*begin};
    for (InputIt it = begin + 1; it != end; ++it) {
      res += delimiter + std::string{(*it)};
    }
    return res;
  }
};
} // namespace chfs_util

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {
  auto entry = std::move(filename) + ':' + std::to_string(id);
  if (src.empty()) {
    return entry;
  }
  return src + '/' + entry;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {
  if (src.empty()) {
    list.clear();
    return;
  }
  auto entries = chfs_util::string::split(src, '/');
  list.resize(entries.size());
  std::transform(
      entries.begin(), entries.end(), list.begin(),
      [](std::string_view sv) -> DirectoryEntry {
        auto name_and_inode = chfs_util::string::split(sv, ':');
        CHFS_ASSERT(name_and_inode.size() == 2, "Unexpected parse error!");
        inode_id_t inode_id = KInvalidInodeID;
        std::from_chars(name_and_inode[1].begin(), name_and_inode[1].end(),
                        inode_id);
        CHFS_ASSERT(inode_id != KInvalidBlockID, "Parse inode number error!");
        return DirectoryEntry{std::string{name_and_inode[0]}, inode_id};
      });
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {
  auto entries = chfs_util::string::split(src, '/');
  auto removed_end = std::remove_if(
      entries.begin(), entries.end(), [&](std::string_view sv) -> bool {
        return chfs_util::string::split(sv)[0] == filename;
      });
  return chfs_util::string::join(entries.begin(), removed_end, '/');
}

/**
 * @brief Read raw string from the inode
 *
 * @param fs pointer to the underlying filesystem
 * @param id inode id
 * @return ChfsResult<std::string>
 */
ChfsResult<std::string> read_raw_content(FileOperation *fs, inode_id_t id) {
  auto read_res = fs->read_file(id);
  if (read_res.is_err()) {
    return read_res.unwrap_error();
  }
  auto directories = read_res.unwrap();
  return std::accumulate(directories.begin(), directories.end(), std::string{},
                         [](std::string &str, u8 ch) {
                           return std::move(str) +
                                  static_cast<std::string::value_type>(ch);
                         });
}

/**
 * @brief Write raw string to the inode
 *
 * @param fs pointer to the underlying filesystem
 * @param id inode id
 * @param sv the string to write
 * @return ChfsNullResult
 */
ChfsNullResult write_raw_content(FileOperation *fs, inode_id_t id,
                                 std::string_view sv) {
  const auto sz = sv.length();
  std::vector<u8> buffer(sz);
  std::copy(sv.begin(), sv.end(), buffer.begin());
  return fs->write_file(id, buffer);
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {

  auto read_res = read_raw_content(fs, id);
  if (read_res.is_err()) {
    return read_res.unwrap_error();
  }
  auto directory_content = read_res.unwrap();
  parse_directory(directory_content, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;
  read_directory(this, id, list);
  auto found_it = std::find_if(
      list.begin(), list.end(),
      [=](const DirectoryEntry &entry) -> bool { return entry.name == name; });
  if (found_it == list.end()) {
    return ErrorType::NotExist;
  }
  return found_it->id;
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t /* parent id */ id, const char *name,
                              InodeType type) -> ChfsResult<inode_id_t> {

  return mk_helper_handler(id, name, [this, type]() -> ChfsResult<inode_id_t> {
    return this->alloc_inode(type);
  });
}

auto FileOperation::mk_helper_metadata_server(inode_id_t /* parent id */ id,
                                              const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  return mk_helper_handler(id, name, [this, type]() -> ChfsResult<inode_id_t> {
    return this->alloc_metadata_server_inode(type);
  });
}

auto FileOperation::mk_helper_handler(
    inode_id_t parent, const char *name,
    std::function<ChfsResult<inode_id_t>()> alloc_node)
    -> ChfsResult<inode_id_t> {
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  if (this->lookup(parent, name).is_ok()) {
    return ErrorType::AlreadyExist;
  }
  // 2. Create the new inode.
  auto alloc_res = alloc_node();

  if (alloc_res.is_err()) {
    return alloc_res.unwrap_error();
  }
  auto inode_id = alloc_res.unwrap();
  // 3. Append the new entry to the parent directory.
  auto read_res = read_raw_content(this, parent);
  if (read_res.is_err()) {
    return read_res.unwrap_error();
  }
  auto dir_content = read_res.unwrap();
  auto dir_content_new = append_to_directory(dir_content, name, inode_id);
  if (auto res = write_raw_content(this, parent, dir_content_new);
      res.is_err()) {
    return res.unwrap_error();
  }
  return inode_id;
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // 1. Remove the file, you can use the function `remove_file`
  const auto lookup_res = this->lookup(parent, name);
  if (lookup_res.is_err()) {
    return ErrorType::NotExist;
  }
  const auto inode_id = lookup_res.unwrap();
  if (auto res = this->remove_file(inode_id); res.is_err()) {
    return res.unwrap_error();
  }
  // 2. Remove the entry from the directory.
  auto read_res = read_raw_content(this, parent);
  if (read_res.is_err()) {
    return read_res.unwrap_error();
  }
  auto dir_content = read_res.unwrap();
  auto dir_content_new = rm_from_directory(dir_content, name);
  if (auto res = write_raw_content(this, parent, dir_content_new);
      res.is_err()) {
    return res.unwrap_error();
  }

  return KNullOk;
}

} // namespace chfs
