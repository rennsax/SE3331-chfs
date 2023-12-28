#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
template <typename Iterator, typename Func, typename BinaryPredicate>
void group_and_call(Iterator begin, Iterator end, Func callback,
                    BinaryPredicate p) {
  if (begin == end) {
    return;
  }
  Iterator group_begin = begin;
  Iterator group_end = begin;

  while (group_begin != end) {
    while (group_end != end && p(*group_begin, *group_end)) {
      ++group_end;
    }
    callback(group_begin, group_end);
    group_begin = group_end;
  }
}
template <typename Iterator, typename Func>
void group_and_call(Iterator begin, Iterator end, Func callback) {
  group_and_call(begin, end, callback, std::equal_to<>());
}

SequentialMapReduce::SequentialMapReduce(
    std::shared_ptr<chfs::ChfsClient> client,
    const std::vector<std::string> &files_, std::string resultFile)
    : chfs_client(std::move(client)), files(files_), outPutFile(resultFile) {

  // Your code goes here (optional)
}

void SequentialMapReduce::doWork() {
  // Your code goes here
  std::stringstream input_ss{};
  for (const auto &file : files) {
    auto res_lookup = chfs_client->lookup(1, file);
    auto inode_id = res_lookup.unwrap();
    auto res_type = chfs_client->get_type_attr(inode_id);
    auto length = res_type.unwrap().second.size;
    auto res_read = chfs_client->read_file(inode_id, 0, length);
    auto char_vec = res_read.unwrap();
    std::string content(char_vec.begin(), char_vec.end());
    input_ss << content << ' ';
  }
  auto intermediate_kvs = Map(input_ss.str());
  sort(begin(intermediate_kvs), end(intermediate_kvs),
       [](const KeyVal &kv1, const KeyVal &kv2) -> bool {
         // std::string::compare returns 0 iff the two strings equal
         return kv1.key < kv2.key;
       });
  using IterType = decltype(intermediate_kvs)::iterator;
  std::stringstream output_ss{};

  group_and_call(
      begin(intermediate_kvs), end(intermediate_kvs),
      [&](IterType begin, IterType end) -> void {
        auto key = begin->key;
        std::vector<std::string> values{};
        transform(begin, end, back_inserter(values),
                  [](const KeyVal &kv) -> std::string { return kv.val; });
        auto reduced_val = Reduce(key, values);
        output_ss << key << " " << reduced_val << "\n";
      },
      [](const KeyVal &kv1, const KeyVal &kv2) -> bool {
        return kv1.key == kv2.key;
      });
  {
    auto res_lookup = chfs_client->lookup(1, outPutFile);
    auto inode_id = res_lookup.unwrap();
    std::string output_content = output_ss.str();
    std::vector<chfs::u8> output_bytes(begin(output_content),
                                       end(output_content));

    chfs_client->write_file(inode_id, 0, std::move(output_bytes));
  }
}
} // namespace mapReduce