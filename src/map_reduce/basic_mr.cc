#include <algorithm>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <vector>

#include "map_reduce/protocol.h"

std::vector<std::string> split(const std::string &s) {

  // Define a regular expression pattern to match words
  std::regex wordRegex("[a-zA-Z]+");

  // Use std::sregex_token_iterator to iterate over matching words
  std::sregex_token_iterator iter(begin(s), end(s), wordRegex, 0);
  std::sregex_token_iterator end{};

  std::vector<std::string> words(iter, end);

  return words;
}

namespace mapReduce {
//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
std::vector<KeyVal> Map(const std::string &content) {
  // Your code goes here
  // Hints: split contents into an array of words.
  std::vector<KeyVal> ret{};
  auto split_words = split(content);

  transform(begin(split_words), end(split_words), back_inserter(ret),
            [](const std::string &word) -> KeyVal {
              return KeyVal{word, "1"};
            });

  return ret;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
std::string Reduce(const std::string &key,
                   const std::vector<std::string> &values) {
  // Your code goes here
  // Hints: return the number of occurrences of the word.
  return std::to_string(accumulate(
      begin(values), end(values), static_cast<std::size_t>(0),
      [](std::size_t count, const std::string &value) -> std::size_t {
        return count + std::stoul(value);
      }));
}
} // namespace mapReduce