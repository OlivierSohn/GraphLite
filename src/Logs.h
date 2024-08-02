#pragma once

#include <ostream>
#include <string>
#include <charconv>

// RAII-object to indent logs in a scope.
struct LogIndentScope
{
  LogIndentScope();
  ~LogIndentScope();

  void endScope();
private:
  bool m_active {true};
};

struct LogIndent{};
// When |LogIndent| is inserted in a stream, spaces are inserted to represent the current scope indent.
std::ostream& operator <<(std::ostream& os, LogIndent _);


[[nodiscard]]
LogIndentScope logScope(std::ostream& os, const std::string& scopeName);


//
// String utility functions
//

// splits |req| in one or more parts.
//
// splitOn('test', '12test45test67') returns '12', 'test', '45', 'test', '67'.
std::vector<std::string> splitOn(const std::string& match, std::string const & req);

void printChart(std::ostream&,
                std::vector<std::string> const & columnNames,
                std::vector<std::vector<std::string>> const & values);

inline int64_t strToInt64(std::string const & str)
{
  int64_t result{};
  const auto last = str.data() + str.size();
  auto [ptr, ec] = std::from_chars(str.data(), last, result);
  
  if (ec == std::errc())
  {
    if(ptr == last)
      return result;
    else
      throw std::logic_error("Found invalid int64 string:'" + str + "'");
  }
  else if (ec == std::errc::invalid_argument)
    throw std::logic_error("Not an int64 string:'" + str + "'");
  else if (ec == std::errc::result_out_of_range)
    throw std::logic_error("Number is larger than int64 :'" + str + "'");
}

