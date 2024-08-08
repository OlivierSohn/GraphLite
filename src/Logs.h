/*
 Copyright 2024-present Olivier Sohn
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

#pragma once

#include <ostream>
#include <string>
#include <charconv>
#include <chrono>

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

std::string toLower(std::string const & str);

// splits |req| in one or more parts.
//
// splitOn('test', '12test45test67') returns '12', 'test', '45', 'test', '67'.
std::vector<std::string> splitOn(const std::string& match, std::string const & req);

/*
 If columnNames is not null:
 
 |---------|---------|
 | column0 | column1 |
 |---------|---------|
 | 3       |  hello  |
 | 4       |  world  |
 |---------|---------|
 
 If columnNames is null:
 
 |---------|---------|
 | 3       |  hello  |
 | 4       |  world  |
 |---------|---------|
 
 */
void printChart(std::ostream&,
                std::vector<std::string> const * columnNames,
                std::vector<std::vector<std::string>> const & values);

inline int64_t strToInt64(std::string const & str, int base = 10)
{
  int64_t result{};
  const auto last = str.data() + str.size();
  auto [ptr, ec] = std::from_chars(str.data(), last, result, base);
  
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

inline double strToDouble(std::string const & str)
{
  // Looks like this from_chars for floating point types is missing in the XCode c++ lib
  // so we replace the code below by:
  return std::stod(str);
  /*
  double result{};
  const auto last = str.data() + str.size();
  auto [ptr, ec] = std::from_chars(str.data(), last, result);
  
  if (ec == std::errc())
  {
    if(ptr == last)
      return result;
    else
      throw std::logic_error("Found invalid double string:'" + str + "'");
  }
  else if (ec == std::errc::invalid_argument)
    throw std::logic_error("Not an double string:'" + str + "'");
  else if (ec == std::errc::result_out_of_range)
    throw std::logic_error("Number is outside double :'" + str + "'");
   */
}

struct Timer
{
  Timer(std::ostream& os)
  : m_os(os)
  {
    m_start = std::chrono::steady_clock::now();
  }
  void endStep(const std::string& title)
  {
    const auto dt = std::chrono::steady_clock::now() - m_start;
    m_start = std::chrono::steady_clock::now();
    auto & values = m_rows.emplace_back();
    values.push_back(title);
    values.push_back(std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(dt).count()) + " ms");
    m_os << "[" << values[0] << "] " << values[1] << std::endl;
  }
  
  ~Timer()
  {
    std::vector<std::string> columns{"Step", "Duration"};
    printChart(m_os, &columns, m_rows);
  }
private:
  std::chrono::steady_clock::time_point m_start;
  std::ostream& m_os;
  std::vector<std::vector<std::string>> m_rows;
};
