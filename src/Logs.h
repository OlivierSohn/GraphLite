#pragma once

#include <ostream>

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
LogIndentScope logScope(std::ostream& os, const char * scopeName);


//
// String utility functions
//

// splits |req| in one or more parts.
//
// splitOn('test', '12test45test67') returns '12', 'test', '45', 'test', '67'.
std::vector<std::string> splitOn(const std::string& match, std::string const & req);
