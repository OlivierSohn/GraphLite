#include "Logs.h"

namespace
{

int& logIndent()
{
  thread_local int indent{};
  return indent;
}

}

std::ostream& operator <<(std::ostream& os, LogIndent _)
{
  const int countSpaces = logIndent() * 2;
  os << std::string(countSpaces, ' ');
  return os;
}

LogIndentScope::LogIndentScope()
{
  ++logIndent();
}

LogIndentScope::~LogIndentScope()
{
  endScope();
}
void LogIndentScope::endScope()
{
  if(m_active)
  {
    --logIndent();
    m_active = false;
  }
}

LogIndentScope logScope(std::ostream& os, const std::string& scopeName)
{
  os << LogIndent{} << scopeName << std::endl;
  return LogIndentScope();
}


//
// String utility functions
//

std::vector<std::string> splitOn(const std::string& match, std::string const & req)
{
  std::vector<std::string> parts;
  auto dist = [](size_t pos, size_t i)
  {
    if(i == std::string::npos)
      return std::string::npos;
    return i - pos;
  };
  size_t pos{};
  do{
    auto i = req.find(match, pos);
    parts.push_back(req.substr(pos, dist(pos, i)));
    pos = i;
    if(pos != std::string::npos)
    {
      i += match.size();
      parts.push_back(req.substr(pos, dist(pos, i)));
      pos = i;
    }
  }
  while(pos != std::string::npos);
  return parts;
}
