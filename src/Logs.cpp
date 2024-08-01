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

void printChart(std::ostream&os,
                std::vector<std::string>const & columnNames,
                std::vector<std::vector<std::string>>const & rows)
{
  /*
   |---------|---------|
   | column0 | column1 |
   |---------|---------|
   | 3       |  hello  |
   | 4       |  world  |
   |---------|---------|
   */

  const auto countColumns = columnNames.size();
  std::vector<size_t> columnWidth(countColumns, 0);
  for(size_t i=0; i<countColumns; ++i)
  {
    columnWidth[i] = std::max(columnWidth[i], columnNames[i].size());
    for(const auto & values : rows)
      columnWidth[i] = std::max(columnWidth[i], values[i].size());

    columnWidth[i]  += 2;
  }
  
  std::string separatorLine{"|"};
  for(const auto w : columnWidth)
  {
    separatorLine += std::string(w, '-');
    separatorLine += "|";
  }
  auto printValues = [&](std::vector<std::string>const & values)
  {
    if(values.size() != countColumns)
      throw std::logic_error("mismatch between values and columns");
    os << "|";
    for(size_t i=0; i<countColumns; ++i)
    {
      const auto countSpaces = columnWidth[i] - values[i].size();
      if(countSpaces)
        os << std::string(countSpaces, ' ');
      os << values[i];
      os << "|";
    }
    os << std::endl;
  };
  os << separatorLine << std::endl;
  printValues(columnNames);
  os << separatorLine << std::endl;
  for(const auto & values : rows)
    printValues(values);
  os << separatorLine << std::endl;
}
