#pragma once


#include "CypherAST.h"


template<typename Index>
struct IndexedTypes
{
  std::optional<Index> getIfExists(std::string const & type) const
  {
    auto it = typeToIndex.find(type);
    if(it == typeToIndex.end())
      return {};
    return it->second;
  }
  const std::string* getIfExists(Index const type) const
  {
    auto it = indexToType.find(type);
    if(it == indexToType.end())
      return {};
    return &it->second;
  }

  void add(Index idx, std::string const & name)
  {
    auto it = typeToIndex.find(name);
    if(it != typeToIndex.end())
      throw std::logic_error("duplicate type");
    typeToIndex[name] = idx;
    indexToType[idx] = name;
    m_maxIndex = std::max(idx, m_maxIndex.value_or(std::numeric_limits<Index>::lowest()));
  }

  const std::unordered_map<std::string, Index>& getTypeToIndex() const { return typeToIndex; }

  const std::optional<Index> getMaxIndex() const { return m_maxIndex; }
private:
  std::unordered_map<std::string, Index> typeToIndex;
  std::unordered_map<Index, std::string> indexToType;
  std::optional<Index> m_maxIndex;
};


enum class Element{
  Node,
  Relationship
};

struct ReturnClauseTerm
{
  // position of the term in the return clause.
  size_t returnClausePosition;

  // TODO support more later.
  openCypher::PropertyKeyName propertyName;
};

struct PathPatternElement
{
  PathPatternElement(const std::optional<openCypher::Variable>& var,
                     const std::vector<std::string>& labels)
  : var(var)
  , labels(labels)
  {}

  std::optional<openCypher::Variable> var;

  // label constraints are AND-ed
  std::vector<std::string> labels;
};

enum class Overwrite{Yes, No};


// Contains information to order results in the same order as they were specified in the return clause.
using ResultOrder = std::vector<std::pair<
unsigned /* i = index into VecValues, VecColumnNames*/,
unsigned /* j = index into *VecValues[i], *VecColumnNames[i] */>>;

using VecColumnNames = std::vector<const std::vector<openCypher::PropertyKeyName>*>;
using VecValues = std::vector<const std::vector<Value>*>;

using FuncResults = std::function<void(const ResultOrder&,
                                       const std::vector<openCypher::Variable>&,
                                       const VecColumnNames&,
                                       const VecValues&)>;

using FuncOnSQLQuery = std::function<void(std::string const & sqlQuery)>;
using FuncOnSQLQueryDuration = std::function<void(const std::chrono::steady_clock::duration)>;
using FuncOnDBDiagnosticContent = std::function<void(int argc,
                                                     Value *argv,
                                                     char **column)>;

inline constexpr const char* c_defaultDBPath{"default.sqlite3db"};
