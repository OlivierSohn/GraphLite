#pragma once


#include "CypherAST.h"


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
                     const openCypher::Labels& labels)
  : var(var)
  , labels(labels)
  {}

  std::optional<openCypher::Variable> var;

  openCypher::Labels labels;
};

enum class Overwrite{Yes, No};


// Contains information to order results in the same order as they were specified in the return clause.
using ResultOrder = std::vector<std::pair<
unsigned /* i = index into VecValues*/,
unsigned /* j = index into *VecValues[i]*/>>;

using VecValues = std::vector<const std::vector<Value>*>;

using FuncColumns = std::function<void(const std::vector<std::string>&)>;

using FuncResults = std::function<void(const ResultOrder&, const VecValues&)>;

using FuncOnSQLQuery = std::function<void(std::string const & sqlQuery)>;
using FuncOnSQLQueryDuration = std::function<void(const std::chrono::steady_clock::duration)>;
using FuncOnDBDiagnosticContent = std::function<void(int argc,
                                                     Value *argv,
                                                     char **column)>;

inline constexpr const char* c_defaultDBPath{"default.sqlite3db"};
