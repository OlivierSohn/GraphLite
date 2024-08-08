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
