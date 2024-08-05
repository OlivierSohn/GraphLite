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
#include "GraphDBSqlite.h"

#include <string>

namespace openCypher
{
namespace detail
{
SingleQuery cypherQueryToAST(const PropertySchema& idProperty,
                             const std::string& query,
                             const std::map<SymbolicName, HomogeneousNonNullableValues>& queryParams,
                             bool printCypherAST);

using FOnOrderAndColumnNames = std::function<void(const ResultOrder&,
                                                  const std::vector<Variable>&,
                                                  const VecColumnNames&)>;

using FOnRow = std::function<void(const VecValues&)>;

//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
template<typename ID>
void runSingleQuery(const SingleQuery& q,
                    GraphDB<ID>& db,
                    const FOnOrderAndColumnNames& fOnOrderAndColumnNames,
                    const FOnRow& fOnRow);
}


template<typename ID, typename ResultsHander>
void runCypher(const std::string& cypherQuery,
               const std::map<SymbolicName, HomogeneousNonNullableValues>& queryParams,
               GraphDB<ID>&db,
               ResultsHander& resultsHandler)
{
  using detail::cypherQueryToAST;
  using detail::runSingleQuery;

  const auto ast = cypherQueryToAST(db.idProperty(), cypherQuery, queryParams, resultsHandler.printCypherAST());
  
  resultsHandler.onCypherQueryStarts(cypherQuery);
  struct Scope
  {
    ~Scope(){
      m_resultsHandler.onCypherQueryEnds();
    }
    ResultsHander& m_resultsHandler;
  } scope{resultsHandler};

  runSingleQuery(ast, db,
                 [&](const ResultOrder& ro, const std::vector<Variable>& varNames, const VecColumnNames& colNames)
                 { resultsHandler.onOrderAndColumnNames(ro, varNames, colNames); },
                 [&](const VecValues& values)
                 { resultsHandler.onRow(values); });
}
} // NS

#include "CypherQuery.inl"
