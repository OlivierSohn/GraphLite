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
