#pragma once

#include "CypherAST.h"
#include "GraphDBSqlite.h"

#include <string>

namespace openCypher
{
namespace detail
{
SingleQuery cypherQueryToAST(const PropertyKeyName& idProperty,
                             const std::string& query,
                             const std::map<SymbolicName, std::vector<std::string>>& queryParams,
                             bool printCypherAST);

using FOnOrderAndColumnNames = std::function<void(const GraphDB::ResultOrder&,
                                                  const std::vector<Variable>&,
                                                  const GraphDB::VecColumnNames&)>;

using FOnRow = std::function<void(const GraphDB::VecValues&)>;

//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
void runSingleQuery(const SingleQuery& q, GraphDB& db, const FOnOrderAndColumnNames& fOnOrderAndColumnNames, const FOnRow& fOnRow);
}


template<typename ResultsHander>
void runCypher(const std::string& cypherQuery,
               const std::map<SymbolicName, std::vector<std::string>>& queryParams,
               GraphDB&db, ResultsHander& resultsHandler)
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
                 [&](const GraphDB::ResultOrder& ro, const std::vector<Variable>& varNames, const GraphDB::VecColumnNames& colNames)
                 { resultsHandler.onOrderAndColumnNames(ro, varNames, colNames); },
                 [&](const GraphDB::VecValues& values)
                 { resultsHandler.onRow(values); });
}
} // NS
