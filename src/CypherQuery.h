#pragma once

#include "CypherAST.h"
#include "GraphDBSqlite.h"

#include <string>

namespace openCypher
{
namespace detail
{
SingleQuery cypherQueryToAST(const std::string& idProperty, const std::string& query);

using FOnOrderAndColumnNames = std::function<void(const GraphDB::ResultOrder&,
                                                  const std::vector<std::string>& /* variable names */,
                                                  const GraphDB::VecColumnNames&)>;

using FOnRow = std::function<void(const GraphDB::VecValues&)>;

//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
void runSingleQuery(const SingleQuery& q, GraphDB& db, const FOnOrderAndColumnNames& fOnOrderAndColumnNames, const FOnRow& fOnRow);
}


template<typename ResultsHander>
void runCypher(const std::string& cypherQuery, GraphDB&db)
{
  using detail::cypherQueryToAST;
  using detail::runSingleQuery;

  const auto ast = cypherQueryToAST(db.idProperty(), cypherQuery);
  
  ResultsHander resultsHandler(cypherQuery);
  
  runSingleQuery(ast, db,
                 [&](const GraphDB::ResultOrder& ro, const std::vector<std::string>& varNames, const GraphDB::VecColumnNames& colNames)
                 { resultsHandler.onOrderAndColumnNames(ro, varNames, colNames); },
                 [&](const GraphDB::VecValues& values)
                 { resultsHandler.onRow(values); });
}
} // NS
