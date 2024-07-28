#pragma once

#include "CypherAST.h"
#include "DBSqlite.h"

#include <string>

namespace openCypher
{
namespace detail
{
SingleQuery cypherQueryToAST(const std::string& idProperty, const std::string& query);

using FOnOrderAndColumnNames = std::function<void(const DB::ResultOrder&,
                                                  const std::vector<std::string>& /* variable names */,
                                                  const DB::VecColumnNames&)>;

using FOnRow = std::function<void(const DB::VecValues&)>;

//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
void runSingleQuery(const SingleQuery& q, DB& db, const FOnOrderAndColumnNames& fOnOrderAndColumnNames, const FOnRow& fOnRow);
}


template<typename ResultsHander>
void runCypher(const std::string& cypherQuery, DB&db)
{
  using detail::cypherQueryToAST;
  using detail::runSingleQuery;

  const auto ast = cypherQueryToAST(db.idProperty(), cypherQuery);
  
  ResultsHander resultsHandler(cypherQuery);
  
  runSingleQuery(ast, db,
                 [&](const DB::ResultOrder& ro, const std::vector<std::string>& varNames, const DB::VecColumnNames& colNames)
                 { resultsHandler.onOrderAndColumnNames(ro, varNames, colNames); },
                 [&](const DB::VecValues& values)
                 { resultsHandler.onRow(values); });
}
} // NS
