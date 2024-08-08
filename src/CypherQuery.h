#pragma once

#include "CypherAST.h"
#include "GraphDBSqlite.h"

#include <string>

namespace openCypher
{
namespace detail
{
RegularQuery cypherQueryToAST(const PropertySchema& idProperty,
                              const std::string& query,
                              const std::map<ParameterName, HomogeneousNonNullableValues>& queryParams,
                              bool printCypherAST);

using FOnColumns = std::function<void(const std::vector<std::string>&)>;
using FOnRowOrder = std::function<void(const ResultOrder&)>;
using FOnRow = std::function<void(const VecValues&)>;

//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
template<typename ID>
void runSingleQuery(const RegularQuery& q,
                    GraphDB<ID>& db,
                    const FOnColumns& fOnColumns,
                    const FOnRowOrder& fOnRowOrder,
                    const FOnRow& fOnRow);
}


template<typename ID, typename ResultsHander>
void runCypher(const std::string& cypherQuery,
               const std::map<ParameterName, HomogeneousNonNullableValues>& queryParams,
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
                 [&](const std::vector<std::string>& colNames)
                 { resultsHandler.onColumns(colNames); },
                 [&](const ResultOrder& ro)
                 { resultsHandler.onOrder(ro); },
                 [&](const VecValues& values)
                 { resultsHandler.onRow(values); });
}
} // NS

#include "CypherQuery.inl"
