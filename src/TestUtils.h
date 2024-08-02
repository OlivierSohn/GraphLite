#pragma once

#include <chrono>
#include <random>
#include <filesystem>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"

namespace openCypher::test
{
struct SQLQueryStat{
  std::string query;
  std::chrono::steady_clock::duration duration;
};

struct GraphWithStats
{
  GraphWithStats(const std::optional<std::filesystem::path>& dbPath = std::nullopt);
  
  GraphDB& getDB() { return *m_graph; }

  bool m_printSQLRequests{false};
  bool m_printSQLRequestsDuration{false};

  std::vector<SQLQueryStat> m_queryStats;
private:
  std::unique_ptr<GraphDB> m_graph;

};


struct QueryResultsHandler
{
  QueryResultsHandler(GraphWithStats& db)
  : m_db(db)
  {}
  
  // Runs the openCypher query |cypherQuery| with optional parameters |Params|.
  void run(const std::string &cypherQuery,
           const std::map<SymbolicName, std::vector<std::string>>& Params = {});

  bool printCypherAST() const { return m_printCypherAST; }
    
  void onCypherQueryStarts(std::string const & cypherQuery);

  void onOrderAndColumnNames(const GraphDB::ResultOrder& ro,
                             const std::vector<openCypher::Variable>& vars,
                             const GraphDB::VecColumnNames& colNames);
  
  void onRow(const GraphDB::VecValues& values);
  
  void onCypherQueryEnds();
  
  size_t countRows() const { return m_rows.size(); }
  const auto& rows() const { return m_rows; }

  size_t countColumns() const { return m_resultOrder.size(); }

  size_t countSQLQueries() const { return m_db.m_queryStats.size(); }

  bool m_printCypherAST{ false };
  bool m_printCypherQueryText{ false };
  bool m_printCypherRows{ false };
  
  // Time to convert the Cypher query string to an AST
  std::chrono::steady_clock::duration m_cypherToASTDuration{};

  // Time to execute the Cypher query
  std::chrono::steady_clock::duration m_cypherQueryDuration{};

  // Time to execute the SQL queries (generated for the Cypher queries)
  // This includes m_sqlRelCbDuration below
  std::chrono::steady_clock::duration m_sqlQueriesExecutionDuration{};

  // Time spent in the SQL query result callbacks (querying the system relationships table)
  std::chrono::steady_clock::duration m_sqlRelCbDuration{};
  // Time spent in the SQL query result callbacks (querying the labeled node/relationship property tables)
  std::chrono::steady_clock::duration m_sqlPropCbDuration{};

private:
  std::chrono::steady_clock::time_point m_tCallRunCypher{};

  std::unique_ptr<LogIndentScope> m_logIndentScope;
  GraphDB::ResultOrder m_resultOrder;
  std::vector<openCypher::Variable> m_variables;
  GraphDB::VecColumnNames m_columnNames;
  
  GraphWithStats& m_db;
  std::vector<std::vector<std::optional<std::string>>> m_rows;
};

}
