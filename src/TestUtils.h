#pragma once

#include <chrono>
#include <random>
#include <filesystem>
#include <type_traits>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"


inline constexpr auto
operator == ( const Nothing& a, const Nothing& b )
{
  return true;
}
inline constexpr auto
operator < ( const Nothing& a, const Nothing& b )
{
  return false;
}

inline constexpr auto
operator < ( const StringPtr& a, const StringPtr& b )
{
  if(static_cast<bool>(a.string) < static_cast<bool>(b.string))
    return true;
  if(static_cast<bool>(a.string) > static_cast<bool>(b.string))
    return false;
  if(a.string)
  {
    if(a.m_bufSz < b.m_bufSz)
      return true;
    if(a.m_bufSz > b.m_bufSz)
      return false;
    for(size_t i{}; i<a.m_bufSz; ++i)
    {
      if(a.string[i] < b.string[i])
        return true;
      if(a.string[i] > b.string[i])
        return false;
    }
  }
  // a == b
  return false;
}

inline constexpr auto
operator < ( const ByteArrayPtr& a, const ByteArrayPtr& b )
{
  if(static_cast<bool>(a.bytes) < static_cast<bool>(b.bytes))
    return true;
  if(static_cast<bool>(a.bytes) > static_cast<bool>(b.bytes))
    return false;
  if(a.bytes)
  {
    if(a.m_bufSz < b.m_bufSz)
      return true;
    if(a.m_bufSz > b.m_bufSz)
      return false;
    for(size_t i{}; i<a.m_bufSz; ++i)
    {
      if(a.bytes[i] < b.bytes[i])
        return true;
      if(a.bytes[i] > b.bytes[i])
        return false;
    }
  }
  // a == b
  return false;
}


inline auto
operator == ( const Value& v, const ByteArrayPtr& b )
{
  return v == Value{b.clone()};
}

inline auto
operator == ( const Value& v, const StringPtr& s )
{
  return v == Value{s.clone()};
}

inline auto
operator == ( const Value& v, const Nothing n )
{
  return v == Value{n};
}

inline auto
operator < ( const Value& v, const Nothing n )
{
  return v < Value{n};
}

inline auto
operator == ( const Value& v, const int64_t i )
{
  return v == Value{i};
}

inline auto
operator < ( const Value& v, const int64_t i )
{
  return v < Value{i};
}

inline auto
operator == ( const Value& v, const double d)
{
  return v == Value{d};
}

inline auto
operator < ( const Value& v, const double d)
{
  return v < Value{d};
}

inline auto
operator == (const Value& v, const char* str)
{
  return v == Value{StringPtr::fromCStr(str)};
}



namespace openCypher::test
{

template<typename T, typename U = std::enable_if_t<isVariantMember<T, Value>::value>>
std::set<std::vector<Value>> toValues(std::set<std::vector<T>> && s)
{
  std::set<std::vector<Value>> res;
  for(const auto & v : s)
  {
    std::vector<Value> vv;
    for(const auto & e : v)
      vv.push_back(Value(std::move(e)));
    res.insert(std::move(vv));
  }
  return res;
}

template<typename T, typename U = std::enable_if_t<isVariantMember<T, Value>::value>>
std::set<Value> mkSet(std::initializer_list<T>&& values)
{
  std::set<Value> res;
  for(const auto & i : values)
    res.emplace(i);
  return res;
}

std::set<Value> mkSet(std::initializer_list<std::reference_wrapper<const Value>>&& values);

std::set<std::vector<Value>> toSet(const std::vector<std::vector<Value>>& values);


struct SQLQueryStat{
  std::string query;
  std::chrono::steady_clock::duration duration;
};

template<typename ID = int64_t>
struct GraphWithStats
{
  GraphWithStats(const std::optional<std::filesystem::path>& dbPath = std::nullopt, std::optional<Overwrite> overwrite = std::nullopt);
  
  GraphDB<ID>& getDB() { return *m_graph; }

  bool m_printSQLRequests{false};
  bool m_printSQLRequestsDuration{false};

  std::vector<SQLQueryStat> m_queryStats;
private:
  std::unique_ptr<GraphDB<ID>> m_graph;
};


template<typename ID = int64_t>
struct QueryResultsHandler
{
  QueryResultsHandler(GraphWithStats<ID>& db)
  : m_db(db)
  {}
  
  // Runs the openCypher query |cypherQuery| with optional parameters |Params|.
  void run(const std::string &cypherQuery,
           const std::map<SymbolicName, HomogeneousNonNullableValues>& Params = {});

  bool printCypherAST() const { return m_printCypherAST; }
    
  void onCypherQueryStarts(std::string const & cypherQuery);

  void onOrderAndColumnNames(const ResultOrder& ro,
                             const std::vector<openCypher::Variable>& vars,
                             const VecColumnNames& colNames);
  
  void onRow(const VecValues& values);
  
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
  ResultOrder m_resultOrder;
  std::vector<openCypher::Variable> m_variables;
  VecColumnNames m_columnNames;
  
  GraphWithStats<ID>& m_db;
  std::vector<std::vector<Value>> m_rows;
};

}

#include "TestUtils.inl"
