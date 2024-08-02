#include <gtest/gtest.h>
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

  size_t totalSQLQueriesCount() const { return m_queryStats.size(); }

  bool m_printSQLRequests{false};
  bool m_printSQLRequestsDuration{false};

  std::vector<SQLQueryStat> m_queryStats;
private:
  std::unique_ptr<GraphDB> m_graph;

};

GraphWithStats::GraphWithStats(const std::optional<std::filesystem::path>& dbPath)
{
  auto onSQLQuery = [&](const std::string& req)
  {
    m_queryStats.push_back(SQLQueryStat{req});
    if(m_printSQLRequests)
    {
      //auto req = reqLarge.substr(0, 700);
      bool first = true;
      for(const auto & part1 : splitOn("UNION ALL ", req))
        for(const auto & part : splitOn("INNER JOIN ", part1))
        {
          std::cout << LogIndent{};
          if(first)
          {
            first = false;
            std::cout << "[SQL] ";
          }
          else
            std::cout << "      ";
          std::cout << part << std::endl;
        }
    }
  };
  auto onSQLQueryDuration = [&](const std::chrono::steady_clock::duration d)
  {
    m_queryStats.back().duration = d;
    if(m_printSQLRequestsDuration)
    {
      std::cout << std::chrono::duration_cast<std::chrono::microseconds>(d).count() << " us" << std::endl;
    }
  };
  auto onDBDiagnosticContent = [&](int argc, char **argv, char **column)
  {
    if(m_printSQLRequests)
    {
      auto _ = LogIndentScope{};
      std::cout << LogIndent{};
      for (int i=0; i < argc; i++)
        printf("%s,\t", argv[i]);
      printf("\n");
    }
    return 0;
  };

  m_graph = std::make_unique<GraphDB>(onSQLQuery, onSQLQueryDuration, onDBDiagnosticContent, dbPath);
}


struct QueryResultsHandler
{
  QueryResultsHandler(GraphWithStats& db)
  : m_db(db)
  {}
  
  void run(const std::string &cypherQuery, const std::map<SymbolicName, std::vector<std::string>>& Params = {})
  {
    m_db.m_queryStats.clear();

    auto sqlDuration1 = m_db.getDB().m_totalSQLQueryExecutionDuration;
    auto relCbDuration1 = m_db.getDB().m_totalSystemRelationshipCbDuration;
    auto propCbDuration1 = m_db.getDB().m_totalPropertyTablesCbDuration;

    m_tCallRunCypher = std::chrono::steady_clock::now();
    
    openCypher::runCypher(cypherQuery, Params, m_db.getDB(), *this);

    m_cypherQueryDuration = (std::chrono::steady_clock::now() - m_tCallRunCypher) - m_cypherToASTDuration;

    auto sqlDuration2 = m_db.getDB().m_totalSQLQueryExecutionDuration;
    auto relCbDuration2 = m_db.getDB().m_totalSystemRelationshipCbDuration;
    auto propCbDuration2 = m_db.getDB().m_totalPropertyTablesCbDuration;

    m_sqlQueriesExecutionDuration = sqlDuration2 - sqlDuration1;
    m_sqlRelCbDuration = relCbDuration2 - relCbDuration1;
    m_sqlPropCbDuration = propCbDuration2 - propCbDuration1;
  }
  
  bool printCypherAST() const { return m_printCypherAST; }
  
  bool m_printCypherQueryText{ false };
  bool m_printCypherRows{ false };
  
  void onCypherQueryStarts(std::string const & cypherQuery)
  {
    m_cypherToASTDuration = std::chrono::steady_clock::now() - m_tCallRunCypher;

    m_rows.clear();
    
    if(m_printCypherQueryText)
    {
      std::cout << std::endl;
      std::cout << "[openCypher] " << cypherQuery << std::endl;
      m_logIndentScope = std::make_unique<LogIndentScope>();
    }
  }
  void onOrderAndColumnNames(const GraphDB::ResultOrder& ro, const std::vector<openCypher::Variable>& vars, const GraphDB::VecColumnNames& colNames) {
    m_resultOrder = ro;
    m_variables = vars;
    m_columnNames = colNames;
  }
  
  void onRow(const GraphDB::VecValues& values)
  {
    if(m_printCypherRows)
    {
      auto _ = LogIndentScope();
      std::cout << LogIndent{};
      for(const auto & [i, j] : m_resultOrder)
        std::cout << m_variables[i] << "." << (*m_columnNames[i])[j] << " = " << (*values[i])[j].value_or("<null>") << '|';
      std::cout << std::endl;
    }
    auto & row = m_rows.emplace_back();
    row.reserve(m_resultOrder.size());
    for(const auto & [i, j] : m_resultOrder)
      row.push_back((*values[i])[j]);
  }
  
  void onCypherQueryEnds()
  {
    m_logIndentScope.release();
  }
  
  size_t countRows() const { return m_rows.size(); }
  const auto& rows() const { return m_rows; }

  size_t countColumns() const { return m_resultOrder.size(); }

  size_t countSQLQueries() const { return m_db.totalSQLQueriesCount(); }

  bool m_printCypherAST{};
  
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

TEST(Test, EmptyDB)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(0, handler.countRows());

  handler.run("MATCH (n) RETURN n.propertyDoesNotExist");
  
  EXPECT_EQ(0, handler.countRows());
  
  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  
  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(0, handler.countRows());
}

TEST(Test, SingleEntity)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();

  auto & db = dbWrapper->getDB();

  db.addType("Entity", true, {});

  const std::string entityID = db.addNode("Entity", {});

  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (n) RETURN n.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(0, handler.countRows());

  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(0, handler.countRows());
}

TEST(Test, SingleRecursiveRelationship)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();

  auto & db = dbWrapper->getDB();

  db.addType("Entity", true, {});
  db.addType("Relationship", false, {});
  
  const std::string entityID = db.addNode("Entity", {});
  const std::string relationshipID = db.addRelationship("Relationship", entityID, entityID, {});
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)<-[r]-(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[R]-(B) RETURN id(a), id(R), id(B)");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  EXPECT_EQ(relationshipID, handler.rows()[1][1]);
  EXPECT_EQ(entityID, handler.rows()[1][0]);
  EXPECT_EQ(entityID, handler.rows()[1][2]);
  EXPECT_EQ(2, handler.countSQLQueries());  // TODO optimize

  handler.run("MATCH (a)-[r]->(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)<-[r]-(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[R]-(a) RETURN id(a), id(R)");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(relationshipID, handler.rows()[1][1]);
  EXPECT_EQ(entityID, handler.rows()[1][0]);
  EXPECT_EQ(2, handler.countSQLQueries()); // TODO optimize

  // id(a) <> id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) <> id(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  // id(a) = id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = id(b) RETURN id(a), id(r), id(b)");

  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());
}


TEST(Test, SingleNonRecursiveRelationship)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();

  auto & db = dbWrapper->getDB();
  db.addType("Entity", true, {});
  db.addType("Relationship", false, {});
  
  const std::string entityIDSource = db.addNode("Entity", {});
  const std::string entityIDDestination = db.addNode("Entity", {});
  const std::string relationshipID = db.addRelationship("Relationship", entityIDSource, entityIDDestination, {});
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(2, handler.countRows());
  const auto expected = std::set<std::optional<std::string>>{entityIDSource, entityIDDestination};
  {
    const auto actual = std::set<std::optional<std::string>>{handler.rows()[0][0], handler.rows()[1][0]};
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)<-[r]-(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][0]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[R]-(B) RETURN id(a), id(R), id(B)");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  {
    const auto actual = std::set<std::optional<std::string>>{handler.rows()[0][0], handler.rows()[0][2]};
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(relationshipID, handler.rows()[1][1]);
  {
    const auto actual = std::set<std::optional<std::string>>{handler.rows()[1][0], handler.rows()[1][2]};
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(2, handler.countSQLQueries());  // TODO optimize

  handler.run("MATCH (a)-[r]->(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)<-[r]-(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[R]-(a) RETURN id(a), id(R)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(2, handler.countSQLQueries());  // TODO optimize

  // id(a) <> id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) <> id(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());

  // id(a) = id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = id(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());
}

TEST(Test, NullProperties)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});

  const std::string entityIDSource = db.addNode("Person", {});
  const std::string entityIDDestination = db.addNode("Person", {});
  const std::string relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {});

  // querying some non-existing properties does require a SQL query on the typed table
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) return n.doesNotExist");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(std::nullopt, handler.rows()[1][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (n) return n.age");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(std::nullopt, handler.rows()[1][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return r.doesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
}


TEST(Test, NonNullProperties)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  const std::string entityIDSource = db.addNode("Person", {{p_age, "5"}});
  const std::string entityIDDestination = db.addNode("Person", {{p_age, "10"}});
  const std::string relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {{p_since, "1234"}});
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) return n.age");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  std::set<std::optional<std::string>> expectedAges{"5", "10"};
  std::set<std::optional<std::string>> actualAges{handler.rows()[0][0], handler.rows()[1][0]};
  EXPECT_EQ(expectedAges, actualAges);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ("1234", handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (a)-[r]->(b) return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("5", handler.rows()[0][0]);
  EXPECT_EQ("10", handler.rows()[0][1]);
  EXPECT_EQ("1234", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (b)<-[r]-(a) return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("5", handler.rows()[0][0]);
  EXPECT_EQ("10", handler.rows()[0][1]);
  EXPECT_EQ("1234", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
}


TEST(Test, ReturnIDs)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  const std::string entityIDSource = db.addNode("Person", {{p_age, "5"}});
  const std::string entityIDDestination = db.addNode("Person", {{p_age, "10"}});
  const std::string relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {{p_since, "1234"}});
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) WHERE n.age > 5 return id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(entityIDDestination, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (n) WHERE n.age > 5 return id(n), id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(2, handler.countColumns());
  EXPECT_EQ(entityIDDestination, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (n) WHERE n.age > 5 return id(n), id(n), n.age, n.age");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(4, handler.countColumns());
  EXPECT_EQ(entityIDDestination, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ("10", handler.rows()[0][2]);
  EXPECT_EQ("10", handler.rows()[0][3]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]-() return id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return id(r), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(2, handler.countColumns());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return id(r), id(r), r.since, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(4, handler.countColumns());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ("1234", handler.rows()[0][2]);
  EXPECT_EQ("1234", handler.rows()[0][3]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (a)-[r]->(b) return id(a), id(b), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ(relationshipID, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (a)-[r]->(b) return id(a), id(b), id(r), id(r), id(r), id(b), id(a)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(7, handler.countColumns());
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ(relationshipID, handler.rows()[0][2]);
  EXPECT_EQ(relationshipID, handler.rows()[0][3]);
  EXPECT_EQ(relationshipID, handler.rows()[0][4]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][5]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][6]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (a)-[r]->(b) return id(a), id(b), id(r), id(r), id(r), id(b), id(a), r.since, r.since, a.age, b.age, a.age");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(12, handler.countColumns());
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ(relationshipID, handler.rows()[0][2]);
  EXPECT_EQ(relationshipID, handler.rows()[0][3]);
  EXPECT_EQ(relationshipID, handler.rows()[0][4]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][5]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][6]);
  EXPECT_EQ("1234", handler.rows()[0][7]);
  EXPECT_EQ("1234", handler.rows()[0][8]);
  EXPECT_EQ("5", handler.rows()[0][9]);
  EXPECT_EQ("10", handler.rows()[0][10]);
  EXPECT_EQ("5", handler.rows()[0][11]);
  EXPECT_EQ(4, handler.countSQLQueries());
  
  handler.run("MATCH (b)<-[r]-(a) return id(a), id(b), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ(relationshipID, handler.rows()[0][2]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (b)<-[r]-(a) return id(a), id(b), id(r), id(r), id(r), id(b), id(a)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(7, handler.countColumns());
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][1]);
  EXPECT_EQ(relationshipID, handler.rows()[0][2]);
  EXPECT_EQ(relationshipID, handler.rows()[0][3]);
  EXPECT_EQ(relationshipID, handler.rows()[0][4]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][5]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][6]);
  EXPECT_EQ(1, handler.countSQLQueries());
}

TEST(Test, WhereClauses)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});

  std::string entityIDSource5;
  std::string RelID;
  {
    entityIDSource5 = db.addNode("Person", {{p_age, "5"}});
    const std::string entityIDDestination = db.addNode("Person", {{p_age, "10"}});
    RelID = db.addRelationship("Knows", entityIDSource5, entityIDDestination, {{p_since, "1234"}});
  }
  {
    const std::string entityIDSource = db.addNode("Person", {{p_age, "105"}});
    const std::string entityIDDestination = db.addNode("Person", {{p_age, "110"}});
    const std::string relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {{p_since, "123456"}});
  }

  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) WHERE n.age < 107 return n.age");
  
  EXPECT_EQ(3, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  std::set<std::optional<std::string>> expectedAges{"5", "10", "105"};
  std::set<std::optional<std::string>> actualAges{handler.rows()[0][0], handler.rows()[1][0], handler.rows()[2][0]};
  EXPECT_EQ(expectedAges, actualAges);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() WHERE r.since > 12345 return r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ("123456", handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = id(b) return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) WHERE id(b) = " + entityIDSource5 + " return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = " + entityIDSource5 + " return a.age, b.age, r.since, id(a), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(5, handler.countColumns());
  EXPECT_EQ("5", handler.rows()[0][0]);
  EXPECT_EQ("10", handler.rows()[0][1]);
  EXPECT_EQ("1234", handler.rows()[0][2]);
  EXPECT_EQ(entityIDSource5, handler.rows()[0][3]);
  EXPECT_EQ(RelID, handler.rows()[0][4]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.

  handler.run("MATCH (a)-[r]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("105", handler.rows()[0][0]);
  EXPECT_EQ("110", handler.rows()[0][1]);
  EXPECT_EQ("123456", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (b)<-[r]-(a) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("105", handler.rows()[0][0]);
  EXPECT_EQ("110", handler.rows()[0][1]);
  EXPECT_EQ("123456", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  // not supported yet: "A non-equi-var expression is using non-id properties"
  EXPECT_THROW(handler.run("MATCH (b)<-[r]-(a) WHERE r.since > 12345 OR a.age < 107 return a.age, b.age, r.since"), std::logic_error);
}

TEST(Test, WhereClausesOptimized)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  /*

   A1   A2   A3
   |^   |^   |^
   v|   v|   v|
   B1   B2   B3

   */
  const auto p_propA = mkProperty("propA");
  const auto p_propB = mkProperty("propB");
  db.addType("EntityA", true, {p_propA});
  db.addType("EntityB", true, {p_propB});
  db.addType("RelAB", false, {p_propA});
  db.addType("RelBA", false, {p_propB});
  
  {
    const std::string entityA1 = db.addNode("EntityA", {{p_propA, "1"}});
    const std::string entityA2 = db.addNode("EntityA", {{p_propA, "2"}});
    const std::string entityA3 = db.addNode("EntityA", {{p_propA, "3"}});

    const std::string entityB1 = db.addNode("EntityB", {{p_propB, "1"}});
    const std::string entityB2 = db.addNode("EntityB", {{p_propB, "2"}});
    const std::string entityB3 = db.addNode("EntityB", {{p_propB, "3"}});

    db.addRelationship("RelAB", entityA1, entityB1, {{p_propA, "10"}});
    db.addRelationship("RelAB", entityA2, entityB2, {{p_propA, "20"}});
    db.addRelationship("RelAB", entityA3, entityB3, {{p_propA, "30"}});
    
    db.addRelationship("RelBA", entityB1, entityA1, {{p_propB, "10"}});
    db.addRelationship("RelBA", entityB2, entityA2, {{p_propB, "20"}});
    db.addRelationship("RelBA", entityB3, entityA3, {{p_propB, "30"}});
  }
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) WHERE n.propA <= 2 return n.propA");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  {
    std::set<std::optional<std::string>> expected{"1", "2"};
    std::set<std::optional<std::string>> actual{handler.rows()[0][0], handler.rows()[1][0]};
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (n)-[r]->() WHERE n.propA <= 2.5 AND n.propA >= 1.5 return n.propA, r.propA");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(2, handler.countColumns());
  EXPECT_EQ("2", handler.rows()[0][0]);
  EXPECT_EQ("20", handler.rows()[0][1]);
  EXPECT_EQ(3, handler.countSQLQueries()); // one for the system relationships table, one for EntityA table, one for RelAB table
  // The reason the table EntityB is not queried is because the where clause evaluates to false in this table (propA is not a field of this table)

  handler.run("MATCH (n)-[r]->() WHERE n.propA <= 2.5 AND r.propA >= 15 return n.propA, r.propA");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(2, handler.countColumns());
  EXPECT_EQ("2", handler.rows()[0][0]);
  EXPECT_EQ("20", handler.rows()[0][1]);
  EXPECT_EQ(3, handler.countSQLQueries()); // one for the system relationships table, one for EntityA table, one for RelAB table
  // The reason the table EntityB is not queried is because the where clause evaluates to false in this table (propA is not a field of this table)
}

TEST(Test, Labels)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  db.addType("WorksWith", false, {p_since});
  
  {
    const std::string entityIDSource = db.addNode("Person", {{p_age, "5"}});
    const std::string entityIDDestination = db.addNode("Person", {{p_age, "10"}});
    const std::string relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {{p_since, "1234"}});
    const std::string relationshipID2 = db.addRelationship("WorksWith", entityIDSource, entityIDDestination, {{p_since, "123444"}});
  }
  {
    const std::string entityIDSource = db.addNode("Person", {{p_age, "105"}});
    const std::string entityIDDestination = db.addNode("Person", {{p_age, "110"}});
    const std::string relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {{p_since, "123456"}});
    const std::string relationshipID2 = db.addRelationship("WorksWith", entityIDSource, entityIDDestination, {{p_since, "12345666"}});
  }
  
  QueryResultsHandler handler(*dbWrapper);
  
  // Non-existing label on relationship
  
  handler.run("MATCH (a)-[r:NotHere]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());
  
  // Non-existing label on source entity
  
  handler.run("MATCH (a:NotHere)-[r]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());
  
  // Non-existing label on destination entity
  
  handler.run("MATCH (a)-[r]->(b:NotHere) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());
  
  // Non-existing label on destination entity (with existing labels on others)
  
  handler.run("MATCH (a:Person)-[r:Knows]->(b:NotHere) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (a)-[r:Knows]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("105", handler.rows()[0][0]);
  EXPECT_EQ("110", handler.rows()[0][1]);
  EXPECT_EQ("123456", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (a:Person)-[r:Knows]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("105", handler.rows()[0][0]);
  EXPECT_EQ("110", handler.rows()[0][1]);
  EXPECT_EQ("123456", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (a:Person)-[r:Knows]->(b:Person) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("105", handler.rows()[0][0]);
  EXPECT_EQ("110", handler.rows()[0][1]);
  EXPECT_EQ("123456", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (a:Person)-[r:WorksWith]->(b:Person) WHERE r.since < 1234444 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ("5", handler.rows()[0][0]);
  EXPECT_EQ("10", handler.rows()[0][1]);
  EXPECT_EQ("123444", handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.

  handler.run("MATCH (a:Person)-[r]->(b) WHERE b.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
}

TEST(Test, PathForbidsRelationshipsRepetition)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  /*
   
   p1 -> p2
   ^      |
   -------
   */
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  std::string p1 = db.addNode("Person", {{p_age, "1"}});
  std::string p2 = db.addNode("Person", {{p_age, "2"}});
  std::string r12 = db.addRelationship("Knows", p1, p2, {{p_since, "12"}});
  std::string r21 = db.addRelationship("Knows", p2, p1, {{p_since, "21"}});
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  handler.run("MATCH (a)-->(b)-->(c)-->(d) return a.age, b.age, c.age, d.age");
  
  EXPECT_EQ(0, handler.countRows());
}

TEST(Test, PathAllowsNodesRepetition)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  /*
   
   p1 -> p2
   ^      |
   -------
   */
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  std::string p1 = db.addNode("Person", {{p_age, "1"}});
  std::string p2 = db.addNode("Person", {{p_age, "2"}});
  std::string r12 = db.addRelationship("Knows", p1, p2, {{p_since, "12"}});
  std::string r21 = db.addRelationship("Knows", p2, p1, {{p_since, "21"}});
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  handler.run("MATCH (a)-->(b)-->(c) return a.age, b.age, c.age");
  
  EXPECT_EQ(2, handler.countRows());
}


TEST(Test, LongerPathPattern)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  /*
          -----
         v     |
   p1 -> p2 -> p3 -> p4
   ^                 |
   -----------------
   */
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  std::string p1 = db.addNode("Person", {{p_age, "1"}});
  std::string p2 = db.addNode("Person", {{p_age, "2"}});
  std::string p3 = db.addNode("Person", {{p_age, "3"}});
  std::string p4 = db.addNode("Person", {{p_age, "4"}});
  std::string r12 = db.addRelationship("Knows", p1, p2, {{p_since, "12"}});
  std::string r23 = db.addRelationship("Knows", p2, p3, {{p_since, "23"}});
  std::string r32 = db.addRelationship("Knows", p3, p2, {{p_since, "32"}});
  std::string r34 = db.addRelationship("Knows", p3, p4, {{p_since, "34"}});
  std::string r41 = db.addRelationship("Knows", p4, p1, {{p_since, "41"}});
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age");
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"1"}, {"2"}, {"3"}
      }, {
        {"3"}, {"2"}, {"3"}
      }, {
        {"2"}, {"3"}, {"4"}
      }, {
        {"2"}, {"3"}, {"2"}
      }, {
        {"3"}, {"4"}, {"1"}
      }, {
        {"4"}, {"1"}, {"2"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }
  
  // With one undirected relationship
  
  handler.run("MATCH (a)-[r1]-(b)-[r2]->(c) WHERE c.age = 3 return a.age, r1.since, b.age, r2.since");
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"3"}, {"32"}, {"2"}, {"23"}
      }, {
        {"1"}, {"12"}, {"2"}, {"23"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }

  // With two undirected relationships
  
  handler.run("MATCH (a)-[r1]-(b)-[r2]-(c) WHERE c.age = 3 return a.age, r1.since, b.age, r2.since");
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"3"}, {"32"}, {"2"}, {"23"}
      }, {
        {"1"}, {"12"}, {"2"}, {"23"}
      }, {
        {"3"}, {"23"}, {"2"}, {"32"}
      }, {
        {"1"}, {"12"}, {"2"}, {"32"}
      }, {
        {"1"}, {"41"}, {"4"}, {"34"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }

  // Non-equi-var expression in WHERE clause is not supported yet.
  EXPECT_THROW(handler.run("MATCH (a)-[]->(b)-[]->(c) WHERE a.age < b.age AND b.age < c.age return a.age, b.age, c.age"), std::exception);
  /*
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"1"}, {"2"}, {"3"}
      }, {
        {"2"}, {"3"}, {"4"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }*/

  handler.run("MATCH (a)-[]->(b)-[]->(a) return a.age, b.age, a.age");
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"3"}, {"2"}, {"3"}
      }, {
        {"2"}, {"3"}, {"2"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }
  
  handler.run("MATCH (a)-[]->(b)-[]->(c) WHERE id(a) <> id(c) return a.age, b.age, c.age");
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"1"}, {"2"}, {"3"}
      }, {
        {"2"}, {"3"}, {"4"}
      }, {
        {"3"}, {"4"}, {"1"}
      }, {
        {"4"}, {"1"}, {"2"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }
  
  handler.run("MATCH (a)-[]->(b)<-[]-(c) return a.age, b.age, c.age");
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {"1"}, {"2"}, {"3"}
      }, {
        {"3"}, {"2"}, {"1"}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes(handler.rows().begin(), handler.rows().end());
    EXPECT_EQ(expectedRes, actualRes);
  }
}


TEST(Test, Limit)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats>();
  
  auto & db = dbWrapper->getDB();
  /*
   -----
   v     |
   p1 -> p2 -> p3 -> p4
   ^                 |
   -----------------
   */
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  std::string p1 = db.addNode("Person", {{p_age, "1"}});
  std::string p2 = db.addNode("Person", {{p_age, "2"}});
  std::string p3 = db.addNode("Person", {{p_age, "3"}});
  std::string p4 = db.addNode("Person", {{p_age, "4"}});
  std::string r12 = db.addRelationship("Knows", p1, p2, {{p_since, "12"}});
  std::string r23 = db.addRelationship("Knows", p2, p3, {{p_since, "23"}});
  std::string r32 = db.addRelationship("Knows", p3, p2, {{p_since, "32"}});
  std::string r34 = db.addRelationship("Knows", p3, p4, {{p_since, "34"}});
  std::string r41 = db.addRelationship("Knows", p4, p1, {{p_since, "41"}});
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age");
  EXPECT_EQ(6, handler.rows().size());
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age LIMIT 10");
  EXPECT_EQ(6, handler.rows().size());
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age LIMIT 6");
  EXPECT_EQ(6, handler.rows().size());
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age LIMIT 5");
  EXPECT_EQ(5, handler.rows().size());
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age LIMIT 0");
  EXPECT_EQ(0, handler.rows().size());

  handler.run("MATCH (a) return a.age");
  EXPECT_EQ(4, handler.rows().size());
  handler.run("MATCH (a) return a.age LIMIT 5");
  EXPECT_EQ(4, handler.rows().size());
  handler.run("MATCH (a) return a.age LIMIT 2");
  EXPECT_EQ(2, handler.rows().size());
  handler.run("MATCH (a) return a.age LIMIT 0");
  EXPECT_EQ(0, handler.rows().size());
}


/*
 [Performance charts made using the test below this comment.]
 
 ----------------------------------------------------
 Simplest query:

 MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b)
 ----------------------------------------------------

 1:SELECT DestinationID FROM relationships WHERE ( OriginID IN carray(?1) )

 For countNodes = 64000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|
 |       1|             1|                     8|                 337 us|       122 us|           16 us|         0 us|       122 us|
 |       2|             8|                    66|                 231 us|       124 us|           48 us|         0 us|       124 us|
 |       3|            66|                    70|                 307 us|       192 us|           47 us|         0 us|       192 us|
 |       4|            70|                    63|                 273 us|       162 us|           36 us|         0 us|       162 us|
 |       5|            63|                   230|                 481 us|       290 us|          124 us|         0 us|       290 us|
 |       6|           230|                   973|                1678 us|      1057 us|          564 us|         0 us|      1057 us|
 |       7|           958|                  5751|                9384 us|      5590 us|         3343 us|         0 us|      5590 us|
 |       8|          5360|                 39080|               64897 us|     40282 us|        25963 us|         0 us|     40282 us|
 |       9|         26175|                     0|               26394 us|     16906 us|            0 us|         0 us|     16906 us|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|----------------------|-----------------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|
 |--------|--------------|----------------------|-----------------------|-------------|
 |       1|             1|                     1|                 148 us|        73 us|
 |       2|             1|                     2|                  91 us|        35 us|
 |       3|             2|                     6|                 110 us|        51 us|
 |       4|             6|                    24|                 144 us|        73 us|
 |       5|            24|                   129|                 379 us|       232 us|
 |       6|           129|                   783|                1485 us|       993 us|
 |       7|           783|                  5612|               10670 us|      7281 us|
 |       8|          5587|                 44023|               74237 us|     48222 us|
 |       9|         42115|                358533|              617320 us|    406305 us|
 |      10|        253529|                     0|              442888 us|    357819 us|
 |--------|--------------|----------------------|-----------------------|-------------|
 For countNodes = 6400000
 |--------|--------------|----------------------|-----------------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|
 |--------|--------------|----------------------|-----------------------|-------------|
 |       1|             1|                     1|                 158 us|        82 us|
 |       2|             1|                     2|                 100 us|        41 us|
 |       3|             2|                     6|                 115 us|        55 us|
 |       4|             6|                    64|                 218 us|       124 us|
 |       5|            64|                   150|                 527 us|       372 us|
 |       6|           150|                   798|                1664 us|      1158 us|
 |       7|           798|                  5593|               10461 us|      7184 us|
 |       8|          5588|                 45058|               81747 us|     55090 us|
 |       9|         44862|                392961|              669717 us|    444718 us|
 |      10|        378123|               3593280|             6111758 us|   4123018 us|
 |      11|       2566282|                     1|             9927384 us|   9087437 us|
 |--------|--------------|----------------------|-----------------------|-------------|

 -------------------------------------------------------------
 Returning a property of the origin entity:
 
 MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), a.age
 -------------------------------------------------------------

 1:SELECT OriginID, nodes.NodeType, DestinationID FROM relationships INNER JOIN nodes ON nodes.SYS__ID = relationships.OriginID WHERE ( OriginID IN carray(?1) )
 2:SELECT SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)

 For countNodes = 64000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     8|                 380 us|       192 us|           14 us|        11 us|       139 us|        53 us|
 |       2|             8|                    66|                 392 us|       230 us|           51 us|        12 us|       162 us|        68 us|
 |       3|            66|                    70|                 533 us|       360 us|           55 us|        11 us|       294 us|        66 us|
 |       4|            70|                    63|                 591 us|       415 us|           49 us|        13 us|       329 us|        86 us|
 |       5|            63|                   230|                 911 us|       557 us|          138 us|        41 us|       413 us|       144 us|
 |       6|           230|                   973|                2825 us|      1644 us|          503 us|       140 us|      1262 us|       382 us|
 |       7|           958|                  5751|               15522 us|      8614 us|         3643 us|       675 us|      7159 us|      1455 us|
 |       8|          5360|                 39080|              103587 us|     57013 us|        26885 us|      4161 us|     48653 us|      8360 us|
 |       9|         26175|                     0|               34216 us|     23531 us|            0 us|         0 us|     23531 us|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     1|                 459 us|       207 us|            8 us|        20 us|       142 us|        65 us|
 |       2|             1|                     2|                 198 us|       105 us|            3 us|         3 us|        64 us|        41 us|
 |       3|             2|                     6|                 197 us|       114 us|            5 us|         4 us|        68 us|        46 us|
 |       4|             6|                    24|                 277 us|       169 us|           18 us|         7 us|       108 us|        61 us|
 |       5|            24|                   129|                 664 us|       415 us|           97 us|        24 us|       296 us|       119 us|
 |       6|           129|                   783|                2750 us|      1790 us|          470 us|       114 us|      1367 us|       423 us|
 |       7|           783|                  5612|               17340 us|     10922 us|         3613 us|       697 us|      8740 us|      2182 us|
 |       8|          5587|                 44023|              124655 us|     72071 us|        29202 us|      4751 us|     60053 us|     12018 us|
 |       9|         42115|                358533|              966005 us|    538178 us|       216758 us|     35330 us|    463729 us|     74449 us|
 |      10|        253529|                     0|             1376975 us|   1272511 us|            0 us|         0 us|   1272511 us|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 6400000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     1|                 403 us|       195 us|            7 us|        20 us|       132 us|        63 us|
 |       2|             1|                     2|                 255 us|       148 us|            3 us|         3 us|       106 us|        42 us|
 |       3|             2|                     6|                 200 us|       118 us|            7 us|         4 us|        71 us|        47 us|
 |       4|             6|                    64|                 418 us|       272 us|           34 us|         9 us|       167 us|       105 us|
 |       5|            64|                   150|                1076 us|       807 us|          119 us|        31 us|       650 us|       157 us|
 |       6|           150|                   798|                3712 us|      2081 us|          495 us|       129 us|      1568 us|       513 us|
 |       7|           798|                  5593|               19201 us|     12551 us|         3486 us|       739 us|     10011 us|      2540 us|
 |       8|          5588|                 45058|              145653 us|     91196 us|        29730 us|      5327 us|     73891 us|     17305 us|
 |       9|         44862|                392961|             1124001 us|    670388 us|       247524 us|     39027 us|    567865 us|    102523 us|
 |      10|        378123|               3593280|            12523821 us|   8330796 us|      2115814 us|    402350 us|   7065993 us|   1264803 us|
 |      11|       2566282|                     1|            11655769 us|  10605694 us|            4 us|         5 us|  10605611 us|        83 us|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|

 -------------------------------------------------------------
 Returning a property of the found relationship:

 MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), r.since
 -------------------------------------------------------------
 
   This is slower than the test above: eventhough the first SQL query is faster because there is one less JOIN,
   the subsequent queries are slower because there are many more relationships (r) than origin entities (a).
   Also, we query 2 relationship tables (Knows / WorksWith) because relationships can have 2 different types,
   and the "since" property is present in both of them.

 1:SELECT relationships.SYS__ID, RelationshipType, DestinationID FROM relationships WHERE ( OriginID IN carray(?1) )
 2:SELECT SYS__ID, since FROM Knows WHERE SYS__ID IN carray(?1) UNION ALL SELECT SYS__ID, since FROM WorksWith WHERE SYS__ID IN carray(?2)
 For countNodes = 64000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     8|                 455 us|       241 us|           15 us|        37 us|       131 us|       110 us|
 |       2|             8|                    66|                 481 us|       296 us|           50 us|        58 us|       137 us|       159 us|
 |       3|            66|                    70|                 562 us|       370 us|           50 us|        66 us|       191 us|       179 us|
 |       4|            70|                    63|                 612 us|       421 us|           41 us|        92 us|       173 us|       248 us|
 |       5|            63|                   230|                1143 us|       759 us|          142 us|       219 us|       341 us|       418 us|
 |       6|           230|                   973|                4025 us|      2685 us|          550 us|       860 us|      1171 us|      1514 us|
 |       7|           958|                  5751|               23584 us|     15779 us|         3550 us|      4934 us|      6663 us|      9116 us|
 |       8|          5360|                 39080|              172256 us|    113364 us|        26420 us|     35387 us|     45874 us|     67490 us|
 |       9|         26175|                     0|               27677 us|     16843 us|            0 us|         0 us|     16843 us|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     1|                 387 us|       198 us|            8 us|        31 us|       122 us|        76 us|
 |       2|             1|                     2|                 202 us|       109 us|            3 us|         5 us|        51 us|        58 us|
 |       3|             2|                     6|                 221 us|       127 us|            8 us|         9 us|        67 us|        60 us|
 |       4|             6|                    24|                 286 us|       169 us|           16 us|        26 us|        86 us|        83 us|
 |       5|            24|                   129|                 768 us|       504 us|          103 us|       121 us|       253 us|       251 us|
 |       6|           129|                   783|                3444 us|      2347 us|          442 us|       680 us|      1102 us|      1245 us|
 |       7|           783|                  5612|               24492 us|     16780 us|         3418 us|      4907 us|      7277 us|      9503 us|
 |       8|          5587|                 44023|              200661 us|    134462 us|        28354 us|     40168 us|     54479 us|     79983 us|
 |       9|         42115|                358533|             1731335 us|   1145587 us|       212241 us|    374140 us|    435712 us|    709875 us|
 |      10|        253529|                     0|              806559 us|    703471 us|            0 us|         0 us|    703471 us|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 6400000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     1|                 408 us|       194 us|            8 us|        21 us|       128 us|        66 us|
 |       2|             1|                     2|                 192 us|       109 us|            3 us|         5 us|        51 us|        58 us|
 |       3|             2|                     6|                 204 us|       114 us|            7 us|         9 us|        59 us|        55 us|
 |       4|             6|                    64|                 493 us|       325 us|           39 us|        66 us|       142 us|       183 us|
 |       5|            64|                   150|                1044 us|       728 us|          113 us|       134 us|       429 us|       299 us|
 |       6|           150|                   798|                4123 us|      2974 us|          825 us|       738 us|      1591 us|      1383 us|
 |       7|           798|                  5593|               25638 us|     17773 us|         3651 us|      4882 us|      8145 us|      9628 us|
 |       8|          5588|                 45058|              208263 us|    143476 us|        28122 us|     40390 us|     60780 us|     82696 us|
 |       9|         44862|                392961|             1938717 us|   1297547 us|       244128 us|    397211 us|    514801 us|    782746 us|
 |      10|        378123|               3593280|            23704474 us|  17835999 us|      2069537 us|   4794693 us|   5864375 us|  11971624 us|
 |      11|       2566282|                     1|             9526119 us|   8495471 us|            5 us|         5 us|   8495383 us|        88 us|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 
 -------------------------------------------------------------
 Returning a property of the destination entity:
 
 MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), b.age
 -------------------------------------------------------------
   
   This is slower (especially when the count of nodes is larger) than the case above
   likely because in the query on the system relationships table,
   we JOIN the system nodes table to know the type of the _destination_ nodes.
   Also, the count of destination nodes is larger than the count of origin nodes
     so a JOIN on ids of destinatinon nodes is less efficient than a JOIN on ids of origin nodes.
   Note: we could try storing types of nodes inline in the system relationships table to improve this.
 
 1:SELECT DestinationID, dualNodes.NodeType FROM relationships INNER JOIN nodes dualNodes ON dualNodes.SYS__ID = relationships.DestinationID WHERE ( OriginID IN carray(?1) )
 2:SELECT SYS__ID, SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)
 For countNodes = 64000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     8|                 489 us|       274 us|           15 us|        46 us|       159 us|       115 us|
 |       2|             8|                    66|                 744 us|       546 us|           48 us|       115 us|       239 us|       307 us|
 |       3|            66|                    70|                 947 us|       712 us|           82 us|       166 us|       327 us|       385 us|
 |       4|            70|                    63|                 770 us|       553 us|           62 us|       104 us|       301 us|       252 us|
 |       5|            63|                   230|                1792 us|      1282 us|          170 us|       338 us|       647 us|       635 us|
 |       6|           230|                   973|                6772 us|      4383 us|          826 us|      1323 us|      1994 us|      2389 us|
 |       7|           958|                  5751|               32197 us|     21078 us|         3393 us|      7696 us|      7896 us|     13182 us|
 |       8|          5360|                 39080|              195810 us|    127431 us|        26510 us|     44000 us|     55770 us|     71661 us|
 |       9|         26175|                     0|               27388 us|     16851 us|            0 us|         0 us|     16851 us|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     1|                 409 us|       216 us|            9 us|        33 us|       137 us|        79 us|
 |       2|             1|                     2|                 194 us|       113 us|            4 us|         6 us|        64 us|        49 us|
 |       3|             2|                     6|                 231 us|       142 us|            7 us|        13 us|        74 us|        68 us|
 |       4|             6|                    24|                 390 us|       271 us|           20 us|        41 us|       137 us|       134 us|
 |       5|            24|                   129|                1324 us|      1025 us|          104 us|       207 us|       488 us|       537 us|
 |       6|           129|                   783|                6418 us|      5085 us|          495 us|      1155 us|      2261 us|      2824 us|
 |       7|           783|                  5612|               39886 us|     30315 us|         3434 us|      8003 us|     14384 us|     15931 us|
 |       8|          5587|                 44023|              301147 us|    222448 us|        29604 us|     60503 us|    114715 us|    107733 us|
 |       9|         42115|                358533|             2668507 us|   1986428 us|       227888 us|    452661 us|    883582 us|   1102846 us|
 |      10|        253529|                     0|              726078 us|    623288 us|            0 us|         0 us|    623288 us|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 6400000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|                     1|                 409 us|       215 us|            8 us|        32 us|       137 us|        78 us|
 |       2|             1|                     2|                 200 us|       119 us|            4 us|         6 us|        68 us|        51 us|
 |       3|             2|                     6|                 251 us|       163 us|            6 us|        13 us|        87 us|        76 us|
 |       4|             6|                    64|                 866 us|       680 us|           34 us|       111 us|       316 us|       364 us|
 |       5|            64|                   150|                1763 us|      1423 us|           99 us|       222 us|       742 us|       681 us|
 |       6|           150|                   798|                7681 us|      6236 us|          504 us|      1289 us|      2854 us|      3382 us|
 |       7|           798|                  5593|               50366 us|     40351 us|         3547 us|      8461 us|     18487 us|     21864 us|
 |       8|          5588|                 45058|              379496 us|    297275 us|        30634 us|     68984 us|    150447 us|    146828 us|
 |       9|         44862|                392961|             4100982 us|   3278733 us|       266490 us|    670160 us|   1256135 us|   2022598 us|
 |      10|        378123|               3593280|            34607726 us|  26770197 us|      2267789 us|   4784058 us|  12550165 us|  14220032 us|
 |      11|       2566282|                     1|             9478686 us|   8456150 us|            4 us|         6 us|   8456064 us|        86 us|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 
 -------------------------------------------------------------
 Returning a property of the destination entity AND a property of the origin entity:
 
 MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), a.age, b.age
 -------------------------------------------------------------
 
   Slightly slower than above, because we have one more JOIN in the system relationships table,
   and one more query per entity type (we could optimize this last part, i.e use the same query to find
   ages of a and b)

 1:SELECT OriginID, nodes.NodeType, DestinationID, dualNodes.NodeType FROM relationships INNER JOIN nodes ON nodes.SYS__ID = relationships.OriginID INNER JOIN nodes dualNodes ON dualNodes.SYS__ID = relationships.DestinationID WHERE ( OriginID IN carray(?1) )
 2:SELECT SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)
 3:SELECT SYS__ID, SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)
 For countNodes = 64000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|  SQL query 3|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|
 |       1|             1|                     8|                 540 us|       306 us|           14 us|        47 us|       161 us|        75 us|        70 us|
 |       2|             8|                    66|                 844 us|       586 us|           50 us|       106 us|       265 us|        58 us|       263 us|
 |       3|            66|                    70|                 913 us|       647 us|           55 us|       124 us|       322 us|        63 us|       262 us|
 |       4|            70|                    63|                 880 us|       623 us|           39 us|       117 us|       265 us|        70 us|       288 us|
 |       5|            63|                   230|                1865 us|      1259 us|          145 us|       353 us|       537 us|       104 us|       618 us|
 |       6|           230|                   973|                6967 us|      4690 us|          756 us|      1648 us|      1915 us|       286 us|      2489 us|
 |       7|           958|                  5751|               35778 us|     22968 us|         3441 us|      8067 us|      9156 us|      1469 us|     12343 us|
 |       8|          5360|                 39080|              226385 us|    139673 us|        26713 us|     46230 us|     62385 us|      8365 us|     68923 us|
 |       9|         26175|                     0|               35885 us|     23481 us|            0 us|         0 us|     23481 us|          N/A|          N/A|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|
 |       1|             1|                     1|                 334 us|       219 us|            2 us|         8 us|
 |       2|             1|                     2|                 234 us|       147 us|            4 us|         7 us|
 |       3|             2|                     6|                 305 us|       206 us|            7 us|        16 us|
 |       4|             6|                    24|                 496 us|       358 us|           17 us|        52 us|
 |       5|            24|                   129|                1592 us|      1202 us|          106 us|       239 us|
 |       6|           129|                   783|                9013 us|      7008 us|          712 us|      1376 us|
 |       7|           783|                  5612|               51923 us|     38992 us|         4166 us|      8930 us|
 |       8|          5587|                 44023|              369494 us|    265884 us|        31417 us|     65406 us|
 |       9|         42115|                358533|             2945133 us|   2035099 us|       258325 us|    465486 us|
 |      10|        253529|                     0|             1249938 us|   1134049 us|            0 us|         0 us|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|
 For countNodes = 6400000
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|
 |  Expand|  #Start nodes|  #Relationships found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|
 |       1|             1|                     1|                 322 us|       211 us|            3 us|         8 us|
 |       2|             1|                     2|                 235 us|       147 us|            2 us|         7 us|
 |       3|             2|                     6|                 308 us|       209 us|            8 us|        16 us|
 |       4|             6|                    64|                 980 us|       758 us|           49 us|       117 us|
 |       5|            64|                   150|                2059 us|      1614 us|          116 us|       271 us|
 |       6|           150|                   798|                9427 us|      7557 us|          812 us|      1437 us|
 |       7|           798|                  5593|               60008 us|     47055 us|         3564 us|      9433 us|
 |       8|          5588|                 45058|              451543 us|    343644 us|        29442 us|     73743 us|
 |       9|         44862|                392961|             5075694 us|   4028316 us|       257306 us|    665885 us|
 |      10|        378123|               3593280|            46501806 us|  36487243 us|      2512922 us|   5081182 us|
 |      11|       2566282|                     1|            13201392 us|  12063676 us|            5 us|         9 us|
 |--------|--------------|----------------------|-----------------------|-------------|----------------|-------------|
 
 -------------------------------------------------------------
 Returning the ages of persons one more hop away
 
 MATCH (a)-[r]->(b)-[r2]->(c) WHERE id(a) IN $list return id(b), c.age
 -------------------------------------------------------------
 
  Now we have a self join on the system relationships table, because the path pattern contains two consecutive relationships.
 
 1:SELECT R0.DestinationID, R1.DestinationID, N2.NodeType FROM relationships R0, relationships R1 INNER JOIN nodes N2 ON N2.SYS__ID = R1.DestinationID WHERE (R0.DestinationID = R1.OriginID) AND (R1.SYS__ID NOT IN (R0.SYS__ID) ) AND ( R0.OriginID IN carray(?1) )
 2:SELECT SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)
 For countNodes = 64000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|           66|                1137 us|       588 us|           44 us|        96 us|       321 us|       267 us|
 |       2|             8|           70|                 768 us|       500 us|           41 us|        70 us|       283 us|       217 us|
 |       3|            10|           63|                 717 us|       450 us|           27 us|        63 us|       252 us|       198 us|
 |       4|            10|          230|                1598 us|      1043 us|          159 us|       210 us|       565 us|       478 us|
 |       5|            36|          973|                4902 us|      3141 us|          422 us|       854 us|      1466 us|      1675 us|
 |       6|           141|         5908|               28216 us|     17784 us|         2726 us|      4910 us|      7950 us|      9834 us|
 |       7|           747|        44373|              185129 us|    109537 us|        20820 us|     27355 us|     56407 us|     53130 us|
 |       8|          4690|        28531|              135661 us|     85138 us|        11965 us|     15516 us|     54221 us|     30917 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|            2|                 549 us|       239 us|            8 us|        32 us|       155 us|        84 us|
 |       2|             1|            6|                 317 us|       158 us|            7 us|         8 us|        92 us|        66 us|
 |       3|             2|           24|                 488 us|       290 us|           17 us|        30 us|       165 us|       125 us|
 |       4|             6|          129|                1364 us|       983 us|           77 us|       128 us|       521 us|       462 us|
 |       5|            24|          783|                6146 us|      4694 us|          391 us|       750 us|      2328 us|      2366 us|
 |       6|           120|         5612|               37455 us|     27718 us|         2801 us|      5014 us|     15131 us|     12587 us|
 |       7|           726|        44404|              280782 us|    199170 us|        21541 us|     37996 us|    115783 us|     83387 us|
 |       8|          5083|       390109|             2645979 us|   1870108 us|       198699 us|    311589 us|   1006736 us|    863372 us|
 |       9|         38639|       232491|             1996900 us|   1541851 us|       110182 us|    142411 us|   1241128 us|    300723 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 6400000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|            2|                 539 us|       235 us|            6 us|        31 us|       153 us|        82 us|
 |       2|             1|            6|                 358 us|       187 us|            6 us|         9 us|       114 us|        73 us|
 |       3|             2|           64|                 920 us|       638 us|           29 us|        72 us|       329 us|       309 us|
 |       4|             6|          150|                1799 us|      1393 us|          104 us|       158 us|       810 us|       583 us|
 |       5|            26|          798|                7935 us|      6374 us|          410 us|       993 us|      2999 us|      3375 us|
 |       6|           122|         5593|               49393 us|     39296 us|         3478 us|      5775 us|     20287 us|     19009 us|
 |       7|           730|        45134|              346800 us|    265206 us|        22700 us|     43912 us|    148973 us|    116233 us|
 |       8|          5201|       396241|             3936920 us|   3055222 us|       198835 us|    454871 us|   1261377 us|   1793845 us|
 |       9|         40894|      3869897|            33781172 us|  24969381 us|      1878666 us|   3376362 us|  12176827 us|  12792554 us|
 |      10|        349395|      2263738|            38021215 us|  32633440 us|      1341331 us|   1832827 us|  25926825 us|   6706615 us|
 |      11|             1|            2|              338696 us|      6491 us|            7 us|         7 us|      6405 us|        86 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 
 -------------------------------------------------------------
 MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d) WHERE id(a) IN $list return id(b), d.age
 -------------------------------------------------------------

 1:SELECT R0.DestinationID, R2.DestinationID, N3.NodeType FROM relationships R0, relationships R1, relationships R2 INNER JOIN nodes N3 ON N3.SYS__ID = R2.DestinationID WHERE (R0.DestinationID = R1.OriginID) AND (R1.SYS__ID NOT IN (R0.SYS__ID) ) AND (R1.DestinationID = R2.OriginID) AND (R2.SYS__ID NOT IN (R0.SYS__ID, R1.SYS__ID) ) AND ( R0.OriginID IN carray(?1) )
 2:SELECT SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)
 For countNodes = 64000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|           70|                1167 us|       697 us|           44 us|       105 us|       409 us|       288 us|
 |       2|             6|           63|                 799 us|       508 us|           31 us|        60 us|       297 us|       211 us|
 |       3|             5|          230|                1641 us|      1066 us|          118 us|       205 us|       562 us|       504 us|
 |       4|             9|          973|                5216 us|      3364 us|          497 us|       870 us|      1642 us|      1722 us|
 |       5|            30|         5908|               28309 us|     18128 us|         3094 us|      4768 us|      8558 us|      9570 us|
 |       6|           134|        45417|              195296 us|    115931 us|        22152 us|     28167 us|     61052 us|     54879 us|
 |       7|           730|        36630|              166376 us|    108083 us|        20791 us|     17312 us|     73271 us|     34812 us|
 |       8|          2533|        48470|              208918 us|    128329 us|        23645 us|     21191 us|     86355 us|     41974 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|            6|                 631 us|       284 us|           11 us|        38 us|       178 us|       106 us|
 |       2|             1|           24|                 520 us|       292 us|           19 us|        27 us|       169 us|       123 us|
 |       3|             2|          129|                1426 us|      1005 us|           80 us|       132 us|       539 us|       466 us|
 |       4|             6|          783|                6270 us|      4765 us|          410 us|       748 us|      2408 us|      2357 us|
 |       5|            24|         5612|               37966 us|     27922 us|         2986 us|      5042 us|     15304 us|     12618 us|
 |       6|           120|        44404|              285005 us|    202092 us|        23680 us|     37640 us|    119202 us|     82890 us|
 |       7|           723|       393600|             2740883 us|   1955176 us|       208079 us|    302959 us|   1007207 us|    947969 us|
 |       8|          5042|       283602|             2345477 us|   1817510 us|       168323 us|    165760 us|   1471415 us|    346095 us|
 |       9|         18881|       378531|             2751460 us|   2078155 us|       195323 us|    178432 us|   1701368 us|    376787 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 For countNodes = 6400000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|
 |       1|             1|            6|                 656 us|       304 us|           11 us|        38 us|       197 us|       107 us|
 |       2|             1|           64|                 974 us|       677 us|           30 us|        66 us|       359 us|       318 us|
 |       3|             2|          150|                1875 us|      1411 us|           92 us|       162 us|       797 us|       614 us|
 |       4|             6|          798|                8119 us|      6459 us|          475 us|       885 us|      3421 us|      3038 us|
 |       5|            25|         5593|               49590 us|     39127 us|         3061 us|      5831 us|     20029 us|     19098 us|
 |       6|           121|        45134|              387765 us|    285351 us|        24319 us|     50041 us|    157812 us|    127539 us|
 |       7|           728|       396949|             4095930 us|   3161685 us|       218615 us|    455956 us|   1348676 us|   1813009 us|
 |       8|          5121|      3902696|            34605599 us|  25672245 us|      2015199 us|   3426533 us|  12659422 us|  13012823 us|
 |       9|         40512|      2687050|            29986624 us|  23929117 us|      1425869 us|   1887975 us|  17226759 us|   6702358 us|
 |      10|        167559|      3707017|            36545133 us|  28671527 us|      1772038 us|   2426256 us|  20169712 us|   8501815 us|
 |      11|             1|            6|              541981 us|       247 us|           10 us|        11 us|       172 us|        75 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|

 
 -------------------------------------------------------------
 MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d) WHERE id(a) IN $list AND b.age < 6000 AND c.age > 2000 return id(b), d.age
 -------------------------------------------------------------

 
 1:SELECT R0.DestinationID, N1.NodeType, R1.DestinationID, N2.NodeType, R2.DestinationID, N3.NodeType FROM relationships R0, relationships R1, relationships R2 INNER JOIN nodes N1 ON N1.SYS__ID = R0.DestinationID INNER JOIN nodes N2 ON N2.SYS__ID = R1.DestinationID INNER JOIN nodes N3 ON N3.SYS__ID = R2.DestinationID WHERE (R0.DestinationID = R1.OriginID) AND (R1.SYS__ID NOT IN (R0.SYS__ID) ) AND (R1.DestinationID = R2.OriginID) AND (R2.SYS__ID NOT IN (R0.SYS__ID, R1.SYS__ID) ) AND ( R0.OriginID IN carray(?1) )
 2:SELECT SYS__ID, SYS__ID FROM Person WHERE SYS__ID IN carray(?1) AND age < 6000
 3:SELECT SYS__ID FROM Person WHERE SYS__ID IN carray(?1) AND age > 2000
 4:SELECT SYS__ID, age FROM Person WHERE SYS__ID IN carray(?1)
 For countNodes = 64000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|  SQL query 3|  SQL query 4|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|-------------|
 |       1|             1|           19|                1690 us|      1172 us|          116 us|        93 us|       784 us|        88 us|        59 us|       241 us|
 |       2|             3|           43|                1013 us|       610 us|           38 us|        72 us|       300 us|        52 us|        69 us|       189 us|
 |       3|             4|          164|                2045 us|      1272 us|          159 us|       225 us|       643 us|        59 us|        78 us|       492 us|
 |       4|             7|          433|                6317 us|      4356 us|          677 us|       961 us|      2298 us|       101 us|       175 us|      1782 us|
 |       5|            19|         2382|               23031 us|     14274 us|         2397 us|      3471 us|      7170 us|       227 us|       638 us|      6239 us|
 |       6|            69|        13583|              136277 us|     83565 us|        17057 us|     18678 us|     45273 us|       817 us|      3071 us|     34404 us|
 |       7|           294|         8425|               96707 us|     64632 us|         9625 us|     10879 us|     41907 us|      1883 us|      1526 us|     19316 us|
 |       8|           672|         7923|               90750 us|     60216 us|         9823 us|     10646 us|     38231 us|      1090 us|      1540 us|     19355 us|
 |       9|           195|         3325|               39933 us|     25989 us|         3993 us|      5075 us|     15512 us|       742 us|       779 us|      8956 us|
 |      10|           123|         2028|               26712 us|     18030 us|         3258 us|      3493 us|     10774 us|       432 us|       541 us|      6283 us|
 |      11|            60|          959|               12914 us|      8531 us|         1160 us|      1671 us|      4998 us|       256 us|       313 us|      2964 us|
 |      12|            28|          529|                6571 us|      4428 us|          536 us|       808 us|      2588 us|       161 us|       210 us|      1469 us|
 |      13|            22|          207|                3527 us|      2460 us|          278 us|       374 us|      1497 us|       112 us|       105 us|       746 us|
 |      14|             1|            0|                 341 us|       116 us|            0 us|         0 us|       116 us|          N/A|          N/A|          N/A|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|-------------|
 For countNodes = 640000
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|-------------|
 |  Expand|  #Start nodes|  #Rows found|  Cypher query handling|  SQL queries|  SQL sys rel cb|  SQL prop cb|  SQL query 1|  SQL query 2|  SQL query 3|  SQL query 4|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|-------------|
 |       1|             1|            3|                 815 us|       408 us|           12 us|        42 us|       217 us|        81 us|        46 us|        64 us|
 |       2|             1|           16|                 715 us|       413 us|           25 us|        36 us|       203 us|        47 us|        45 us|       118 us|
 |       3|             2|           79|                1725 us|      1202 us|          114 us|       151 us|       625 us|        53 us|        67 us|       457 us|
 |       4|             4|          291|                5333 us|      3990 us|          491 us|       570 us|      2098 us|        97 us|       232 us|      1563 us|
 |       5|            11|         1424|               22432 us|     16779 us|         2138 us|      2585 us|      9244 us|       223 us|       789 us|      6523 us|
 |       6|            40|         8415|              117896 us|     82918 us|         9557 us|     14178 us|     48097 us|       779 us|      3519 us|     30523 us|
 |       7|           182|        57319|              754653 us|    523761 us|        72215 us|     90588 us|    323266 us|      3300 us|     15054 us|    182141 us|
 |       8|           976|        30950|              674360 us|    547452 us|        37773 us|     48010 us|    434921 us|      8225 us|      8888 us|     95418 us|
 |       9|          2377|        29646|              578894 us|    459761 us|        38254 us|     45970 us|    351597 us|      4591 us|      8961 us|     94612 us|
 |      10|           997|        26236|              455932 us|    354118 us|        33007 us|     41925 us|    257407 us|      3914 us|      7953 us|     84844 us|
 |      11|           884|        23945|              434510 us|    337860 us|        33040 us|     38859 us|    247622 us|      3861 us|      7416 us|     78961 us|
 |      12|           807|        18076|              361133 us|    285926 us|        29554 us|     31384 us|    212153 us|      3296 us|      6204 us|     64273 us|
 |      13|           600|        10369|              213201 us|    170737 us|        14577 us|     16591 us|    130235 us|      2311 us|      3752 us|     34439 us|
 |      14|           394|         5750|              117462 us|     92936 us|         7894 us|      9506 us|     68917 us|      1199 us|      2235 us|     20585 us|
 |      15|           165|         5227|               93537 us|     71875 us|         7453 us|      9278 us|     48378 us|       805 us|      2198 us|     20494 us|
 |      16|           121|         2602|               59861 us|     49123 us|         3822 us|      4620 us|     36441 us|       824 us|      1174 us|     10684 us|
 |      17|           131|         2109|               43598 us|     34567 us|         2570 us|      3969 us|     23734 us|       469 us|       962 us|      9402 us|
 |      18|            55|         1028|               23171 us|     18570 us|         1134 us|      1821 us|     13163 us|       386 us|       469 us|      4552 us|
 |      19|            52|          968|               18379 us|     14253 us|         1105 us|      1643 us|      9287 us|       235 us|       439 us|      4292 us|
 |      20|            22|         1501|               22951 us|     17417 us|         1806 us|      2328 us|     10556 us|       284 us|       630 us|      5947 us|
 |      21|            39|         5898|               77600 us|     56430 us|         7445 us|      8705 us|     34365 us|       516 us|      2053 us|     19496 us|
 |      22|            92|         2514|               60781 us|     49585 us|         3622 us|      4566 us|     36970 us|       957 us|      1078 us|     10580 us|
 |      23|           149|         2063|               43509 us|     34791 us|         2530 us|      4043 us|     23527 us|       497 us|      1001 us|      9766 us|
 |      24|            49|         1182|               25617 us|     20717 us|         1682 us|      2184 us|     14233 us|       368 us|       551 us|      5565 us|
 |      25|            43|          907|               18512 us|     14666 us|         1125 us|      1688 us|      9616 us|       245 us|       421 us|      4384 us|
 |      26|            22|          526|               11071 us|      8880 us|          555 us|       937 us|      5992 us|       191 us|       253 us|      2444 us|
 |      27|            20|          328|                7789 us|      6240 us|          474 us|       614 us|      4189 us|       123 us|       186 us|      1742 us|
 |      28|             4|           17|                1992 us|      1511 us|           87 us|       131 us|       984 us|        68 us|        65 us|       394 us|
 |      29|             2|           63|                1286 us|       857 us|           77 us|        97 us|       463 us|        45 us|        49 us|       300 us|
 |      30|             1|           36|                1169 us|       797 us|           45 us|        76 us|       471 us|        52 us|        46 us|       228 us|
 |      31|             4|            0|                 635 us|       384 us|           12 us|        14 us|       245 us|        37 us|        37 us|        65 us|
 |--------|--------------|-------------|-----------------------|-------------|----------------|-------------|-------------|-------------|-------------|-------------|
 // TODO run tests variations with:
 // - a WHERE clause on ids
 // - a WHERE clause on properties
 // - in each case, compare the case where the WHERE clause filters almost all records and the case where the clause does not filter records.
 */
TEST(Test, Perfs2)
{
  Timer timer{std::cout};

  LogIndentScope _{};

  const size_t countNodes {64000};

  auto dbWrapper = std::make_unique<GraphWithStats>("test.Perf2." + std::to_string(countNodes) + ".sqlite3db");
  //dbWrapper->m_printSQLRequests = true;

  std::mt19937 gen;
  std::uniform_int_distribution<size_t> distrNodes(0, countNodes - 1ull);
  // pick "root" node at random
  const auto rootNodeIdx = distrNodes(gen);

  auto & db = dbWrapper->getDB();
  if(!db.typesAndProperties().empty())
    timer.endStep("Read existing DB file");
  else
  {
    const auto p_age = mkProperty("age");
    const auto p_since = mkProperty("since");
    db.addType("Person", true, {p_age});
    db.addType("Knows", false, {p_since});
    db.addType("WorksWith", false, {p_since});
    
    timer.endStep("Non-system labeled entity/relationship property tables creation");

    // TODO make a parametrized tests.
    // See results in comment above this test.
    std::vector<ID> nodeIds;
    nodeIds.reserve(10000);
    
    const auto maxAge = 8000;
    // 5 ms per iteration without a transaction
    // 0.1 ms per iteration with a transaction
    // Ideally we should have one transaction per ~10000 inserts.
    for(int i=0;; ++i)
    {
      std::cout << i << "." << std::flush;
      db.beginTransaction();
      for(size_t i=0; i<maxAge; ++i)
      {
        nodeIds.push_back(db.addNode("Person", {{p_age, std::to_string(i)}}));
        if(nodeIds.size() == countNodes)
          break;
      }
      db.endTransaction();
      if(nodeIds.size() == countNodes)
        break;
    }
    std::cout << std::endl;
    timer.endStep(std::to_string(nodeIds.size()) + " nodes creation");
    
    std::vector<std::pair<size_t, size_t>> rels;
    rels.reserve(nodeIds.size());

    std::vector<size_t> curNodeIDx;
    std::vector<size_t> nextNodeIDx;
    curNodeIDx.push_back(rootNodeIdx);
    
    // start with 2 neighbours per node and double at each iteration.
    for(int countNeighbours = 1;; countNeighbours++)
    {
      const size_t countRelsToAdd = curNodeIDx.size() * countNeighbours;
      if(countRelsToAdd > nodeIds.size())
        break;
      std::cout << "will specify " << countRelsToAdd << " rels" << std::endl;
      for(const auto nodeIdx : curNodeIDx)
      {
        for(int i=0; i<countNeighbours; ++i)
        {
          // pick neighbours at random
          const auto neighbourIdx = distrNodes(gen);
          nextNodeIDx.push_back(neighbourIdx);
          rels.emplace_back(nodeIdx, neighbourIdx);
        }
      }
      curNodeIDx.clear();
      nextNodeIDx.swap(curNodeIDx);
    }
    std::cout << "Will create " << rels.size() << " relationships." << std::endl;
    
    size_t relIdx{};
    for(int i=0;; ++i)
    {
      std::cout << i << "." << std::flush;
      db.beginTransaction();
      for(size_t i=0; i<4000; ++i)
      {
        if(relIdx == rels.size())
          break;
        db.addRelationship("Knows", nodeIds[rels[relIdx].first], nodeIds[rels[relIdx].second], {{p_since, std::to_string(i)}});
        ++relIdx;
        if(relIdx == rels.size())
          break;
        db.addRelationship("WorksWith", nodeIds[rels[relIdx].first], nodeIds[rels[relIdx].second], {{p_since, std::to_string(2 * i)}});
        ++relIdx;
      }
      db.endTransaction();
      if(relIdx == rels.size())
        break;
    }
    std::cout << std::endl;
    timer.endStep(std::to_string(relIdx) + " relationships creation");
  }

  QueryResultsHandler handler(*dbWrapper);
  
  //dbWrapper->m_printSQLRequestsDuration = true;
  //dbWrapper->m_printSQLRequests = true;
  //handler.m_printCypherAST = true;

  std::unordered_set<std::string> nodeVisisted;
  nodeVisisted.reserve(countNodes);
  
  struct Stats{
    size_t countStartNodes{};
    size_t countRowsFetched{};
    
    std::chrono::steady_clock::duration timeCypher;
    std::chrono::steady_clock::duration timeSQL;
    
    // first query of queryStats
    std::chrono::steady_clock::duration timeSQLRelCb;
    // subsequent queries of queryStats
    std::chrono::steady_clock::duration timeSQLPropCb;
    
    std::vector<SQLQueryStat> queryStats;
  };
  std::vector<Stats> stats;

  // Here we assume that the id is 1 for the first node, 2 for the second, etc...
  // This is how sqlite auto increment "integer primary key" column works.
  std::vector<std::string> expandFronteer{/*nodeIds[rootNodeIdx]*/ std::to_string(1 + rootNodeIdx)};
  for(;;)
  {
    if(expandFronteer.empty())
      break;
    std::cout << "Expanding " << expandFronteer.size() << " nodes." << std::endl;
    std::ostringstream s;
    bool first = true;
    for(const auto & id : expandFronteer)
    {
      if(first)
        first = false;
      else
        s << ", ";
      s << id;
    }

    // Performances related to these queries are documented in the comment above this test function.

    handler.run("MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d) WHERE id(a) IN $list AND b.age < 6000 AND c.age > 2000 return id(b), d.age", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d) WHERE id(a) IN $list return id(b), d.age", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b)-[r2]->(c) WHERE id(a) IN $list return id(b), c.age", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), a.age", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), a.age, b.age", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), b.age", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b), r.since", {{SymbolicName{"list"}, expandFronteer}});
    //handler.run("MATCH (a)-[r]->(b) WHERE id(a) IN $list return id(b)", {{SymbolicName{"list"}, expandFronteer}});
    
    auto & st = stats.emplace_back();
    st.countRowsFetched = handler.countRows();
    st.countStartNodes = expandFronteer.size();
    st.timeCypher = handler.m_cypherQueryDuration;
    st.timeSQL = handler.m_sqlQueriesExecutionDuration;
    st.timeSQLRelCb = handler.m_sqlRelCbDuration;
    st.timeSQLPropCb = handler.m_sqlPropCbDuration;
    st.queryStats = dbWrapper->m_queryStats;

    expandFronteer.clear();
    
    for(const auto & row : handler.rows())
    {
      ASSERT_TRUE(row[0].has_value());
      const auto inserted = nodeVisisted.insert(*row[0]).second;
      if(inserted)
        expandFronteer.push_back(*row[0]);
      else
      {
        // Do nothing, we have already visited this node.
      }
    }
  }

  timer.endStep("Expand queries");

  auto writeMicros = [](auto duration){
    std::ostringstream s;
    s << std::chrono::duration_cast<std::chrono::microseconds>(duration).count() << " us";
    return s.str();
  };
  size_t countQueries{};
  for(const auto & stat : stats)
    countQueries = std::max(countQueries, stat.queryStats.size());

  std::vector<std::string> columnNames{
    "Expand",
    "#Start nodes",
    "#Rows found",
    "Cypher query handling",
    "SQL queries",
    "SQL sys rel cb",
    "SQL prop cb"
  };
  for(size_t i{}; i<countQueries; ++i)
    columnNames.push_back("SQL query " + std::to_string(i+1));

  std::vector<std::vector<std::string>> values;
  {
    size_t i{};
    for(const auto & stat : stats)
    {
      ++i;
      auto & v = values.emplace_back();
      v.push_back(std::to_string(i));
      v.push_back(std::to_string(stat.countStartNodes));
      v.push_back(std::to_string(stat.countRowsFetched));
      v.push_back(writeMicros(stat.timeCypher));
      v.push_back(writeMicros(stat.timeSQL));
      v.push_back(writeMicros(stat.timeSQLRelCb));
      v.push_back(writeMicros(stat.timeSQLPropCb));
      for(size_t j{}; j<countQueries; ++j)
      {
        if(j < stat.queryStats.size())
          v.push_back(writeMicros(stat.queryStats[j].duration));
        else
          v.push_back("N/A");
      }
    }
  }

  // Verify same-index queries are the same
  // and print SQL queries
  std::vector<std::string> refQueries;
  size_t expandIdx{1};
  for(const auto & stat : stats)
  {
    for(size_t i=0; i<stat.queryStats.size(); ++i)
    {
      const auto & query = stat.queryStats[i].query;
      if(i < refQueries.size())
      {
        if(refQueries[i] != query)
          std::cout << "[expand " << expandIdx << "]" << i+1 << ":" << query << std::endl;
      }
      else
      {
        refQueries.push_back(query);
        std::cout << i+1 << ":" << query << std::endl;
      }
    }
    ++expandIdx;
  }
  std::cout << "For countNodes = " << countNodes << std::endl;
  printChart(std::cout, &columnNames, values);
}

}
