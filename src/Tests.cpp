#include <gtest/gtest.h>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"

namespace test
{

struct QueryResultsHandler
{
  QueryResultsHandler(GraphDB& db)
  : m_db(db)
  {}
  
  void run(const std::string &cypherQuery)
  {
    openCypher::runCypher(cypherQuery, m_db, *this);
  }

  bool printCypherAST() const { return false; }

  bool m_printCypherQueryText{ false };
  bool m_printCypherRows{ false };

  void onCypherQueryStarts(std::string const & cypherQuery)
  {
    m_rows.clear();

    if(m_printCypherQueryText)
    {
      std::cout << std::endl;
      std::cout << "[openCypher] " << cypherQuery << std::endl;
      m_logIndentScope = std::make_unique<LogIndentScope>();
    }
  }
  void onOrderAndColumnNames(const GraphDB::ResultOrder& ro, const std::vector<std::string>& varNames, const GraphDB::VecColumnNames& colNames) {
    m_resultOrder = ro;
    m_variablesNames = varNames;
    m_columnNames = colNames;
  }
  
  void onRow(const GraphDB::VecValues& values)
  {
    if(m_printCypherRows)
    {
      auto _ = LogIndentScope();
      std::cout << LogIndent{};
      for(const auto & [i, j] : m_resultOrder)
        std::cout << m_variablesNames[i] << "." << (*m_columnNames[i])[j] << " = " << (*values[i])[j].value_or("<null>") << '|';
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

private:
  std::unique_ptr<LogIndentScope> m_logIndentScope;
  GraphDB::ResultOrder m_resultOrder;
  std::vector<std::string> m_variablesNames;
  GraphDB::VecColumnNames m_columnNames;
  
  GraphDB& m_db;
  std::vector<std::vector<std::optional<std::string>>> m_rows;
};


std::unique_ptr<GraphDB> createDB()
{
  const bool printSQLRequests{true};
  auto onSQLQuery = [=](const std::string& req)
  {
    if(printSQLRequests)
    {
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
  auto onDBDiagnosticContent = [=](int argc, char **argv, char **column)
  {
    if(printSQLRequests)
    {
      auto _ = LogIndentScope{};
      std::cout << LogIndent{};
      for (int i=0; i < argc; i++)
        printf("%s,\t", argv[i]);
      printf("\n");
    }
    return 0;
  };

  return std::make_unique<GraphDB>(onSQLQuery, onDBDiagnosticContent);
}


TEST(Test, EmptyDB)
{
  LogIndentScope _{};
  
  auto db = createDB();
  
  QueryResultsHandler handler(*db);
  
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
  
  auto db = createDB();
  
  db->addType("Entity", true, {});

  const std::string entityID = db->addNode("Entity", {});

  QueryResultsHandler handler(*db);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  
  handler.run("MATCH (n) RETURN n.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);

  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  
  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(0, handler.countRows());
}

TEST(Test, SingleRecursiveRelationship)
{
  LogIndentScope _{};
  
  auto db = createDB();
  
  db->addType("Entity", true, {});
  db->addType("Relationship", false, {});
  
  const std::string entityID = db->addNode("Entity", {});
  const std::string relationshipID = db->addRelationship("Relationship", entityID, entityID, {});
  
  QueryResultsHandler handler(*db);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  
  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  
  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  
  handler.run("MATCH (a)-[r]->(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  
  handler.run("MATCH (a)<-[r]-(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  
  handler.run("MATCH (a)-[R]-(B) RETURN id(a), id(R), id(B)");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);
  EXPECT_EQ(relationshipID, handler.rows()[1][1]);
  EXPECT_EQ(entityID, handler.rows()[1][0]);
  EXPECT_EQ(entityID, handler.rows()[1][2]);
  
  handler.run("MATCH (a)-[r]->(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  
  handler.run("MATCH (a)<-[r]-(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  
  handler.run("MATCH (a)-[R]-(a) RETURN id(a), id(R)");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(relationshipID, handler.rows()[1][1]);
  EXPECT_EQ(entityID, handler.rows()[1][0]);
  
  // id(a) <> id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) <> id(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(0, handler.countRows());
  
  // id(a) = id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = id(b) RETURN id(a), id(r), id(b)");

  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(entityID, handler.rows()[0][2]);

}


TEST(Test, SingleNonRecursiveRelationship)
{
  LogIndentScope _{};
  
  auto db = createDB();
  
  db->addType("Entity", true, {});
  db->addType("Relationship", false, {});
  
  const std::string entityIDSource = db->addNode("Entity", {});
  const std::string entityIDDestination = db->addNode("Entity", {});
  const std::string relationshipID = db->addRelationship("Relationship", entityIDSource, entityIDDestination, {});
  
  QueryResultsHandler handler(*db);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(2, handler.countRows());
  const auto expected = std::set<std::optional<std::string>>{entityIDSource, entityIDDestination};
  {
    const auto actual = std::set<std::optional<std::string>>{handler.rows()[0][0], handler.rows()[1][0]};
    EXPECT_EQ(expected, actual);
  }
  
  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  
  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(std::nullopt, handler.rows()[0][0]);
  
  handler.run("MATCH (a)-[r]->(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][2]);
  
  handler.run("MATCH (a)<-[r]-(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][0]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][2]);
  
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
  
  handler.run("MATCH (a)-[r]->(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  
  handler.run("MATCH (a)<-[r]-(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  
  handler.run("MATCH (a)-[R]-(a) RETURN id(a), id(R)");
  
  EXPECT_EQ(0, handler.countRows());
  
  // id(a) <> id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) <> id(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][1]);
  EXPECT_EQ(entityIDSource, handler.rows()[0][0]);
  EXPECT_EQ(entityIDDestination, handler.rows()[0][2]);
  
  // id(a) = id(b) is a constraint enforced while scanning the system relationships table.
  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = id(b) RETURN id(a), id(r), id(b)");
  
  EXPECT_EQ(0, handler.countRows());
}
}
