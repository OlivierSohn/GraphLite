#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <filesystem>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"
#include "TestUtils.h"


namespace openCypher::test
{

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
  EXPECT_EQ(1, handler.countSQLQueries());

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
  EXPECT_EQ(1, handler.countSQLQueries());

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
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)<-[r]-(a) RETURN id(a), id(r)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[R]-(a) RETURN id(a), id(R)");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

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

}
