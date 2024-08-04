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

TEST(Test, ComparingValuesWithRefWrappedValues)
{
  Value v1(1ll);
  Value v2(2ll);
  std::set<Value> s1 = mkSet({v1, v2});

  const Value vConst1(1ll);
  const Value vConst2(2ll);
  std::set<Value> s2 = mkSet({vConst1, vConst2});
  ASSERT_EQ(s1, s2);
}


TEST(Test, EmptyDB)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  
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
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();

  auto & db = dbWrapper->getDB();

  db.addType("Entity", true, {});

  const auto entityID = db.addNode("Entity", {});

  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(entityID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (n) RETURN n.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(0, handler.countRows());

  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(0, handler.countRows());
}

TEST(Test, SingleRecursiveRelationship)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();

  auto & db = dbWrapper->getDB();

  db.addType("Entity", true, {});
  db.addType("Relationship", false, {});
  
  const auto entityID = db.addNode("Entity", {});
  const auto relationshipID = db.addRelationship("Relationship", entityID, entityID, {});
  
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
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
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
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();

  auto & db = dbWrapper->getDB();
  db.addType("Entity", true, {});
  db.addType("Relationship", false, {});
  
  const auto entityIDSource = db.addNode("Entity", {});
  const auto entityIDDestination = db.addNode("Entity", {});
  const auto relationshipID = db.addRelationship("Relationship", entityIDSource, entityIDDestination, {});
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) RETURN id(n)");
  
  EXPECT_EQ(2, handler.countRows());
  const auto expected = mkSet({entityIDSource, entityIDDestination});
  {
    const auto actual = mkSet({handler.rows()[0][0], handler.rows()[1][0]});
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(relationshipID, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH ()-[r]->() RETURN r.propertyDoesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
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
    const auto actual = mkSet({handler.rows()[0][0], handler.rows()[0][2]});
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(relationshipID, handler.rows()[1][1]);
  {
    const auto actual = mkSet({handler.rows()[1][0], handler.rows()[1][2]});
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
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});

  const auto entityIDSource = db.addNode("Person", {});
  const auto entityIDDestination = db.addNode("Person", {});
  const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, {});

  // querying some non-existing properties does require a SQL query on the typed table
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) return n.doesNotExist");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
  EXPECT_EQ(Nothing{}, handler.rows()[1][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (n) return n.age");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
  EXPECT_EQ(Nothing{}, handler.rows()[1][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return r.doesNotExist");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(Nothing{}, handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
}


TEST(Test, NonNullProperties)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  const auto entityIDSource = db.addNode("Person", mkVec(std::pair{p_age, Value(5)}));
  const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(10)}));
  const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(1234)}));
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) return n.age");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  auto expectedAges = mkSet<int64_t>({5, 10});
  const auto actualAges = mkSet({handler.rows()[0][0], handler.rows()[1][0]});
  EXPECT_EQ(expectedAges, actualAges);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() return r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(Value(1234), handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (a)-[r]->(b) return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(5), handler.rows()[0][0]);
  EXPECT_EQ(Value(10), handler.rows()[0][1]);
  EXPECT_EQ(Value(1234), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (b)<-[r]-(a) return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(5), handler.rows()[0][0]);
  EXPECT_EQ(Value(10), handler.rows()[0][1]);
  EXPECT_EQ(Value(1234), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
}

TEST(Test, DefaultValues)
{
  LogIndentScope _{};
  
  const std::filesystem::path dbFile{"Test.DefaultValues.sqlite3db"};

  const auto p_age = mkProperty("Age");
  const auto p_bytes = mkProperty("Bytes");
  const auto p_string = mkProperty("String");
  const auto p_double = mkProperty("Double");

  const auto p_since = mkProperty("since");
  const auto p_since2 = mkProperty("since2");

  const auto ageSchema = PropertySchema{
    p_age,
    ValueType::Integer,
    IsNullable::Yes,
    std::make_shared<Value>(3)
  };

  std::vector<unsigned char> bytes;
  bytes.push_back(0);
  bytes.push_back(0);
  bytes.push_back(2);
  bytes.push_back(0);
  bytes.push_back(0);
  const auto bytesSchema = PropertySchema{
    p_bytes,
    ValueType::ByteArray,
    IsNullable::No,
    std::make_shared<Value>(ByteArrayPtr::fromByteArray(bytes.data(), bytes.size()))
  };
  
  const auto stringSchema = PropertySchema{
    p_string,
    ValueType::String,
    IsNullable::Yes,
    std::make_shared<Value>(StringPtr::fromCStr("Hello 'World''"))
  };

  const auto doubleSchema = PropertySchema{
    p_double,
    ValueType::Float,
    IsNullable::No,
    std::make_shared<Value>(5.)
  };

  const auto sinceSchema = PropertySchema{
    p_since,
    ValueType::Integer,
    IsNullable::Yes,
    std::make_shared<Value>(Nothing{})
  };
  const auto since2Schema = PropertySchema{
    p_since2,
    ValueType::Integer,
    IsNullable::Yes
  };

  // Here we write the DB file
  {
    auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>(dbFile, Overwrite::Yes);
    
    auto & db = dbWrapper->getDB();
    db.addType("Person", true, {ageSchema, bytesSchema, stringSchema, doubleSchema});
    db.addType("Knows", false, {sinceSchema, since2Schema});
  }
  // Here we read the DB file we have written above
  {
    auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>(dbFile, Overwrite::No);
    
    auto & db = dbWrapper->getDB();
    ASSERT_TRUE(db.typesAndProperties().count("Person"));
    ASSERT_TRUE(db.typesAndProperties().count("Knows"));

    auto itPerson = db.typesAndProperties().find("Person");
    auto itKnows = db.typesAndProperties().find("Knows");
    ASSERT_TRUE(itPerson != db.typesAndProperties().end());
    ASSERT_TRUE(itKnows != db.typesAndProperties().end());

    auto itAge = itPerson->second.find({p_age});
    auto itBytes = itPerson->second.find({p_bytes});
    auto itString = itPerson->second.find({p_string});
    auto itDouble = itPerson->second.find({p_double});
    auto itSince = itKnows->second.find({p_since});
    auto itSince2 = itKnows->second.find({p_since2});
    ASSERT_TRUE(itAge != itPerson->second.end());
    ASSERT_TRUE(itBytes != itPerson->second.end());
    ASSERT_TRUE(itString != itPerson->second.end());
    ASSERT_TRUE(itDouble != itPerson->second.end());
    ASSERT_TRUE(itSince != itKnows->second.end());
    ASSERT_TRUE(itSince2 != itKnows->second.end());

    ASSERT_EQ(p_age.symbolicName.str, itAge->name.symbolicName.str);
    ASSERT_EQ(p_bytes.symbolicName.str, itBytes->name.symbolicName.str);
    ASSERT_EQ(p_string.symbolicName.str, itString->name.symbolicName.str);
    ASSERT_EQ(p_double.symbolicName.str, itDouble->name.symbolicName.str);
    ASSERT_EQ(p_since.symbolicName.str, itSince->name.symbolicName.str);
    ASSERT_EQ(p_since2.symbolicName.str, itSince2->name.symbolicName.str);

    ASSERT_EQ(ageSchema.type, itAge->type);
    ASSERT_EQ(bytesSchema.type, itBytes->type);
    ASSERT_EQ(stringSchema.type, itString->type);
    ASSERT_EQ(doubleSchema.type, itDouble->type);
    ASSERT_EQ(sinceSchema.type, itSince->type);
    ASSERT_EQ(since2Schema.type, itSince2->type);

    ASSERT_EQ(ageSchema.isNullable, itAge->isNullable);
    ASSERT_EQ(bytesSchema.isNullable, itBytes->isNullable);
    ASSERT_EQ(stringSchema.isNullable, itString->isNullable);
    ASSERT_EQ(doubleSchema.isNullable, itDouble->isNullable);
    ASSERT_EQ(sinceSchema.isNullable, itSince->isNullable);
    ASSERT_EQ(since2Schema.isNullable, itSince2->isNullable);

    ASSERT_EQ(static_cast<bool>(ageSchema.defaultValue), static_cast<bool>(itAge->defaultValue));
    ASSERT_EQ(static_cast<bool>(bytesSchema.defaultValue), static_cast<bool>(itBytes->defaultValue));
    ASSERT_EQ(static_cast<bool>(stringSchema.defaultValue), static_cast<bool>(itString->defaultValue));
    ASSERT_EQ(static_cast<bool>(doubleSchema.defaultValue), static_cast<bool>(itDouble->defaultValue));
    ASSERT_EQ(static_cast<bool>(sinceSchema.defaultValue), static_cast<bool>(itSince->defaultValue));
    ASSERT_EQ(static_cast<bool>(since2Schema.defaultValue), static_cast<bool>(itSince2->defaultValue));

    if(ageSchema.defaultValue)
      ASSERT_EQ(*ageSchema.defaultValue, *itAge->defaultValue);
    if(bytesSchema.defaultValue)
      ASSERT_EQ(*bytesSchema.defaultValue, *itBytes->defaultValue);
    if(stringSchema.defaultValue)
      ASSERT_EQ(*stringSchema.defaultValue, *itString->defaultValue);
    if(doubleSchema.defaultValue)
      ASSERT_EQ(*doubleSchema.defaultValue, *itDouble->defaultValue);
    if(sinceSchema.defaultValue)
      ASSERT_EQ(*sinceSchema.defaultValue, *itSince->defaultValue);
    if(since2Schema.defaultValue)
      ASSERT_EQ(*since2Schema.defaultValue, *itSince2->defaultValue);
    
    const std::vector<unsigned char> bytes1{0,1,2,3,6,7,8};

    const auto bytesVal = Value(ByteArrayPtr::fromByteArray(bytes1.data(), bytes1.size()));
    const auto stringVal = Value(StringPtr::fromCStr("ABC"));

    const auto entityIDSource = db.addNode("Person", mkVec(std::pair{p_age, Value(5)},
                                                         std::pair{p_bytes, copy(bytesVal)},
                                                         std::pair{p_string, copy(stringVal)},
                                                         std::pair{p_double, Value(-5.5)}));
    const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(10)}));
    const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(1234)}));

    QueryResultsHandler handler(*dbWrapper);

    handler.run("MATCH (n) WHERE n.Age = 5 return n.Age, n.Bytes, n.String, n.Double");

    EXPECT_EQ(1, handler.countRows());
    {
      EXPECT_EQ(Value(5), handler.rows()[0][0]);
      EXPECT_EQ(bytesVal, handler.rows()[0][1]);
      EXPECT_EQ(stringVal, handler.rows()[0][2]);
      EXPECT_EQ(Value(-5.5), handler.rows()[0][3]);
    }
  }
}

TEST(Test, ReturnIDs)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  
  const auto entityIDSource = db.addNode("Person", mkVec(std::pair{p_age, Value(5)}));
  const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(10)}));
  const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(1234)}));
  
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
  EXPECT_EQ(Value(10), handler.rows()[0][2]);
  EXPECT_EQ(Value(10), handler.rows()[0][3]);
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
  EXPECT_EQ(Value(1234), handler.rows()[0][2]);
  EXPECT_EQ(Value(1234), handler.rows()[0][3]);
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
  EXPECT_EQ(Value(1234), handler.rows()[0][7]);
  EXPECT_EQ(Value(1234), handler.rows()[0][8]);
  EXPECT_EQ(Value(5), handler.rows()[0][9]);
  EXPECT_EQ(Value(10), handler.rows()[0][10]);
  EXPECT_EQ(Value(5), handler.rows()[0][11]);
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
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  using ID = int64_t;
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});

  ID entityIDSource5;
  ID RelID;
  {
    entityIDSource5 = db.addNode("Person", mkVec(std::pair{p_age, Value(5)}));
    const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(10)}));
    RelID = db.addRelationship("Knows", entityIDSource5, entityIDDestination, mkVec(std::pair{p_since, Value(1234)}));
  }
  {
    const auto entityIDSource = db.addNode("Person", mkVec(std::pair{p_age, Value(105)}));
    const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(110)}));
    const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(123456)}));
  }

  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) WHERE n.age < 107 return n.age");
  
  EXPECT_EQ(3, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  std::set<Value> expectedAges;
  expectedAges.emplace(5);
  expectedAges.emplace(10);
  expectedAges.emplace(105);
  const auto actualAges = mkSet({handler.rows()[0][0], handler.rows()[1][0], handler.rows()[2][0]});
  EXPECT_EQ(expectedAges, actualAges);
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH ()-[r]-() WHERE r.since > 12345 return r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  EXPECT_EQ(Value(123456), handler.rows()[0][0]);
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = id(b) return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) WHERE id(b) = " + std::to_string(entityIDSource5) + " return a.age, b.age, r.since");
  
  EXPECT_EQ(0, handler.countRows());
  EXPECT_EQ(1, handler.countSQLQueries());

  handler.run("MATCH (a)-[r]->(b) WHERE id(a) = " + std::to_string(entityIDSource5) + " return a.age, b.age, r.since, id(a), id(r)");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(5, handler.countColumns());
  EXPECT_EQ(Value(5), handler.rows()[0][0]);
  EXPECT_EQ(Value(10), handler.rows()[0][1]);
  EXPECT_EQ(Value(1234), handler.rows()[0][2]);
  EXPECT_EQ(entityIDSource5, handler.rows()[0][3]);
  EXPECT_EQ(RelID, handler.rows()[0][4]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.

  handler.run("MATCH (a)-[r]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(105), handler.rows()[0][0]);
  EXPECT_EQ(Value(110), handler.rows()[0][1]);
  EXPECT_EQ(Value(123456), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (b)<-[r]-(a) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(105), handler.rows()[0][0]);
  EXPECT_EQ(Value(110), handler.rows()[0][1]);
  EXPECT_EQ(Value(123456), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  // not supported yet: "A non-equi-var expression is using non-id properties"
  EXPECT_THROW(handler.run("MATCH (b)<-[r]-(a) WHERE r.since > 12345 OR a.age < 107 return a.age, b.age, r.since"), std::logic_error);
}

TEST(Test, WhereClausesOptimized)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  
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
    const auto entityA1 = db.addNode("EntityA", mkVec(std::pair{p_propA, Value(1)}));
    const auto entityA2 = db.addNode("EntityA", mkVec(std::pair{p_propA, Value(2)}));
    const auto entityA3 = db.addNode("EntityA", mkVec(std::pair{p_propA, Value(3)}));

    const auto entityB1 = db.addNode("EntityB", mkVec(std::pair{p_propB, Value(1)}));
    const auto entityB2 = db.addNode("EntityB", mkVec(std::pair{p_propB, Value(2)}));
    const auto entityB3 = db.addNode("EntityB", mkVec(std::pair{p_propB, Value(3)}));

    db.addRelationship("RelAB", entityA1, entityB1, mkVec(std::pair{p_propA, Value(10)}));
    db.addRelationship("RelAB", entityA2, entityB2, mkVec(std::pair{p_propA, Value(20)}));
    db.addRelationship("RelAB", entityA3, entityB3, mkVec(std::pair{p_propA, Value(30)}));
    
    db.addRelationship("RelBA", entityB1, entityA1, mkVec(std::pair{p_propB, Value(10)}));
    db.addRelationship("RelBA", entityB2, entityA2, mkVec(std::pair{p_propB, Value(20)}));
    db.addRelationship("RelBA", entityB3, entityA3, mkVec(std::pair{p_propB, Value(30)}));
  }
  
  QueryResultsHandler handler(*dbWrapper);
  
  handler.run("MATCH (n) WHERE n.propA <= 2 return n.propA");
  
  EXPECT_EQ(2, handler.countRows());
  EXPECT_EQ(1, handler.countColumns());
  {
    auto expected = mkSet<int64_t>({1, 2});
    std::set<Value> actual = mkSet({handler.rows()[0][0], handler.rows()[1][0]});
    EXPECT_EQ(expected, actual);
  }
  EXPECT_EQ(1, handler.countSQLQueries());
  
  handler.run("MATCH (n)-[r]->() WHERE n.propA <= 2.5 AND n.propA >= 1.5 return n.propA, r.propA");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(2, handler.countColumns());
  EXPECT_EQ(Value(2), handler.rows()[0][0]);
  EXPECT_EQ(Value(20), handler.rows()[0][1]);
  EXPECT_EQ(3, handler.countSQLQueries()); // one for the system relationships table, one for EntityA table, one for RelAB table
  // The reason the table EntityB is not queried is because the where clause evaluates to false in this table (propA is not a field of this table)

  handler.run("MATCH (n)-[r]->() WHERE n.propA <= 2.5 AND r.propA >= 15 return n.propA, r.propA");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(2, handler.countColumns());
  EXPECT_EQ(Value(2), handler.rows()[0][0]);
  EXPECT_EQ(Value(20), handler.rows()[0][1]);
  EXPECT_EQ(3, handler.countSQLQueries()); // one for the system relationships table, one for EntityA table, one for RelAB table
  // The reason the table EntityB is not queried is because the where clause evaluates to false in this table (propA is not a field of this table)
}

TEST(Test, Labels)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  
  auto & db = dbWrapper->getDB();
  const auto p_age = mkProperty("age");
  const auto p_since = mkProperty("since");
  db.addType("Person", true, {p_age});
  db.addType("Knows", false, {p_since});
  db.addType("WorksWith", false, {p_since});
  
  {
    const auto entityIDSource = db.addNode("Person", mkVec(std::pair{p_age, Value(5)}));
    const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(10)}));
    const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(1234)}));
    const auto relationshipID2 = db.addRelationship("WorksWith", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(123444)}));
  }
  {
    const auto entityIDSource = db.addNode("Person", mkVec(std::pair{p_age, Value(105)}));
    const auto entityIDDestination = db.addNode("Person", mkVec(std::pair{p_age, Value(110)}));
    const auto relationshipID = db.addRelationship("Knows", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(123456)}));
    const auto relationshipID2 = db.addRelationship("WorksWith", entityIDSource, entityIDDestination, mkVec(std::pair{p_since, Value(12345666)}));
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
  EXPECT_EQ(Value(105), handler.rows()[0][0]);
  EXPECT_EQ(Value(110), handler.rows()[0][1]);
  EXPECT_EQ(Value(123456), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (a:Person)-[r:Knows]->(b) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(105), handler.rows()[0][0]);
  EXPECT_EQ(Value(110), handler.rows()[0][1]);
  EXPECT_EQ(Value(123456), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (a:Person)-[r:Knows]->(b:Person) WHERE r.since > 12345 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(105), handler.rows()[0][0]);
  EXPECT_EQ(Value(110), handler.rows()[0][1]);
  EXPECT_EQ(Value(123456), handler.rows()[0][2]);
  EXPECT_EQ(4, handler.countSQLQueries());
  // 4 because we need different queries on node and dualNode.
  
  handler.run("MATCH (a:Person)-[r:WorksWith]->(b:Person) WHERE r.since < 1234444 AND a.age < 107 return a.age, b.age, r.since");
  
  EXPECT_EQ(1, handler.countRows());
  EXPECT_EQ(3, handler.countColumns());
  EXPECT_EQ(Value(5), handler.rows()[0][0]);
  EXPECT_EQ(Value(10), handler.rows()[0][1]);
  EXPECT_EQ(Value(123444), handler.rows()[0][2]);
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
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  using ID = int64_t;

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
  
  ID p1 = db.addNode("Person", mkVec(std::pair{p_age, Value(1)}));
  ID p2 = db.addNode("Person", mkVec(std::pair{p_age, Value(2)}));
  ID r12 = db.addRelationship("Knows", p1, p2, mkVec(std::pair{p_since, Value(12)}));
  ID r21 = db.addRelationship("Knows", p2, p1, mkVec(std::pair{p_since, Value(21)}));
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  handler.run("MATCH (a)-->(b)-->(c)-->(d) return a.age, b.age, c.age, d.age");
  
  EXPECT_EQ(0, handler.countRows());
}

TEST(Test, PathAllowsNodesRepetition)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  using ID = int64_t;

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
  
  ID p1 = db.addNode("Person", mkVec(std::pair{p_age, Value(1)}));
  ID p2 = db.addNode("Person", mkVec(std::pair{p_age, Value(2)}));
  ID r12 = db.addRelationship("Knows", p1, p2, mkVec(std::pair{p_since, Value(12)}));
  ID r21 = db.addRelationship("Knows", p2, p1, mkVec(std::pair{p_since, Value(21)}));
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  handler.run("MATCH (a)-->(b)-->(c) return a.age, b.age, c.age");
  
  EXPECT_EQ(2, handler.countRows());
}


TEST(Test, LongerPathPattern)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  using ID = int64_t;

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
  
  ID p1 = db.addNode("Person", mkVec(std::pair{p_age, Value(1)}));
  ID p2 = db.addNode("Person", mkVec(std::pair{p_age, Value(2)}));
  ID p3 = db.addNode("Person", mkVec(std::pair{p_age, Value(3)}));
  ID p4 = db.addNode("Person", mkVec(std::pair{p_age, Value(4)}));
  ID r12 = db.addRelationship("Knows", p1, p2, mkVec(std::pair{p_since, Value(12)}));
  ID r23 = db.addRelationship("Knows", p2, p3, mkVec(std::pair{p_since, Value(23)}));
  ID r32 = db.addRelationship("Knows", p3, p2, mkVec(std::pair{p_since, Value(32)}));
  ID r34 = db.addRelationship("Knows", p3, p4, mkVec(std::pair{p_since, Value(34)}));
  ID r41 = db.addRelationship("Knows", p4, p1, mkVec(std::pair{p_since, Value(41)}));
  
  QueryResultsHandler handler(*dbWrapper);
  dbWrapper->m_printSQLRequests = true;
  
  handler.run("MATCH (a)-[]->(b)-[]->(c) return a.age, b.age, c.age");
  {
    std::vector<Value> test;
    test.push_back(Value{1ll});
    test.push_back(Value{1ll});
    const auto expectedRes = toValues(std::set<std::vector<int64_t>>{
      {
        1, 2, 3
      }, {
        3, 2, 3
      }, {
        2, 3, 4
      }, {
        2, 3, 2
      }, {
        3, 4, 1
      }, {
        4, 1, 2
      },
    });
    const std::set<std::vector<Value>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }
  
  // With one undirected relationship
  
  handler.run("MATCH (a)-[r1]-(b)-[r2]->(c) WHERE c.age = 3 return a.age, r1.since, b.age, r2.since");
  {
    const auto expectedRes = toValues(std::set<std::vector<int64_t>>{
      {
        3, 32, 2, 23
      }, {
        1, 12, 2, 23
      },
    });
    const std::set<std::vector<Value>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }
                                      
                                    

  // With two undirected relationships
  
  handler.run("MATCH (a)-[r1]-(b)-[r2]-(c) WHERE c.age = 3 return a.age, r1.since, b.age, r2.since");
  {
    const auto expectedRes = toValues(std::set<std::vector<int64_t>>{
      {
        3, 32, 2, 23
      }, {
        1, 12, 2, 23
      }, {
        3, 23, 2, 32
      }, {
        1, 12, 2, 32
      }, {
        1, 41, 4, 34
      },
    });
    const std::set<std::vector<Value>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }

  // Non-equi-var expression in WHERE clause is not supported yet.
  EXPECT_THROW(handler.run("MATCH (a)-[]->(b)-[]->(c) WHERE a.age < b.age AND b.age < c.age return a.age, b.age, c.age"), std::exception);
  /*
  {
    const std::set<std::vector<std::optional<std::string>>> expectedRes{
      {
        {Value(1)}, {Value(2)}, {Value(3)}
      }, {
        {Value(2)}, {Value(3)}, {Value(4)}
      },
    };
    const std::set<std::vector<std::optional<std::string>>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }*/

  handler.run("MATCH (a)-[]->(b)-[]->(a) return a.age, b.age, a.age");
  {
    const auto expectedRes = toValues(std::set<std::vector<int64_t>>{
      {
        3, 2, 3
      }, {
        2, 3, 2
      },
    });
    const std::set<std::vector<Value>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }
  
  handler.run("MATCH (a)-[]->(b)-[]->(c) WHERE id(a) <> id(c) return a.age, b.age, c.age");
  {
    const auto expectedRes = toValues(std::set<std::vector<int64_t>>{
      {
        1, 2, 3
      }, {
        2, 3, 4
      }, {
        3, 4, 1
      }, {
        4, 1, 2
      },
    });
    const std::set<std::vector<Value>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }
  
  handler.run("MATCH (a)-[]->(b)<-[]-(c) return a.age, b.age, c.age");
  {
    const auto expectedRes = toValues(std::set<std::vector<int64_t>>{
      {
        1, 2, 3
      }, {
        3, 2, 1
      },
    });
    const std::set<std::vector<Value>> actualRes = toSet(handler.rows());
    EXPECT_EQ(expectedRes, actualRes);
  }
}


TEST(Test, Limit)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  using ID = int64_t;

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
  
  ID p1 = db.addNode("Person", mkVec(std::pair{p_age, Value(1)}));
  ID p2 = db.addNode("Person", mkVec(std::pair{p_age, Value(2)}));
  ID p3 = db.addNode("Person", mkVec(std::pair{p_age, Value(3)}));
  ID p4 = db.addNode("Person", mkVec(std::pair{p_age, Value(4)}));
  ID r12 = db.addRelationship("Knows", p1, p2, mkVec(std::pair{p_since, Value(12)}));
  ID r23 = db.addRelationship("Knows", p2, p3, mkVec(std::pair{p_since, Value(23)}));
  ID r32 = db.addRelationship("Knows", p3, p2, mkVec(std::pair{p_since, Value(32)}));
  ID r34 = db.addRelationship("Knows", p3, p4, mkVec(std::pair{p_since, Value(34)}));
  ID r41 = db.addRelationship("Knows", p4, p1, mkVec(std::pair{p_since, Value(41)}));
  
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


TEST(Test, ParameterTypes)
{
  LogIndentScope _{};
  
  auto dbWrapper = std::make_unique<GraphWithStats<int64_t>>();
  using ID = int64_t;

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
  
  ID p1 = db.addNode("Person", mkVec(std::pair{p_age, Value(1)}));
  ID p2 = db.addNode("Person", mkVec(std::pair{p_age, Value(2)}));
  ID p3 = db.addNode("Person", mkVec(std::pair{p_age, Value(3)}));
  ID p4 = db.addNode("Person", mkVec(std::pair{p_age, Value(4)}));
  ID r12 = db.addRelationship("Knows", p1, p2, mkVec(std::pair{p_since, Value(12)}));
  ID r23 = db.addRelationship("Knows", p2, p3, mkVec(std::pair{p_since, Value(23)}));
  ID r32 = db.addRelationship("Knows", p3, p2, mkVec(std::pair{p_since, Value(32)}));
  ID r34 = db.addRelationship("Knows", p3, p4, mkVec(std::pair{p_since, Value(34)}));
  ID r41 = db.addRelationship("Knows", p4, p1, mkVec(std::pair{p_since, Value(41)}));
  
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
