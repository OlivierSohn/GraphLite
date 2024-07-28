#include "GraphDBSqlite.h"
#include "Logs.h"
#include "SqlAST.h"

#include <iostream>
#include <sstream>
#include <numeric>


struct IDAndType
{
  ID id{};
  size_t type{};
  
  bool operator == (IDAndType const & o) const { return id == o.id; }
};

namespace std
{
template<>
struct hash<IDAndType>
{
  size_t operator()(const IDAndType& i) const
  {
    return std::hash<std::string>()(i.id);
  }
};
}

GraphDB::GraphDB(const FuncOnSQLQuery& fOnSQLQuery, const FuncOnDBDiagnosticContent& fOnDiagnostic)
: m_fOnSQLQuery(fOnSQLQuery)
, m_fOnDiagnostic(fOnDiagnostic)
{
  LogIndentScope _ = logScope(std::cout, "Creating System tables...");

  if(auto res = sqlite3_open("test.sqlite3db", &m_db))
    throw std::logic_error(sqlite3_errstr(res));
  
  // TODO do not overwrite tables, read types from namedTypes.
  
  {
    LogIndentScope _ = logScope(std::cout, "Creating Nodes System table...");
    // This table avoids having to lookup into all nodes tables when looking for a specifiic entity.
    std::string typeName = "nodes";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      m_fOnSQLQuery(req);
      // ignore error
      auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0);
    }
    {
      std::ostringstream s;
      s << "CREATE TABLE " << typeName << " (";
      {
        s << m_idProperty << " INTEGER PRIMARY KEY, ";
        s << "NodeType INTEGER";
      }
      s << ");";
      const std::string req = s.str();
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      const std::string req = "CREATE INDEX NodeTypeIndex ON " + typeName + "(NodeType);";
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
  {
    LogIndentScope _ = logScope(std::cout, "Creating Relationships System table...");
    std::string typeName = "relationships";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      m_fOnSQLQuery(req);
      // ignore error
      auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0);
    }
    {
      std::ostringstream s;
      s << "CREATE TABLE " << typeName << " (";
      {
        s << m_idProperty << " INTEGER PRIMARY KEY, ";
        s << "RelationshipType INTEGER, ";
        s << "OriginID INTEGER, ";
        s << "DestinationID INTEGER";
      }
      s << ");";
      const std::string req = s.str();
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      const std::string req = "CREATE INDEX RelationshipTypeIndex ON " + typeName + "(RelationshipType);";
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      const std::string req = "CREATE INDEX originIDIndex ON " + typeName + "(OriginID);";
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      const std::string req = "CREATE INDEX destinationIDIndex ON " + typeName + "(DestinationID);";
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
  {
    LogIndentScope _ = logScope(std::cout, "Creating Types System table...");
    std::string typeName = "namedTypes";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      m_fOnSQLQuery(req);
      // ignore error
      auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0);
    }
    std::ostringstream s;
    s << "CREATE TABLE " << typeName << " (";
    {
      s << "TypeIdx INTEGER PRIMARY KEY, ";
      s << "Kind INTEGER, ";
      s << "NamedType TEXT";
    }
    s << ");";
    const std::string req = s.str();
    m_fOnSQLQuery(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
}

GraphDB::~GraphDB()
{
  sqlite3_close(m_db);
}

void GraphDB::addType(const std::string &typeName, bool isNode, const std::vector<PropertyKeyName> &properties)
{
  {
    const std::string req = "DROP TABLE " + typeName + ";";
    m_fOnSQLQuery(req);
    // ignore error
    auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0);
  }
  {
    std::ostringstream s;
    s << "CREATE TABLE " << typeName << " (";
    {
      s << m_idProperty << " INTEGER PRIMARY KEY"; // For simplicity we don't have a notion of objectid, only id.
      for(const auto & property: properties)
        s << ", " << property << " int DEFAULT NULL";
    }
    s << ");";
    const std::string req = s.str();
    m_fOnSQLQuery(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  // record type
  {
    std::ostringstream s;
    s << "INSERT INTO namedTypes (NamedType, Kind) Values('"
    << typeName << "', '"
    << (isNode ? "E" : "R")
    << "') RETURNING TypeIdx";
    const std::string req = s.str();
    m_fOnSQLQuery(req);
    size_t typeIdx{std::numeric_limits<size_t>::max()};
    char* msg{};
    if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_typeIdx, int argc, char **argv, char **column) {
      auto & typeIdx = *static_cast<size_t*>(p_typeIdx);
      for (int i=0; i< argc; i++)
        typeIdx = atoi(argv[i]);
      return 0;
    }, &typeIdx, &msg))
      throw std::logic_error(std::string{msg});
    if(typeIdx == std::numeric_limits<size_t>::max())
      throw std::logic_error("no result for typeIdx.");
    if(isNode)
      m_indexedNodeTypes.add(typeIdx, typeName);
    else
      m_indexedRelationshipTypes.add(typeIdx, typeName);
    auto & set = m_properties[typeName];
    for(const auto & propertyName : properties)
      set.insert(propertyName);
    m_properties[typeName].insert(PropertyKeyName{SymbolicName{m_idProperty}});
  }
  // todo rollback if there is an error.
}

bool GraphDB::prepareProperties(const std::string& typeName,
                                const std::vector<std::pair<PropertyKeyName, std::string>>& propValues,
                                std::vector<PropertyKeyName>& propertyNames,
                                std::vector<std::string>& propertyValues)
{
  propertyNames.clear();
  propertyValues.clear();
  
  auto it = m_properties.find(typeName);
  if(it == m_properties.end())
    return false;
  for(const auto & [name, value] : propValues)
  {
    if(!it->second.count(name))
      // Property doesn't exist in the schema
      return false;
    propertyValues.push_back(value);
    propertyNames.push_back(name);
  }
  return true;
}

bool GraphDB::findValidProperties(const std::string& typeName,
                                  const std::vector<PropertyKeyName>& propNames,
                                  std::vector<bool>& valid) const
{
  valid.clear();

  auto it = m_properties.find(typeName);
  if(it == m_properties.end())
    return false;
  valid.reserve(propNames.size());
  for(const auto& name : propNames)
    valid.push_back(it->second.count(name) > 0);
  return true;
}


ID GraphDB::addNode(const std::string& typeName,
                    const std::vector<std::pair<PropertyKeyName, std::string>>& propValues)
{
  const auto typeIdx = m_indexedNodeTypes.getIfExists(typeName);
  if(!typeIdx.has_value())
    throw std::logic_error("unknown node type: " + typeName);

  std::string nodeId;
  {
    std::ostringstream s;
    s << "INSERT INTO nodes (NodeType) Values(" << *typeIdx << ") RETURNING " << m_idProperty;
    const std::string req = s.str();
    m_fOnSQLQuery(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_nodeId, int argc, char **argv, char **column) {
      auto & nodeId = *static_cast<std::string*>(p_nodeId);
      for (int i=0; i< argc; i++)
        nodeId = argv[i];
      return 0;
    }, &nodeId, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  if(nodeId.empty())
    throw std::logic_error("no result for nodeId.");
  
  addElement(typeName, nodeId, propValues);
  return nodeId;
}

// There is a system table to generate relationship ids.
ID GraphDB::addRelationship(const std::string& typeName,
                            const ID& originEntity,
                            const ID& destinationEntity,
                            const std::vector<std::pair<PropertyKeyName, std::string>>& propValues)
{
  const auto typeIdx = m_indexedRelationshipTypes.getIfExists(typeName);
  if(!typeIdx.has_value())
    throw std::logic_error("unknown relationship type: " + typeName);

  // Verify the origin & destination node ids exist
  {
    size_t countMatches{};
    size_t expectedCountMatches{1};
    {
      std::ostringstream s;
      s << "SELECT " << m_idProperty << " from nodes WHERE " << m_idProperty << " in (";
      s << originEntity;
      if(originEntity != destinationEntity)
      {
        s << ", " << destinationEntity;
        ++expectedCountMatches;
      }
      s << ")";
      const std::string req = s.str();
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_countMatches, int argc, char **argv, char **column) {
        auto & countMatches = *static_cast<size_t*>(p_countMatches);
        ++countMatches;
        return 0;
      }, &countMatches, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    if(countMatches != expectedCountMatches)
      throw std::logic_error("origin or destination node not found.");
  }

  std::string relId;
  {
    std::ostringstream s;
    s << "INSERT INTO relationships (RelationshipType, OriginID, DestinationID) Values("
    << *typeIdx
    << ", "<< originEntity
    << ", " << destinationEntity
    <<") RETURNING " << m_idProperty;
    const std::string req = s.str();
    m_fOnSQLQuery(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_relId, int argc, char **argv, char **column) {
      auto & relId = *static_cast<std::string*>(p_relId);
      for (int i=0; i< argc; i++)
        relId = argv[i];
      return 0;
    }, &relId, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  if(relId.empty())
    throw std::logic_error("no result for relId.");
  addElement(typeName, relId, propValues);
  return relId;
}

void GraphDB::addElement(const std::string& typeName,
                         const ID& id,
                         const std::vector<std::pair<PropertyKeyName, std::string>>& propValues)
{
  std::vector<PropertyKeyName> propertyNames;
  std::vector<std::string> propertyValues;
  if(!prepareProperties(typeName, propValues, propertyNames, propertyValues))
    throw std::logic_error("some properties don't exist in the schema.");
  std::ostringstream s;
  s << "INSERT INTO " << typeName << " (" << m_idProperty;
  for(const auto & propertyName : propertyNames)
    s << ", " << propertyName;
  s << ") VALUES (";
  s << id;
  for(const auto & propertyValue : propertyValues)
    s << ", " << propertyValue;
  s << ");";
  const std::string req = s.str();
  m_fOnSQLQuery(req);
  if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
}

void GraphDB::print()
{
  std::vector<std::string> names;
  if(auto res = sqlite3_exec(m_db, "SELECT name FROM sqlite_master WHERE type='table';",
                             [](void *p_names, int argc, char **argv, char **column) {
    auto & names = *static_cast<std::vector<std::string>*>(p_names);
    for (int i=0; i< argc; i++)
      names.push_back(argv[i]);
    return 0;
  }, &names, 0))
    throw std::logic_error(sqlite3_errstr(res));
  
  for(const auto & name: names)
  {
    auto diagFunc = [](void *p_This, int argc, char **argv, char **column) {
      static_cast<GraphDB*>(p_This)->m_fOnDiagnostic(argc, argv, column);
      return 0;
    };
    {
      std::ostringstream s;
      s << "SELECT * FROM " << name;
      const std::string req = s.str();
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), diagFunc, this, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      std::ostringstream s;
      s << "PRAGMA table_info('" << name << "')";
      const std::string req = s.str();
      m_fOnSQLQuery(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(), diagFunc, this, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
}

std::vector<std::string> GraphDB::computeLabels(const Element elem, const std::vector<std::string>& inputLabels) const
{
  std::vector<std::string> labels = inputLabels;
  if(labels.empty())
  {
    switch(elem)
    {
      case Element::Node:
        for(const auto&[key, _] : m_indexedNodeTypes.getTypeToIndex())
          labels.push_back(key);
        break;
      case Element::Relationship:
        for(const auto&[key, _] : m_indexedRelationshipTypes.getTypeToIndex())
          labels.push_back(key);
        break;
    }
  }
  return labels;
}

std::vector<size_t> GraphDB::labelsToTypeIndices(const Element elem, const std::vector<std::string>& inputLabels) const
{
  std::vector<size_t> indices;
  switch(elem)
  {
    case Element::Node:
      for(const auto & label : inputLabels)
        if(auto val = m_indexedNodeTypes.getIfExists(label))
          indices.push_back(*val);
      break;
    case Element::Relationship:
      for(const auto & label : inputLabels)
        if(auto val = m_indexedRelationshipTypes.getIfExists(label))
          indices.push_back(*val);
      break;
  }
  return indices;
}

// In cypher when a property is missing, it is handled the same as if its value was null,
// but in SQL when a field is missing the query errors.
// So we replace non-existing properties by NULL.
// We also simplify (using a post order traversal) the SQL expression tree,
// and if the tree results in a single "NULL" node, we return false.
[[nodiscard]]
bool toEquivalentSQLFilter(const std::vector<const openCypher::Expression*>& cypherExprs,
                           const std::set<openCypher::PropertyKeyName>& sqlFields,
                           const std::map<openCypher::Variable, std::map<openCypher::PropertyKeyName, std::string>>& propertyMappingCypherToSQL,
                           std::string& sqlFilter)
{
  sqlFilter.clear();
  if(cypherExprs.empty())
    throw std::logic_error("expected at least one expression");
  
  std::unique_ptr<sql::Expression> sqlExpr;
  if(cypherExprs.size() == 1)
    sqlExpr = cypherExprs[0]->toSQLExpressionTree(sqlFields, propertyMappingCypherToSQL);
  else
  {
    std::vector<std::unique_ptr<sql::Expression>> sqlExprs;
    sqlExprs.reserve(cypherExprs.size());
    for(const auto & cypherExpr : cypherExprs)
      sqlExprs.push_back(cypherExpr->toSQLExpressionTree(sqlFields, propertyMappingCypherToSQL));
    sqlExpr = std::make_unique<sql::AggregateExpression>(sql::Aggregator::AND, std::move(sqlExprs));
  }

  // if the expression would be evaluated as FALSE in the WHERE clause, we return false.
  // if the expression would be evaluated as TRUE in the WHERE clause, we return true and make the clause empty.
  if(auto eval = sqlExpr->tryEvaluate())
  {
    switch(*eval)
    {
      case sql::Evaluation::Unknown:
      case sql::Evaluation::False:
        return false;
      case sql::Evaluation::True:
        return true;
    }
  }
  std::ostringstream s;
  sqlExpr->toString(s);
  sqlFilter = s.str();
  return true;
}


size_t countPropertiesNotEqual(const openCypher::PropertyKeyName& property,
                               const openCypher::VarsAndProperties& varsAndProperties)
{
  size_t count{};
  for(const auto & [_, properties]: varsAndProperties)
    count += properties.size() - properties.count(property);
  return count;
}

void GraphDB::forEachNodeAndRelatedRelationship(const TraversalDirection traversalDirection,
                                                const Variable* nodeVar,
                                                const Variable* relVar,
                                                const Variable* dualNodeVar,
                                                const std::vector<ReturnClauseTerm>& propertiesNode,
                                                const std::vector<ReturnClauseTerm>& propertiesRel,
                                                const std::vector<ReturnClauseTerm>& propertiesDualNode,
                                                const std::vector<std::string>& nodeLabelsStr,
                                                const std::vector<std::string>& relLabelsStr,
                                                const std::vector<std::string>& dualNodeLabelsStr,
                                                const ExpressionsByVarAndProperties& allFilters,
                                                FuncResults&f)
{
  if(traversalDirection == TraversalDirection::Any)
  {
    // Todo: Divide the count of queries by 2 with a special case.
    for(const auto td : {TraversalDirection::Forward, TraversalDirection::Backward})
      forEachNodeAndRelatedRelationship(td, nodeVar, relVar, dualNodeVar, propertiesNode, propertiesRel, propertiesDualNode,
                                        nodeLabelsStr, relLabelsStr, dualNodeLabelsStr,
                                        allFilters, f);
    return;
  }

  std::set<Variable> varsWithIDFiltering;
  std::vector<const Expression*> idFilters;
  std::map<Variable, std::vector<const Expression*>> postFilters;

  const auto idProperty = PropertyKeyName{SymbolicName{m_idProperty}};
  for(const auto & [varsAndProperties, expressions] : allFilters)
  {
    if(varsAndProperties.size() >= 2)
    {
      // 'expressions' use 2 or more variables
      
      if(countPropertiesNotEqual(idProperty, varsAndProperties) > 0)
        // At least one non-id property is used.
        // We could support this in the future by evaluating these expressions at the end
        // of this function when returning the results.
        throw std::logic_error("[Not supported] A non-equi-var expression is using non-id properties.");
      
      // only id properties are used so we will use these expressions to filter the system relationships table.
      
      for(const auto & [var, _] : varsAndProperties)
        varsWithIDFiltering.insert(var);
      idFilters.insert(idFilters.end(), expressions.begin(), expressions.end());
    }
    else if(varsAndProperties.size() == 1)
    {
      // 'expressions' uses a single variable
      if(countPropertiesNotEqual(idProperty, varsAndProperties) > 0)
      {
        // At least one non-id property is used.
        auto & postFiltersForVar = postFilters[varsAndProperties.begin()->first];
        postFiltersForVar.insert(postFiltersForVar.end(), expressions.begin(), expressions.end());
      }
      else if(varsAndProperties.begin()->second.count(idProperty))
      {
        // only id properties are used
        for(const auto & [var, _] : varsAndProperties)
          varsWithIDFiltering.insert(var);
        idFilters.insert(idFilters.end(), expressions.begin(), expressions.end());
      }
      else
        throw std::logic_error("[Unexpected] A filter expression has no property.");
    }
    else
      throw std::logic_error("[Unexpected] A filter expression has no variable.");
  }
  
  const std::string relatedNodeID = (traversalDirection == TraversalDirection::Forward) ? "OriginID" : "DestinationID";
  const std::string relatedDualNodeID = (traversalDirection == TraversalDirection::Backward) ? "OriginID" : "DestinationID";

  std::map<Variable, std::map<PropertyKeyName, std::string>> propertyMappingCypherToSQL;
  if(nodeVar)
    propertyMappingCypherToSQL[*nodeVar][idProperty] = relatedNodeID;
  if(dualNodeVar)
    propertyMappingCypherToSQL[*dualNodeVar][idProperty] = relatedDualNodeID;
  if(relVar)
    propertyMappingCypherToSQL[*relVar][idProperty] = "relationships.SYS__ID";

  const std::optional<std::vector<size_t>> nodeTypesFilter = nodeLabelsStr.empty() ?
    std::nullopt : std::optional{labelsToTypeIndices(Element::Node, nodeLabelsStr)};
  const std::optional<std::vector<size_t>> dualNodeTypesFilter = dualNodeLabelsStr.empty() ?
  std::nullopt : std::optional{labelsToTypeIndices(Element::Node, dualNodeLabelsStr)};
  const std::optional<std::vector<size_t>> relTypesFilter = relLabelsStr.empty() ?
    std::nullopt : std::optional{labelsToTypeIndices(Element::Relationship, relLabelsStr)};

  // No need to take into account varsWithIDFiltering here.
  const bool withDualNodesInfo = dualNodeTypesFilter.has_value() || !propertiesDualNode.empty() || (dualNodeVar && postFilters.count(*dualNodeVar));
  
  // No need to take into account varsWithIDFiltering here.
  const bool withNodeInfo = nodeTypesFilter.has_value() || !propertiesNode.empty() || (nodeVar && postFilters.count(*nodeVar));
  if(!withNodeInfo)
    throw std::logic_error("Unexpected, maybe you should use forEachElementPropertyWithLabelsIn");

  // todo: optionally use pagination to handle very large graphs.

  // 1. Scan the system table of relationships to gather callerRows

  using CallersRows =
  std::unordered_map<IDAndType /*node*/,
    std::unordered_map<IDAndType /*dual node*/,
  std::vector<IDAndType /*relationship*/>>>;

  CallersRows callerRows;

  // Filter relationships based on
  // - nodeLabelsStr
  // - relLabelsStr
  // - dualNodeLabelsStr

  // Filter on ids of rel, node, dualnode when possible.
  // For cases like this:
  // #  id(n) in <IDS> AND n.weight > 3 AND n.weight < 10
  // we know that we can filter on IDS (and this is what we do in this function), but in this case:
  // #  id(n) in <IDS> OR n.weight > 3
  // filtering on IDS would filter too much because some nodes with n.weight > 3 may not have their ids in IDS.
  // To handle this case we could run queries against non-system node tables to gather ids for which n.weight > 3
  // (if there is an index on weight?).
  // To decide whether that would be beneficial, we can look at the number of nodes in tables vs number of relationships,
  // whether there are indexes on weight properties,
  // whether other filters on ids (on the other node of the pattern, or on the relationship) exist so that
  // relying on them could be sufficient to reduce the count of relationships scanned, etc...
  // We could also leverage parallelism:
  // - in thread A, scan relationships without filtering ids of n, and proceed to computing the results
  // - in thread B, gather ids of nodes s.t n.weight > 3, then scan relationships by filtering on ids of n, and proceed to computing the results
  // The first thread ready to return results cancels the other thread.

  {
    std::ostringstream s;
    s << "SELECT relationships.SYS__ID, RelationshipType";
    s << ", " << relatedNodeID << ", nodes.NodeType";
    if(withDualNodesInfo)
      s << ", " << relatedDualNodeID << ", dualNodes.NodeType";
    s << " FROM relationships";
    s << " INNER JOIN nodes ON nodes.SYS__ID = relationships." << relatedNodeID;
    if(withDualNodesInfo)
      s << " INNER JOIN nodes dualNodes ON dualNodes.SYS__ID = relationships." << relatedDualNodeID;

    bool hasWhere{};
    auto addWhereTerm = [&]()
    {
      if(hasWhere)
        s << " AND ";
      else
      {
        hasWhere = true;
        s << " WHERE ";
      }
    };
    
    if(!idFilters.empty())
    {
      std::string sqlFilter;
      if(!toEquivalentSQLFilter(idFilters, {idProperty}, propertyMappingCypherToSQL, sqlFilter))
        throw std::logic_error("Should never happen: expressions in *FilterIDs are all equi-property with property m_idProperty");
      if(!sqlFilter.empty())
      {
        addWhereTerm();
        s << "( " << sqlFilter << " )";
      }
    }
    
    if(nodeVar && dualNodeVar && (*nodeVar == *dualNodeVar))
    {
      addWhereTerm();
      s << "( " << relatedDualNodeID << " = " << relatedNodeID << " )";
    }
    
    auto tryFilterTypes = [&](const std::optional<std::vector<size_t>>& typesFilter, std::string const& typeAlias)
    {
      if(typesFilter.has_value())
      {
        addWhereTerm();
        s << " " << typeAlias << " IN (";
        bool first = true;
        for(const auto typeIdx : *typesFilter)
        {
          if(first)
            first = false;
          else
            s << ",";
          s << typeIdx;
        }
        s << ")";
      }
    };

    tryFilterTypes(relTypesFilter, "RelationshipType");
    tryFilterTypes(nodeTypesFilter, "nodes.NodeType");
    tryFilterTypes(dualNodeTypesFilter, "dualNodes.NodeType");

    const std::string req = s.str();

    m_fOnSQLQuery(req);
    char*msg{};
    if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_callerRows, int argc, char **argv, char **column) {
      auto & callerRows = *static_cast<CallersRows*>(p_callerRows);
      auto relKey = IDAndType{argv[0], static_cast<size_t>(std::atoll(argv[1]))};
      auto nodeKey = IDAndType{argv[2], static_cast<size_t>(std::atoll(argv[3]))};
      auto dualNodeKey = (argc > 4) /* i.e withDualNodesInfo */
      ? IDAndType{argv[4], static_cast<size_t>(std::atoll(argv[5]))}
      : IDAndType{};
      callerRows[std::move(nodeKey)][std::move(dualNodeKey)].push_back(std::move(relKey));
      return 0;
    }, &callerRows, &msg))
      throw std::logic_error(msg);
  }

  // split nodes by types
  // split relationships by types

  std::unordered_map<size_t /*node type*/, std::vector<ID> /* node ids*/> nodesByTypes;
  std::unordered_map<size_t /*node type*/, std::unordered_set<ID> /* node ids*/> dualNodesByTypes;
  std::unordered_map<size_t /*rel type*/, std::vector<ID> /* rel ids*/> relsByTypes;
  
  for(const auto & [nodeIdAndType, dualNodeIdAndRelsIdsAndTypes] : callerRows)
  {
    nodesByTypes[nodeIdAndType.type].push_back(nodeIdAndType.id);
    for(const auto & [dualNodeIdAndType, relsIdsAndTypes] : dualNodeIdAndRelsIdsAndTypes)
    {
      if(withDualNodesInfo)
        dualNodesByTypes[dualNodeIdAndType.type].insert(dualNodeIdAndType.id);
      for(const auto & relIdAndType : relsIdsAndTypes)
        relsByTypes[relIdAndType.type].push_back(relIdAndType.id);
    }
  }

  // (todo in parallel)
  // Get needed property values of nodes, dual nodes and relationships
  //   and filter.

  std::vector<PropertyKeyName> strPropertiesNode;
  for(const auto & rct : propertiesNode)
    strPropertiesNode.push_back(rct.propertyName);

  std::vector<PropertyKeyName> strPropertiesDualNode;
  for(const auto & rct : propertiesDualNode)
    strPropertiesDualNode.push_back(rct.propertyName);

  std::vector<PropertyKeyName> strPropertiesRel;
  for(const auto & rct : propertiesRel)
    strPropertiesRel.push_back(rct.propertyName);

  auto gatherPropertyValues = [this, &postFilters](const Variable* var,
                                     const auto& elemsByType,
                                     const Element elem,
                                     const std::vector<PropertyKeyName>& strProperties,
                                     std::unordered_map<ID, std::vector<std::optional<std::string>>>& properties)
  {
    bool firstOutter = true;
    std::ostringstream s;

    std::vector<const Expression*>* postFilterForVar{};

    if(var)
      if(const auto it = postFilters.find(*var); it != postFilters.end())
        postFilterForVar = &it->second;

    for(const auto & [type, ids] : elemsByType)
    {
      const auto label = (elem == Element::Node)
      ? *m_indexedNodeTypes.getIfExists(type)
      : *m_indexedRelationshipTypes.getIfExists(type);

      std::vector<bool> validProperty;
      if(!findValidProperties(label, strProperties, validProperty))
        // label does not exist.
        continue;
      std::string sqlFilter{};
      if(postFilterForVar && !postFilterForVar->empty())
        if(!toEquivalentSQLFilter(*postFilterForVar, m_properties[label], {}, sqlFilter))
          // These items are excluded by the filter.
          continue;
      bool hasValidProperty{};
      for(const auto valid : validProperty)
        if(valid)
          hasValidProperty = true;
      
      if(!hasValidProperty && sqlFilter.empty())
      {
        const auto countProperties = strProperties.size();
        // no property is valid, so we know all returned property values would be null.
        // Since we don't do any filtering we can skip this query and instead manually
        // write the results now.
        for(const auto & id : ids)
          properties[id].resize(countProperties);
        continue;
      }
      if(firstOutter)
        firstOutter = false;
      else
        s << " UNION ALL ";
      s << "SELECT SYS__ID";
      for(size_t i=0, sz=validProperty.size(); i<sz; ++i)
      {
        const auto & propertyName = strProperties[i];
        s << ", ";
        if(!validProperty[i])
          s << "NULL as ";
        s << propertyName;
      }
      s << " FROM " << label;
      // TODO use bound param
      s << " WHERE SYS__ID IN (";
      bool first{true};
      for(const auto & id : ids)
      {
        if(first)
          first = false;
        else
          s << ", ";
        s << id;
      }
      s << ")";
      if(!sqlFilter.empty())
        s << " AND " << sqlFilter;
    }

    const std::string queryStr = s.str();
    if(!queryStr.empty())
    {
      m_fOnSQLQuery(queryStr);
      if(auto res = sqlite3_exec(m_db, queryStr.c_str(), [](void *p_properties, int argc, char **argv, char **column) {
        auto & properties = *static_cast<std::unordered_map<ID, std::vector<std::optional<std::string>>>*>(p_properties);
        auto & props = properties[argv[0]];
        for(int i=1; i<argc; ++i)
        {
          auto * arg = argv[i];
          props.push_back(arg ? std::optional{std::string{arg}} : std::nullopt);
        }
        return 0;
      }, &properties, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  };

  std::unordered_map<ID, std::vector<std::optional<std::string>>> nodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> dualNodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> relProperties;

  gatherPropertyValues(nodeVar, nodesByTypes, Element::Node, strPropertiesNode, nodeProperties);
  gatherPropertyValues(dualNodeVar, dualNodesByTypes, Element::Node, strPropertiesDualNode, dualNodeProperties);
  gatherPropertyValues(relVar, relsByTypes, Element::Relationship, strPropertiesRel, relProperties);

  // Return results according to callerRows

  // vecValues[i=0] will point to an element of nodeProperties
  // vecValues[i=1] will point to an element of relProperties
  // vecValues[i=2] will point to an element of dualNodeProperties
  VecValues vecValues;
  
  VecColumnNames vecColumnNames;

  vecColumnNames.push_back(&strPropertiesNode);
  vecColumnNames.push_back(&strPropertiesRel);
  vecColumnNames.push_back(&strPropertiesDualNode);

  std::vector<const std::vector<ReturnClauseTerm>*> vecReturnClauses;
  
  vecReturnClauses.push_back(&propertiesNode);
  vecReturnClauses.push_back(&propertiesRel);
  vecReturnClauses.push_back(&propertiesDualNode);

  vecValues.resize(vecReturnClauses.size());

  const ResultOrder resultOrder = computeResultOrder(vecReturnClauses);

  const auto emptyVec = std::vector<std::optional<std::string>>{};
  for(auto & v : vecValues)
    v = &emptyVec;

  for(const auto & [nodeIdAndType, dualNodeAndRelsIDsAndTypes] : callerRows)
  {
    const auto itNodeProperties = nodeProperties.find(nodeIdAndType.id);
    if(itNodeProperties == nodeProperties.end())
      continue;
    vecValues[0] = &itNodeProperties->second;
    for(const auto & [dualNodeIdAndType, relsIDsAndTypes] : dualNodeAndRelsIDsAndTypes)
    {
      const auto itDualNodeProperties = dualNodeProperties.find(dualNodeIdAndType.id);
      if(withDualNodesInfo)
      {
        if (itDualNodeProperties == dualNodeProperties.end())
          continue;
        else
          vecValues[2] = &itDualNodeProperties->second;
      }
      for(const auto & relIDAndType : relsIDsAndTypes)
      {
        const auto itRelProperties = relProperties.find(relIDAndType.id);
        if(itRelProperties == relProperties.end())
          continue;
        vecValues[1] = &itRelProperties->second;
        
        f(resultOrder,
          vecColumnNames,
          vecValues);
      }
    }
  }
}

void GraphDB::forEachElementPropertyWithLabelsIn(const Element elem,
                                            const std::vector<ReturnClauseTerm>& returnClauseTerms,
                                            const std::vector<std::string>& inputLabels,
                                            const std::vector<const Expression*>* filter,
                                            FuncResults& f)
{
  // extract property names
  std::vector<PropertyKeyName> propertyNames;
  propertyNames.reserve(returnClauseTerms.size());
  for(const auto & rct : returnClauseTerms)
    propertyNames.push_back(rct.propertyName);
  
  struct Results{
    Results(ResultOrder&&ro, VecColumnNames&& vecColumnNames, const FuncResults& func)
    : m_resultsOrder(std::move(ro))
    , m_vecColumnNames(std::move(vecColumnNames))
    , m_f(func)
    , m_vecValues{&m_values}
    {
      m_values.resize(m_vecColumnNames[0]->size());
    }

    const ResultOrder m_resultsOrder;
    const VecColumnNames m_vecColumnNames;
    std::vector<std::optional<std::string>> m_values;
    const FuncResults& m_f;
    const std::vector<const std::vector<std::optional<std::string>>*> m_vecValues;
  } results {
    computeResultOrder({&returnClauseTerms}),
    {&propertyNames},
    f
  };
  
  std::vector<bool> validProperty;
  std::ostringstream s;
  bool firstOutter = true;
  for(const auto & label : computeLabels(elem, inputLabels))
  {
    if(!findValidProperties(label, propertyNames, validProperty))
      // label does not exist.
      continue;
    std::string sqlFilter{};
    if(filter && !filter->empty())
      if(!toEquivalentSQLFilter(*filter, m_properties[label], {}, sqlFilter))
        // These items are excluded by the filter.
        continue;
    // in forEachNodeAndRelatedRelationship we have an optimization where
    // if all properties are invalid and we don't filter,
    // then we don't query and return results directly.
    // But here we don't know the ids so we have to query anyway.
    if(firstOutter)
      firstOutter = false;
    else
      s << " UNION ALL ";
    s << "SELECT ";
    bool first = true;
    for(size_t i=0, sz=validProperty.size(); i<sz; ++i)
    {
      const auto & propertyName = propertyNames[i];
      if(first)
        first = false;
      else
        s << ", ";
      if(!validProperty[i])
        s << "NULL as ";
      s << propertyName;
    }
    s << " FROM " << label;
    if(!sqlFilter.empty())
      s << " WHERE " << sqlFilter;
  }

  const std::string req = s.str();
  if(!req.empty())
  {
    m_fOnSQLQuery(req);
    char*msg{};
    if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_results, int argc, char **argv, char **column) {
      auto & results = *static_cast<Results*>(p_results);
      for(int i=0; i<argc; ++i)
      {
        auto * arg = argv[i];
        results.m_values[i] = arg ? std::optional{std::string{arg}} : std::nullopt;
      }
      results.m_f(results.m_resultsOrder, results.m_vecColumnNames, results.m_vecValues);
      return 0;
    }, &results, &msg))
      throw std::logic_error(msg);
  }
}

auto GraphDB::computeResultOrder(const std::vector<const std::vector<ReturnClauseTerm>*>& vecReturnClauses) -> ResultOrder
{
  const size_t resultsSize = std::accumulate(vecReturnClauses.begin(),
                                             vecReturnClauses.end(),
                                             0ull,
                                             [](const size_t acc, const std::vector<ReturnClauseTerm>* pVecReturnClauseTerms)
                                             {
    return acc + pVecReturnClauseTerms->size();
  });
  
  ResultOrder resultOrder(resultsSize);
  
  for(size_t i=0, szI = vecReturnClauses.size(); i<szI; ++i)
  {
    const auto & properties = *vecReturnClauses[i];
    for(size_t j=0, szJ = properties.size(); j<szJ; ++j)
    {
      const auto& p = properties[j];
      resultOrder[p.returnClausePosition] = {i, j};
    }
  }
  
  return resultOrder;
}
