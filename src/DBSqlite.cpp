#include "DBSqlite.h"
#include "Logs.h"
#include "SqlAST.h"

#include <iostream>
#include <sstream>


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

int cbPrint(void *a_param, int argc, char **argv, char **column){
  auto _ = LogIndentScope{};
  std::cout << LogIndent{};
  for (int i=0; i< argc; i++)
    printf("%s,\t", argv[i]);
  printf("\n");
  return 0;
}

DB::DB(bool printSQLRequests)
: m_printSQLRequests(printSQLRequests)
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
      printReq(req);
      // ignore error
      auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0);
    }
    std::ostringstream s;
    s << "CREATE TABLE " << typeName << " (";
    {
      s << m_idProperty << " INTEGER PRIMARY KEY, ";
      s << "NodeType INTEGER";
    }
    s << ");";
    const std::string req = s.str();
    printReq(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  {
    LogIndentScope _ = logScope(std::cout, "Creating Relationships System table...");
    std::string typeName = "relationships";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      printReq(req);
      // ignore error
      auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0);
    }
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
    printReq(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  {
    LogIndentScope _ = logScope(std::cout, "Creating Types System table...");
    std::string typeName = "namedTypes";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      printReq(req);
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
    printReq(req);
    if(auto res = sqlite3_exec(m_db, req.c_str(), 0, 0, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }

}

DB::~DB()
{
  sqlite3_close(m_db);
}

void DB::addType(const std::string &typeName, bool isNode, const std::vector<std::string> &properties)
{
  {
    const std::string req = "DROP TABLE " + typeName + ";";
    printReq(req);
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
    printReq(req);
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
    printReq(req);
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
    m_properties[typeName] = std::unordered_set<std::string>{
      properties.begin(),
      properties.end()
    };
    m_properties[typeName].insert(m_idProperty);
  }
  // todo rollback if there is an error.
}

bool DB::prepareProperties(const std::string& typeName,
                           const std::vector<std::pair<std::string, std::string>>& propValues,
                           std::vector<std::string>& propertyNames,
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

bool DB::findValidProperties(const std::string& typeName,
                             const std::vector<std::string>& propNames,
                             std::vector<bool>& valid) const
{
  valid.clear();

  auto it = m_properties.find(typeName);
  if(it == m_properties.end())
    return false;
  for(const auto& name : propNames)
    valid.push_back(it->second.count(name) > 0);
  return true;
}


ID DB::addNode(const std::string& typeName, const std::vector<std::pair<std::string, std::string>>& propValues)
{
  const auto typeIdx = m_indexedNodeTypes.getIfExists(typeName);
  if(!typeIdx.has_value())
    throw std::logic_error("unknown node type: " + typeName);

  std::string nodeId;
  {
    std::ostringstream s;
    s << "INSERT INTO nodes (NodeType) Values(" << *typeIdx << ") RETURNING " << m_idProperty;
    const std::string req = s.str();
    printReq(req);
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
ID DB::addRelationship(const std::string& typeName, const ID& originEntity, const ID& destinationEntity, const std::vector<std::pair<std::string, std::string>>& propValues)
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
      printReq(req);
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
    printReq(req);
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

void DB::addElement(const std::string& typeName, const ID& id, const std::vector<std::pair<std::string, std::string>>& propValues)
{
  std::vector<std::string> propertyNames;
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
  printReq(req);
  if(auto res = sqlite3_exec(m_db, req.c_str(), cbPrint, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
}

void DB::print()
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
    {
      std::ostringstream s;
      s << "SELECT * FROM " << name;
      const std::string req = s.str();
      printReq(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(),
                                 cbPrint, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      std::ostringstream s;
      s << "PRAGMA table_info('" << name << "')";
      const std::string req = s.str();
      printReq(req);
      if(auto res = sqlite3_exec(m_db, req.c_str(),
                                 cbPrint, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
}

std::vector<std::string> DB::computeLabels(const Element elem, const std::vector<std::string>& inputLabels) const
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

std::vector<size_t> DB::labelsToTypeIndices(const Element elem, const std::vector<std::string>& inputLabels) const
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
                           const std::unordered_set<std::string>& sqlFields,
                           std::string& sqlFilter)
{
  if(cypherExprs.empty())
    throw std::logic_error("expected at least one expression");
  
  std::unique_ptr<sql::Expression> sqlExpr;
  if(cypherExprs.size() == 1)
    sqlExpr = cypherExprs[0]->toSQLExpressionTree(sqlFields);
  else
  {
    std::vector<std::unique_ptr<sql::Expression>> sqlExprs;
    sqlExprs.reserve(cypherExprs.size());
    for(const auto & cypherExpr : cypherExprs)
      sqlExprs.push_back(cypherExpr->toSQLExpressionTree(sqlFields));
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
        sqlFilter.clear();
        return true;
    }
  }
  std::ostringstream s;
  sqlExpr->toString(s);
  sqlFilter = s.str();
  return true;
}

void DB::forEachNodeAndRelatedRelationship(const TraversalDirection traversalDirection,
                                           const std::vector<ReturnClauseTerm>& propertiesNode,
                                           const std::vector<ReturnClauseTerm>& propertiesRel,
                                           const std::vector<ReturnClauseTerm>& propertiesDualNode,
                                           const std::vector<std::string>& nodeLabelsStr,
                                           const std::vector<std::string>& relLabelsStr,
                                           const std::vector<std::string>& dualNodeLabelsStr,
                                           const std::vector<const Expression*>& nodeFilter,
                                           const std::vector<const Expression*>& relFilter,
                                           const std::vector<const Expression*>& dualNodeFilter,
                                           FuncProp2&f)
{
  if(traversalDirection == TraversalDirection::Any)
  {
    // Todo: optimize the count of queries by doing a special case.
    // We can traverse the system relationships table once only.
    for(const auto td : {TraversalDirection::Forward, TraversalDirection::Backward})
      forEachNodeAndRelatedRelationship(td, propertiesNode, propertiesRel, propertiesDualNode, nodeLabelsStr, relLabelsStr, dualNodeLabelsStr,
                                        nodeFilter, relFilter, dualNodeFilter, f);
    return;
  }

  const std::string relatedNodeID = (traversalDirection == TraversalDirection::Forward) ? "OriginID" : "DestinationID";
  const std::string relatedDualNodeID = (traversalDirection == TraversalDirection::Backward) ? "OriginID" : "DestinationID";

  const std::optional<std::vector<size_t>> nodeTypesFilter = nodeLabelsStr.empty() ?
    std::nullopt : std::optional{labelsToTypeIndices(Element::Node, nodeLabelsStr)};
  const std::optional<std::vector<size_t>> dualNodeTypesFilter = dualNodeLabelsStr.empty() ?
  std::nullopt : std::optional{labelsToTypeIndices(Element::Node, dualNodeLabelsStr)};
  const std::optional<std::vector<size_t>> relTypesFilter = relLabelsStr.empty() ?
    std::nullopt : std::optional{labelsToTypeIndices(Element::Relationship, relLabelsStr)};

  const bool withDualNodesInfo = dualNodeTypesFilter.has_value() || !propertiesDualNode.empty();
  
  const bool withNodeInfo = nodeTypesFilter.has_value() || !propertiesNode.empty();
  if(!withNodeInfo)
    throw std::logic_error("Unexpected, maybe you should use forEachElementPropertyWithLabelsIn");

  // todo: optionally use pagination to handle very large graphs.

  // 1. Scan the system table of relationships to gather callerRows

  using CallersRows =
  std::unordered_map<IDAndType /*node*/,
    std::unordered_map<IDAndType /*dual node*/,
  std::vector<IDAndType /*relationship*/>>>;

  CallersRows callerRows;

  // Do not insert elements that are filtered by
  // - nodeLabelsStr
  // - relLabelsStr
  // - dualNodeLabelsStr

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
    if(relTypesFilter.has_value())
    {
      s << " WHERE RelationshipType IN (";
      hasWhere = true;
      bool first = true;
      for(const auto typeIdx : *relTypesFilter)
      {
        if(first)
          first = false;
        else
          s << ",";
        s << typeIdx;
      }
      s << ")";
    }
    if(nodeTypesFilter.has_value())
    {
      if(hasWhere)
        s << " AND ";
      else
      {
        hasWhere = true;
        s << " WHERE ";
      }
      s << " nodes.NodeType IN (";
      bool first = true;
      for(const auto typeIdx : *nodeTypesFilter)
      {
        if(first)
          first = false;
        else
          s << ",";
        s << typeIdx;
      }
      s << ")";
    }
    if(dualNodeTypesFilter.has_value())
    {
      if(hasWhere)
        s << " AND ";
      else
      {
        hasWhere = true;
        s << " WHERE ";
      }
      s << " dualNodes.NodeType IN (";
      bool first = true;
      for(const auto typeIdx : *dualNodeTypesFilter)
      {
        if(first)
          first = false;
        else
          s << ",";
        s << typeIdx;
      }
      s << ")";
    }
    const std::string req = s.str();

    printReq(req);
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
  
  // todo optimize for cases where we have no filter and we only want ids which we already have.

  std::vector<std::string> strPropertiesNode;
  for(const auto & rct : propertiesNode)
    strPropertiesNode.push_back(rct.propertyName);

  std::vector<std::string> strPropertiesDualNode;
  for(const auto & rct : propertiesDualNode)
    strPropertiesDualNode.push_back(rct.propertyName);

  std::vector<std::string> strPropertiesRel;
  for(const auto & rct : propertiesRel)
    strPropertiesRel.push_back(rct.propertyName);

  auto buildQuery = [this](const auto& elemsByType,
                           const Element elem,
                           const std::vector<std::string>& strProperties,
                           const std::vector<const Expression*>& filter,
                           std::unordered_map<ID, std::vector<std::optional<std::string>>>& properties)
  {
    bool firstOutter = true;
    std::ostringstream s;
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
      if(!filter.empty())
        if(!toEquivalentSQLFilter(filter, m_properties[label], sqlFilter))
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
    return s.str();
  };

  std::unordered_map<ID, std::vector<std::optional<std::string>>> nodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> dualNodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> relProperties;

  auto nodesQuery = buildQuery(nodesByTypes, Element::Node, strPropertiesNode, nodeFilter, nodeProperties);
  auto dualNodesQuery = buildQuery(dualNodesByTypes, Element::Node, strPropertiesDualNode, dualNodeFilter, dualNodeProperties);
  auto relsQuery = buildQuery(relsByTypes, Element::Relationship, strPropertiesRel, relFilter, relProperties);

  if(!nodesQuery.empty())
  {
    printReq(nodesQuery);
    if(auto res = sqlite3_exec(m_db, nodesQuery.c_str(), [](void *p_nodeProperties, int argc, char **argv, char **column) {
      auto & nodeProperties = *static_cast<std::unordered_map<ID, std::vector<std::optional<std::string>>>*>(p_nodeProperties);
      auto & props = nodeProperties[argv[0]];
      for(int i=1; i<argc; ++i)
      {
        auto * arg = argv[i];
        props.push_back(arg ? std::optional{std::string{arg}} : std::nullopt);
      }
      return 0;
    }, &nodeProperties, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }

  if(!dualNodesQuery.empty())
  {
    printReq(dualNodesQuery);
    if(auto res = sqlite3_exec(m_db, dualNodesQuery.c_str(), [](void *p_dualNodeProperties, int argc, char **argv, char **column) {
      auto & dualNodeProperties = *static_cast<std::unordered_map<ID, std::vector<std::optional<std::string>>>*>(p_dualNodeProperties);
      auto & props = dualNodeProperties[argv[0]];
      for(int i=1; i<argc; ++i)
      {
        auto * arg = argv[i];
        props.push_back(arg ? std::optional{std::string{arg}} : std::nullopt);
      }
      return 0;
    }, &dualNodeProperties, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  
  if(!relsQuery.empty())
  {
    printReq(relsQuery);
    if(auto res = sqlite3_exec(m_db, relsQuery.c_str(), [](void *p_relProperties, int argc, char **argv, char **column) {
      auto & relProperties = *static_cast<std::unordered_map<ID, std::vector<std::optional<std::string>>>*>(p_relProperties);
      auto & props = relProperties[argv[0]];
      for(int i=1; i<argc; ++i)
      {
        auto * arg = argv[i];
        props.push_back(arg ? std::optional{std::string{arg}} : std::nullopt);
      }
      return 0;
    }, &relProperties, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }

  // Return results according to callerRows

  for(const auto & [nodeIdAndType, dualNodeAndRelsIDsAndTypes] : callerRows)
  {
    const auto itNodeProperties = nodeProperties.find(nodeIdAndType.id);
    if(itNodeProperties == nodeProperties.end())
      continue;
    for(const auto & [dualNodeIdAndType, relsIDsAndTypes] : dualNodeAndRelsIDsAndTypes)
    {
      const auto itDualNodeProperties = dualNodeProperties.find(dualNodeIdAndType.id);
      if(withDualNodesInfo && (itDualNodeProperties == dualNodeProperties.end()))
        continue;
      for(const auto & relIDAndType : relsIDsAndTypes)
      {
        const auto itRelProperties = relProperties.find(relIDAndType.id);
        if(itRelProperties == relProperties.end())
          continue;
        f(itNodeProperties->second,
          itRelProperties->second,
          (!withDualNodesInfo) ? std::vector<std::optional<std::string>>{} : itDualNodeProperties->second,
          strPropertiesNode,
          strPropertiesRel,
          strPropertiesDualNode);
      }
    }
  }
}

void DB::forEachElementPropertyWithLabelsIn(const Element elem,
                                            const std::vector<ReturnClauseTerm>& returnClauseTerms,
                                            const std::vector<std::string>& inputLabels,
                                            const Expression* filter,
                                            FuncProp& f)
{
  // extract property names and verify they are in ascending order.
  std::vector<std::string> propertyNames;
  for(size_t i=0, sz=returnClauseTerms.size(); i<sz; ++i)
  {
    if(returnClauseTerms[i].returnClausePosition != i)
      // else we need to change how we print the properties.
      throw std::logic_error("This function assumes return clauses are ordered.");
    propertyNames.push_back(returnClauseTerms[i].propertyName);
  }
  
  std::vector<bool> validProperty;
  std::ostringstream s;
  bool firstOutter = true;
  for(const auto & label : computeLabels(elem, inputLabels))
  {
    if(!findValidProperties(label, propertyNames, validProperty))
      // label does not exist.
      continue;
    std::string sqlFilter{};
    if(filter)
      if(!toEquivalentSQLFilter(std::vector<const openCypher::Expression*>{filter}, m_properties[label], sqlFilter))
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
    printReq(req);
    char*msg{};
    if(auto res = sqlite3_exec(m_db, req.c_str(), [](void *p_f, int argc, char **argv, char **column) {
      auto & f = *static_cast<FuncProp*>(p_f);
      f(argc, argv, column);
      return 0;
    }, &f, &msg))
      throw std::logic_error(msg);
  }
}


void DB::printReq(const std::string& req) const
{
  if(m_printSQLRequests)
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
}
