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

GraphDB::GraphDB(const FuncOnSQLQuery& fOnSQLQuery, const FuncOnSQLQueryDuration& fOnSQLQueryDuration, const FuncOnDBDiagnosticContent& fOnDiagnostic)
: m_fOnSQLQuery(fOnSQLQuery)
, m_fOnSQLQueryDuration(fOnSQLQueryDuration)
, m_fOnDiagnostic(fOnDiagnostic)
{
  LogIndentScope _ = logScope(std::cout, "Creating System tables...");
  
  const bool useIndices = true;

  if(auto res = sqlite3_open("test.sqlite3db", &m_db))
    throw std::logic_error(sqlite3_errstr(res));
  
  // TODO do not overwrite tables, read types from namedTypes.
  
  {
    LogIndentScope _ = logScope(std::cout, "Creating Nodes System table...");
    // This table avoids having to lookup into all nodes tables when looking for a specifiic entity.
    std::string typeName = "nodes";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      // ignore error
      auto res = sqlite3_exec(req, 0, 0, 0);
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
      if(auto res = sqlite3_exec(req, 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    if(useIndices)
    {
      const std::string req = "CREATE INDEX NodeTypeIndex ON " + typeName + "(NodeType);";
      if(auto res = sqlite3_exec(req, 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
  {
    LogIndentScope _ = logScope(std::cout, "Creating Relationships System table...");
    std::string typeName = "relationships";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      // ignore error
      auto res = sqlite3_exec(req, 0, 0, 0);
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
      if(auto res = sqlite3_exec(req, 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    if(useIndices)
    {
      const std::string req = "CREATE INDEX RelationshipTypeIndex ON " + typeName + "(RelationshipType);";
      if(auto res = sqlite3_exec(req, 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    if(useIndices)
    {
      const std::string req = "CREATE INDEX originIDIndex ON " + typeName + "(OriginID);";
      if(auto res = sqlite3_exec(req, 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    if(useIndices)
    {
      const std::string req = "CREATE INDEX destinationIDIndex ON " + typeName + "(DestinationID);";
      if(auto res = sqlite3_exec(req, 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
  {
    LogIndentScope _ = logScope(std::cout, "Creating Types System table...");
    std::string typeName = "namedTypes";
    {
      const std::string req = "DROP TABLE " + typeName + ";";
      // ignore error
      auto res = sqlite3_exec(req, 0, 0, 0);
    }
    std::ostringstream s;
    s << "CREATE TABLE " << typeName << " (";
    {
      s << "TypeIdx INTEGER PRIMARY KEY, ";
      s << "Kind INTEGER, ";
      s << "NamedType TEXT";
    }
    s << ");";
    if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
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
    // ignore error
    auto res = sqlite3_exec(req, 0, 0, 0);
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
    if(auto res = sqlite3_exec(req, 0, 0, 0))
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
    size_t typeIdx{std::numeric_limits<size_t>::max()};
    char* msg{};
    if(auto res = sqlite3_exec(req, [](void *p_typeIdx, int argc, char **argv, char **column) {
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

void GraphDB::beginTransaction()
{
  if(auto res = sqlite3_exec("BEGIN TRANSACTION", 0, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
}
void GraphDB::endTransaction()
{
  if(auto res = sqlite3_exec("END TRANSACTION", 0, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
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
    if(auto res = sqlite3_exec(req, [](void *p_nodeId, int argc, char **argv, char **column) {
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
      if(auto res = sqlite3_exec(req, [](void *p_countMatches, int argc, char **argv, char **column) {
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
    if(auto res = sqlite3_exec(req, [](void *p_relId, int argc, char **argv, char **column) {
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
  if(auto res = sqlite3_exec(req, 0, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
}

void GraphDB::print()
{
  std::vector<std::string> names;
  if(auto res = sqlite3_exec("SELECT name FROM sqlite_master WHERE type='table';",
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
      if(auto res = sqlite3_exec(req, diagFunc, this, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      std::ostringstream s;
      s << "PRAGMA table_info('" << name << "')";
      const std::string req = s.str();
      if(auto res = sqlite3_exec(req, diagFunc, this, 0))
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

// An empty |labelsStr| means we want all types.
//
// When no value is returned it means all types are possible.
std::optional<std::set<size_t>>
GraphDB::computeTypeFilter(const Element e,
                           const std::vector<std::string>& labelsStr)
{
  if(labelsStr.empty())
    return std::nullopt;
  const auto & allTypes = (e == Element::Node) ? m_indexedNodeTypes : m_indexedRelationshipTypes;
  const auto countPossibleTypes = allTypes.getTypeToIndex().size();
  std::set<size_t> types;
  for(const auto & labelStr : labelsStr)
    if(auto i = allTypes.getIfExists(labelStr))
      types.insert(*i);
  if(types.size() == countPossibleTypes)
    // all types are posible.
    // the reason we return no value instead of all possible values
    // is because it will possibly optimize some queries.
    return std::nullopt;
  return types;
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
    // Todo: Divide the count of queries by 2 with a special case?
    for(const auto td : {TraversalDirection::Forward, TraversalDirection::Backward})
      forEachNodeAndRelatedRelationship(td, nodeVar, relVar, dualNodeVar, propertiesNode, propertiesRel, propertiesDualNode,
                                        nodeLabelsStr, relLabelsStr, dualNodeLabelsStr,
                                        allFilters, f);
    return;
  }

  std::set<Variable> varsWithIDFiltering;
  
  // These constraints are only on ids so we can apply them while querying the relationships system table.
  std::vector<const Expression*> idFilters;
  
  // these constraints contain non-id properties so we apply them while querying the non-system relationship/entity tables.
  struct VariablePostFilters{
    // These are the properties used in filters.
    std::set<PropertyKeyName> properties;
 
    std::vector<const Expression*> filters;
  };
  std::map<Variable, VariablePostFilters> postFilters;

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
        VariablePostFilters & postFiltersForVar = postFilters[varsAndProperties.begin()->first];
        for(const PropertyKeyName & property : varsAndProperties.begin()->second)
          postFiltersForVar.properties.insert(property);
        postFiltersForVar.filters.insert(postFiltersForVar.filters.end(), expressions.begin(), expressions.end());
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

  auto needsTypeInfo = [&](const Variable * var, const std::vector<ReturnClauseTerm>& propertiesReturnClause)
  {
    // If the return clause contains some non-id properties,
    // we need to know the type of the element because
    // we may need to query property tables to find the value of these properties
    // (unless these properties are not valid for the property table but this will be checked later,
    //  once we know the type of the element)
    for(const auto & p : propertiesReturnClause)
      if(p.propertyName != idProperty)
        return true;
    
    // If there are some constraints on non-id properties,
    // we will have to query the property tables to know if the element is discarded.
    if(var)
    {
      if(auto it = postFilters.find(*var); it != postFilters.end())
      {
        // By construction of post filters, a non-id property will be used by this post-filter.
        
        // Sanity check
        bool hasNonIdProperty{};
        for(const auto & prop : it->second.properties)
          if(prop != idProperty)
          {
            hasNonIdProperty = true;
            break;
          }
        if(!hasNonIdProperty)
          throw std::logic_error("[Unexpected] A post-filter has no non-id property.");
        
        return true;
      }
    }
    return false;
  };

  // Whether to return the type information in the relationships system table query.
  const bool nodeNeedsTypeInfo = needsTypeInfo(nodeVar, propertiesNode);
  const bool relNeedsTypeInfo = needsTypeInfo(relVar, propertiesRel);
  const bool dualNodeNeedsTypeInfo = needsTypeInfo(dualNodeVar, propertiesDualNode);

  const std::optional<std::set<size_t>> nodeTypesFilter = computeTypeFilter(Element::Node, nodeLabelsStr);
  const std::optional<std::set<size_t>> relTypesFilter = computeTypeFilter(Element::Relationship, relLabelsStr);
  const std::optional<std::set<size_t>> dualNodeTypesFilter = computeTypeFilter(Element::Node, dualNodeLabelsStr);
  
  const std::string relatedNodeID = (traversalDirection == TraversalDirection::Forward) ? "OriginID" : "DestinationID";
  const std::string relatedDualNodeID = (traversalDirection == TraversalDirection::Backward) ? "OriginID" : "DestinationID";

  std::map<Variable, std::map<PropertyKeyName, std::string>> propertyMappingCypherToSQL;
  // Note: when *nodeVar == *dualNodeVar, the code below overwrites relatedNodeID with relatedDualNodeID
  // whish is OK because when *nodeVar == *dualNodeVar we add a query constraint "relatedDualNodeID = relatedNodeID".
  if(nodeVar)
    propertyMappingCypherToSQL[*nodeVar][idProperty] = relatedNodeID;
  if(dualNodeVar)
    propertyMappingCypherToSQL[*dualNodeVar][idProperty] = relatedDualNodeID;
  if(relVar)
    propertyMappingCypherToSQL[*relVar][idProperty] = "relationships.SYS__ID";

  // todo: optionally use pagination to handle very large graphs.
  // https://stackoverflow.com/a/65647734/3940228
  // This will be needed when the cypher query has a LIMIT clause.

  // 1. Scan the system table of relationships to gather candidate rows

  struct CandidateRow{
    // 0 : node, 1 : relationship, 2 : dual node
    std::array<IDAndType, 3> idsAndTypes;
  };
  using CandidateRows = std::vector<CandidateRow>;

  CandidateRows candidateRows;

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

  // if the node/dualNode/rel is not post filtered, and no property is returned for the node/dualNode/rel,
  // we don't lookup the properties.
  const bool lookupNodesProperties = !propertiesNode.empty() || (nodeVar && postFilters.count(*nodeVar));
  const bool lookupRelsProperties = !propertiesRel.empty() || (relVar && postFilters.count(*relVar));
  const bool lookupDualNodesProperties = !propertiesDualNode.empty() || (dualNodeVar && postFilters.count(*dualNodeVar));
 
  {
    struct RelationshipQueryInfo{
      std::chrono::steady_clock::duration& totalSystemRelationshipCbDuration;
      CandidateRows & candidateRows;
      std::optional<unsigned> indexNodeID;
      std::optional<unsigned> indexRelationshipID;
      std::optional<unsigned> indexDualNodeID;
      std::optional<unsigned> indexNodeType;
      std::optional<unsigned> indexRelationshipType;
      std::optional<unsigned> indexDualNodeType;
    } queryInfo{m_totalSystemRelationshipCbDuration, candidateRows};

    std::ostringstream s;
    s << "SELECT ";
    unsigned index{};
    if(lookupNodesProperties)
    {
      if(index)
        s << ", ";
      s << relatedNodeID;
      queryInfo.indexNodeID = index;
      ++index;
    }
    if(nodeNeedsTypeInfo)
    {
      if(index)
        s << ", ";
      s << "nodes.NodeType";
      queryInfo.indexNodeType = index;
      ++index;
    }
    if(lookupRelsProperties)
    {
      if(index)
        s << ", ";
      s << "relationships.SYS__ID";
      queryInfo.indexRelationshipID = index;
      ++index;
    }
    if(relNeedsTypeInfo)
    {
      if(index)
        s << ", ";
      s << "RelationshipType";
      queryInfo.indexRelationshipType = index;
      ++index;
    }
    if(lookupDualNodesProperties)
    {
      if(index)
        s << ", ";
      s << relatedDualNodeID;
      queryInfo.indexDualNodeID = index;
      ++index;
    }
    if(dualNodeNeedsTypeInfo)
    {
      if(index)
        s << ", ";
      s << "dualNodes.NodeType";
      queryInfo.indexDualNodeType = index;
      ++index;
    }
    s << " FROM relationships";
    if(nodeTypesFilter.has_value() || nodeNeedsTypeInfo)
      s << " INNER JOIN nodes ON nodes.SYS__ID = relationships." << relatedNodeID;
    if(dualNodeTypesFilter.has_value() || dualNodeNeedsTypeInfo)
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
    
    auto tryFilterTypes = [&](const std::optional<std::set<size_t>>& typesFilter, std::string const& typeAlias)
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

    char*msg{};
    if(auto res = sqlite3_exec(req, [](void *p_queryInfo, int argc, char **argv, char **column) {
      const auto t1 = std::chrono::system_clock::now();

      auto & queryInfo = *static_cast<RelationshipQueryInfo*>(p_queryInfo);
      auto & candidateRow = queryInfo.candidateRows.emplace_back();
      candidateRow.idsAndTypes[0] = IDAndType{
        queryInfo.indexNodeID.has_value() ? argv[*queryInfo.indexNodeID] : std::string{},
        queryInfo.indexNodeType.has_value() ? static_cast<size_t>(std::atoll(argv[*queryInfo.indexNodeType])) : 0ull
      };
      candidateRow.idsAndTypes[1] = IDAndType{
        queryInfo.indexRelationshipID.has_value() ? argv[*queryInfo.indexRelationshipID] : std::string{},
        queryInfo.indexRelationshipType.has_value() ? static_cast<size_t>(std::atoll(argv[*queryInfo.indexRelationshipType])) : 0ull
      };
      candidateRow.idsAndTypes[2] = IDAndType{
        queryInfo.indexDualNodeID.has_value() ? argv[*queryInfo.indexDualNodeID] : std::string{},
        queryInfo.indexDualNodeType.has_value() ? static_cast<size_t>(std::atoll(argv[*queryInfo.indexDualNodeType])) : 0ull
      };
 
      const auto duration = std::chrono::system_clock::now() - t1;
      queryInfo.totalSystemRelationshipCbDuration += duration;
      return 0;
    }, &queryInfo, &msg))
      throw std::logic_error(msg);
  }

  // split nodes, dualNodes, relationships by types
  
  std::unordered_map<size_t /*node type*/, std::unordered_set<ID> /* node ids*/> nodesByTypes;
  std::unordered_map<size_t /*node type*/, std::unordered_set<ID> /* node ids*/> dualNodesByTypes;
  std::unordered_map<size_t /*rel type*/, std::vector<ID> /* rel ids*/> relsByTypes;

  std::unordered_map<ID, std::vector<std::optional<std::string>>> nodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> dualNodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> relProperties;

  const bool nodeOnlyReturnsId = !nodeNeedsTypeInfo && lookupNodesProperties;
  const bool relOnlyReturnsId = !relNeedsTypeInfo && lookupRelsProperties;
  const bool dualNodeOnlyReturnsId = !dualNodeNeedsTypeInfo && lookupDualNodesProperties;

  if(nodeOnlyReturnsId)
  {
    // means we return only the id (sanity check this) and no post filtering occurs.
    const auto countReturnedProps = propertiesNode.size();
    if(countReturnedProps == 0)
      throw std::logic_error("[Unexpected] !nodeNeedsTypeInfo && lookupNodesProperties but has no id property returned.");
    for(const auto & p : propertiesNode)
      if(p.propertyName != idProperty)
        throw std::logic_error("[Unexpected] !nodeNeedsTypeInfo but has some non-id property returned.");
  }
  if(relOnlyReturnsId)
  {
    // means we return only the id (sanity check this) and no post filtering occurs.
    const auto countReturnedProps = propertiesRel.size();
    if(countReturnedProps == 0)
      throw std::logic_error("[Unexpected] !relNeedsTypeInfo && lookupRelsProperties but has no id property returned.");
    for(const auto & p : propertiesRel)
      if(p.propertyName != idProperty)
        throw std::logic_error("[Unexpected] !relNeedsTypeInfo but has some non-id property returned.");
  }
  if(dualNodeOnlyReturnsId)
  {
    // means we return only the id (sanity check this) and no post filtering occurs.
    const auto countReturnedProps = propertiesDualNode.size();
    if(countReturnedProps == 0)
      throw std::logic_error("[Unexpected] !dualNodeNeedsTypeInfo && lookupDualNodesProperties but has no id property returned.");
    for(const auto & p : propertiesDualNode)
      if(p.propertyName != idProperty)
        throw std::logic_error("[Unexpected] !dualNodeNeedsTypeInfo but has some non-id property returned.");
  }

  const auto countNodeReturnedProps = propertiesNode.size();
  const auto countRelReturnedProps = propertiesRel.size();
  const auto countDualNodeReturnedProps = propertiesDualNode.size();
  for(const auto & candidateRow : candidateRows)
  {
    if(nodeOnlyReturnsId)
    {
      // TODO: instead of storing in nodeProperties, when we return results we should construct the vectors then.
      auto res = nodeProperties.try_emplace(candidateRow.idsAndTypes[0].id);
      if(res.second)
        res.first->second.resize(countNodeReturnedProps, candidateRow.idsAndTypes[0].id);
    }
    else if(nodeNeedsTypeInfo && lookupNodesProperties)
      nodesByTypes[candidateRow.idsAndTypes[0].type].insert(candidateRow.idsAndTypes[0].id);

    if(relOnlyReturnsId)
    {
      auto res = relProperties.try_emplace(candidateRow.idsAndTypes[1].id);
      if(res.second)
        res.first->second.resize(countRelReturnedProps, candidateRow.idsAndTypes[1].id);
    }
    else if(relNeedsTypeInfo && lookupRelsProperties)
      relsByTypes[candidateRow.idsAndTypes[1].type].push_back(candidateRow.idsAndTypes[1].id);

    if(dualNodeOnlyReturnsId)
    {
      auto res = dualNodeProperties.try_emplace(candidateRow.idsAndTypes[2].id);
      if(res.second)
        res.first->second.resize(countDualNodeReturnedProps, candidateRow.idsAndTypes[2].id);
    }
    else if(dualNodeNeedsTypeInfo && lookupDualNodesProperties)
      dualNodesByTypes[candidateRow.idsAndTypes[2].type].insert(candidateRow.idsAndTypes[2].id);
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

  auto gatherPropertyValues = [this, &postFilters, &idProperty](const Variable* var,
                                     const auto& elemsByType,
                                     const Element elem,
                                     const std::vector<PropertyKeyName>& strProperties,
                                     std::unordered_map<ID, std::vector<std::optional<std::string>>>& properties)
  {
    bool firstOutter = true;
    std::ostringstream s;

    VariablePostFilters * postFilterForVar{};

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

      if(postFilterForVar && !postFilterForVar->filters.empty())
        if(!toEquivalentSQLFilter(postFilterForVar->filters, m_properties[label], {}, sqlFilter))
          // These items are excluded by the filter.
          continue;
      if(sqlFilter.empty())
      {
        bool hasValidNonIdProperty{};
        std::vector<size_t> indicesValidIDProperties;
        {
          size_t i{};
          for(const auto valid : validProperty)
          {
            if(valid)
            {
              const bool isIDProperty = strProperties[i] == idProperty;
              if(isIDProperty)
                indicesValidIDProperties.push_back(i);
              else
                hasValidNonIdProperty = true;
            }
            ++i;
          }
        }
        if(!hasValidNonIdProperty)
        {
          const auto countProperties = strProperties.size();
          // no property is valid (except id properties), and we don't do any filtering
          // so we manually compute the results.
          for(const auto & id : ids)
          {
            auto & vec = properties[id];
            vec.resize(countProperties);
            for(const auto i : indicesValidIDProperties)
              vec[i] = id;
          }
          continue;
        }
      }
      // At this point a query is needed.
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
      struct QueryData{
        std::chrono::steady_clock::duration& totalPropertyTablesCbDuration;
        std::unordered_map<ID, std::vector<std::optional<std::string>>> & properties;
      } queryData{m_totalPropertyTablesCbDuration, properties};

      if(auto res = sqlite3_exec(queryStr.c_str(), [](void *p_queryData, int argc, char **argv, char **column) {
        const auto t1 = std::chrono::system_clock::now();

        auto & queryData = *static_cast<QueryData*>(p_queryData);
        {
          auto & props = queryData.properties[argv[0]];
          for(int i=1; i<argc; ++i)
          {
            auto * arg = argv[i];
            props.push_back(arg ? std::optional{std::string{arg}} : std::nullopt);
          }
        }

        const auto duration = std::chrono::system_clock::now() - t1;
        queryData.totalPropertyTablesCbDuration += duration;
        return 0;
      }, &queryData, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  };

  if(nodeNeedsTypeInfo && lookupNodesProperties)
    gatherPropertyValues(nodeVar, nodesByTypes, Element::Node, strPropertiesNode, nodeProperties);
  if(dualNodeNeedsTypeInfo && lookupDualNodesProperties)
    gatherPropertyValues(dualNodeVar, dualNodesByTypes, Element::Node, strPropertiesDualNode, dualNodeProperties);
  if(relNeedsTypeInfo && lookupRelsProperties)
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

  for(const auto & candidateRow : candidateRows)
  {
    if(lookupNodesProperties)
    {
      const auto itNodeProperties = nodeProperties.find(candidateRow.idsAndTypes[0].id);
      if(itNodeProperties == nodeProperties.end())
        continue;
      vecValues[0] = &itNodeProperties->second;
    }

    if(lookupRelsProperties)
    {
      const auto itRelProperties = relProperties.find(candidateRow.idsAndTypes[1].id);
      if(itRelProperties == relProperties.end())
        continue;
      vecValues[1] = &itRelProperties->second;
    }

    if(lookupDualNodesProperties)
    {
      const auto itDualNodeProperties = dualNodeProperties.find(candidateRow.idsAndTypes[2].id);
      if (itDualNodeProperties == dualNodeProperties.end())
        continue;
      else
        vecValues[2] = &itDualNodeProperties->second;
    }        
    f(resultOrder,
      vecColumnNames,
      vecValues);
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
    char*msg{};
    if(auto res = sqlite3_exec(req, [](void *p_results, int argc, char **argv, char **column) {
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

int GraphDB::sqlite3_exec(const std::string& queryStr,
                          int (*callback)(void*,int,char**,char**),
                          void * cbParam,
                          char **errmsg)
{
  m_fOnSQLQuery(queryStr);

  const auto t1 = std::chrono::system_clock::now();

  const auto res = ::sqlite3_exec(m_db, queryStr.c_str(), callback, cbParam, errmsg);

  const auto duration = std::chrono::system_clock::now() - t1;
  m_totalSQLQueryExecutionDuration += duration;

  m_fOnSQLQueryDuration(duration);

  return res;
}
