#include "GraphDBSqlite.h"
#include "Logs.h"
#include "SqlAST.h"

#include <iostream>
#include <filesystem>
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
  
  const bool overwrite = true;
  const auto dbFile = std::filesystem::path("test.sqlite3db");
  if(overwrite)
  {
    std::filesystem::remove(dbFile);
  }

  if(auto res = sqlite3_open(dbFile.string().c_str(), &m_db))
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
      if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
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
      if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
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
    if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
      throw std::logic_error(sqlite3_errstr(res));
  }
  // record type
  {
    std::ostringstream s;
    s << "INSERT INTO namedTypes (NamedType, Kind) Values('"
    << typeName << "', '"
    << (isNode ? "E" : "R")
    << "') RETURNING TypeIdx";
    size_t typeIdx{std::numeric_limits<size_t>::max()};
    char* msg{};
    if(auto res = sqlite3_exec(s.str(), [](void *p_typeIdx, int argc, char **argv, char **column) {
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
    m_properties[typeName].insert(m_idProperty);
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
    if(auto res = sqlite3_exec(s.str(), [](void *p_nodeId, int argc, char **argv, char **column) {
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
      if(auto res = sqlite3_exec(s.str(), [](void *p_countMatches, int argc, char **argv, char **column) {
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
    if(auto res = sqlite3_exec(s.str(), [](void *p_relId, int argc, char **argv, char **column) {
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
  if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
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
      if(auto res = sqlite3_exec(s.str(), diagFunc, this, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    {
      std::ostringstream s;
      s << "PRAGMA table_info('" << name << "')";
      if(auto res = sqlite3_exec(s.str(), diagFunc, this, 0))
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


/*
 The input could be modeled as a vector of

struct
{
  std::unique_ptr<VariableAndReturnedProperties> var;
  std::vector<std::vector<std::string>> labels;
};
 
and std::vector<std::optional<size_t>> patternPositionToVar
 
 which will then be converted to a vector of

struct
{
 std::unique_ptr<VariableAndReturnedProperties> var;
 std::vector<std::optional<std::set<size_t>>> typesFilters;
 bool needsTypeInfo;
 bool lookupProperties;
};
 */


void GraphDB::forEachPath(const std::vector<TraversalDirection>& traversalDirections,
                          const std::map<Variable, std::vector<ReturnClauseTerm>>& variablesI,
                          const std::vector<PathPatternElement>& pathPattern,
                          const ExpressionsByVarAndProperties& allFilters,
                          FuncResults& f)
{
  // Todo : How to efficiently implement TraversalDirection::Any?
  // - Have a reversed version of the relationship in the table
  
  for(const auto & d : traversalDirections)
    if(d == TraversalDirection::Any)
      throw std::logic_error("[Not supported] Undirected relationship in complex path pattern.");
  
  const size_t pathPatternSize{pathPattern.size()};

  // These constraints are only on ids so we can apply them while querying the relationships system table.
  std::vector<const Expression*> idFilters;
  
  // These constraints contain non-id properties so we apply them while querying the non-system relationship/entity tables.
  std::map<Variable, VariablePostFilters> postFilters;
  
  analyzeFilters(allFilters, idFilters, postFilters);
  
  std::map<Variable, VariableInfo> varInfo;

  for(const auto & [var, returnedProperties] : variablesI)
  {
    VariableInfo& info = varInfo[var];
    info.needsTypeInfo = varRequiresTypeInfo(var, returnedProperties, postFilters);
    info.lookupProperties = postFilters.count(var) || !returnedProperties.empty();
  }

  auto pathPatternIndexToElement = [](size_t i)
  {
    return (i % 2) ? Element::Relationship : Element::Node;
  };

  std::map<Variable, Element> varToElement;

  // parallel to pathPattern
  std::vector<std::optional<std::set<size_t>>> nodesRelsTypesFilters;
  nodesRelsTypesFilters.reserve(pathPatternSize);
  {
    size_t i{};
    for(const auto & pattern : pathPattern)
    {
      const Element element = pathPatternIndexToElement(i);
      nodesRelsTypesFilters.push_back(computeTypeFilter(element, pattern.labels));
      if(pattern.var.has_value())
        varToElement[*pattern.var] = element;
      ++i;
    }
  }

  std::map<Variable, size_t> varToVarIdx;
  for(const auto & [var, _] : variablesI)
    varToVarIdx[var] = varToVarIdx.size();

  const size_t countDistinctVariables{variablesI.size()};

  std::vector<Variable> varIdxToVar(countDistinctVariables);
  for(const auto & [var, i] : varToVarIdx)
    varIdxToVar[i] = var;

  // To minimize allocations, we use a "struct of arrays" approach here.
  using CandidateRows = std::vector<std::vector<IDAndType>>;
  // indexed by varToVarIdx[var]
  CandidateRows candidateRows(countDistinctVariables);
  
  // 1. Query relationships system table (with self joins and joins on nodes system table)

  {
    struct RelationshipQueryInfo{
      std::chrono::steady_clock::duration& totalSystemRelationshipCbDuration;
      CandidateRows & candidateRows;
      size_t countDistinctVariables;

      // parallel to 'varIdxToVar'
      std::vector<std::optional<unsigned>> indexIDs;
      // parallel to 'varIdxToVar'
      std::vector<std::optional<unsigned>> indexTypes;
    } queryInfo{m_totalSystemRelationshipCbDuration, candidateRows, countDistinctVariables};
    queryInfo.indexIDs.resize(countDistinctVariables);
    queryInfo.indexTypes.resize(countDistinctVariables);

    std::ostringstream s;

    {
      s << "SELECT ";
      unsigned selectIndex{};
      auto pushSelect = [&](const std::string& columnName)
      {
        if(selectIndex)
          s << ", ";
        s << columnName;
        return selectIndex++;
      };

      std::vector<std::string> nodeJoins;
      std::vector<std::string> relationshipSelfJoins;
      std::vector<std::string> constraints;

      std::map<Variable, std::string> variableToIDField;
      std::map<Variable, std::string> variableToTypeField;

      std::optional<std::string> prevToField;
      std::set<std::string> prevRelationshipIDFields;

      unsigned patternIndex{};
      for(const auto & pathPattern : pathPattern)
      {
        // A relationship can only be traversed once in a given match for a graph pattern.
        // The same restriction doesn’t hold for nodes, which may be re-traversed any number of times in a match.
        const bool varAlreadySeen = pathPattern.var.has_value() && variableToIDField.count(*pathPattern.var);

        // assuming all traveral directions are Forward, the path pattern looks like this:
        // (v0)-[v1]->(v2)-[v3]->(v4)-[v5]->(v6) ...
        //  0    0     0    1     1    2     2    // system relationship join index
        //  0    -     1    -     2    -     3    // system node join index
        unsigned int relJoinIndex = patternIndex ? ((patternIndex-1u) / 2u) : 0u;
        unsigned int nodeJoinIndex = patternIndex / 2u;
        // corresponding select clause:
        // SELECT R0.OriginID, R0.SYS__ID, R0.DestinationID, R1.SYS__ID, R1.DestinationID, R2.SYS__ID, R2.DestinationID,
        //        N0.NodeType, N1.NodeType, N2.NodeType
        // FROM relationships R0, relationships R1, relationships R2
        // INNER JOIN nodes N0 ON N0.SYS__ID = R0.OriginID
        // INNER JOIN nodes N1 ON N1.SYS__ID = R0.DestinationID
        // INNER JOIN nodes N2 ON N2.SYS__ID = R1.DestinationID

        const auto traversalDirection = traversalDirections[relJoinIndex];

        const bool isFirstNode = patternIndex == 0;
        const auto elem = pathPatternIndexToElement(patternIndex);
        const std::string relationshipTableJoinAlias{"R" + std::to_string(relJoinIndex)};

        if(relationshipSelfJoins.size() == relJoinIndex)
          relationshipSelfJoins.push_back("relationships " + relationshipTableJoinAlias);

        std::string columnNameForID(relationshipTableJoinAlias);
        std::optional<std::string> columnNameForType;
        if(elem == Element::Node)
        {
          if(traversalDirection == TraversalDirection::Any)
            throw std::logic_error("[Not supported] Undirected relationship in complex path pattern.");
          const bool isOrigin = isFirstNode != (traversalDirection == TraversalDirection::Backward);
          columnNameForID += isOrigin ? ".OriginID" : ".DestinationID";
          prevToField = columnNameForID;

          if(varAlreadySeen)
            constraints.push_back("( " + columnNameForID + " = " + variableToIDField[*pathPattern.var] + " )" );

          const bool needsTypeField =
          // We have a filter on type
          nodesRelsTypesFilters[patternIndex].has_value() ||
          // The query returns the type
          (pathPattern.var.has_value() && varInfo[*pathPattern.var].needsTypeInfo);

          if(needsTypeField)
          {
            if(pathPattern.var.has_value())
              if(auto it = variableToTypeField.find(*pathPattern.var); it != variableToTypeField.end())
                columnNameForType = it->second;
            if(!columnNameForType.has_value())
            {
              const std::string nodeTableJoinAlias{"N" + std::to_string(nodeJoinIndex)};
              nodeJoins.push_back(" INNER JOIN nodes " + nodeTableJoinAlias + " ON " + nodeTableJoinAlias + ".SYS__ID = " + columnNameForID);
              columnNameForType = nodeTableJoinAlias + ".NodeType";
            }
          }
        }
        else
        {
          columnNameForID += ".SYS__ID";
          columnNameForType = relationshipTableJoinAlias + ".RelationshipType";
          if(varAlreadySeen)
            // Because openCypher only allows paths to traverse a relationship once (see comment on relationship uniqueness above),
            // repeating a variable-length relationship in the same graph pattern will yield no results.
            return;
          if(!prevToField.has_value())
            throw std::logic_error("[Unexpected]");
          if(traversalDirection == TraversalDirection::Any)
            throw std::logic_error("[Not supported] Undirected relationship in complex path pattern.");
          const std::string curFromField = relationshipTableJoinAlias + ((traversalDirection == TraversalDirection::Forward) ? ".OriginID" : ".DestinationID");
          if(*prevToField != curFromField)
            constraints.push_back("(" + *prevToField + " = " + curFromField + ")");
          if(!prevRelationshipIDFields.empty())
          {
            std::string allPrevRelIds;
            bool first = true;
            for(const auto & id : prevRelationshipIDFields)
            {
              if(first)
                first = false;
              else
                allPrevRelIds += ", ";
              allPrevRelIds += id;
            }
            constraints.push_back("(" + columnNameForID + " NOT IN (" + allPrevRelIds + ") )");
          }
          prevRelationshipIDFields.insert(columnNameForID);
        }
        if(pathPattern.var.has_value() && !varAlreadySeen)
        {
          const size_t i = varToVarIdx[*pathPattern.var];
          const auto & info = varInfo[*pathPattern.var];
          if(info.lookupProperties)
            queryInfo.indexIDs[i] = pushSelect(columnNameForID);
          if(info.needsTypeInfo)
          {
            if(!columnNameForType.has_value())
              throw std::logic_error("[Unexpected]");
            queryInfo.indexTypes[i] = pushSelect(*columnNameForType);
          }

          variableToIDField.emplace(*pathPattern.var, columnNameForID);
          if(columnNameForType.has_value())
            variableToTypeField[*pathPattern.var] = *columnNameForType;
        }

        if(const auto & typeFilter = nodesRelsTypesFilters[patternIndex])
        {
          if(!columnNameForType.has_value())
            throw std::logic_error("[Unexpected]");
          // Note: for MATCH (a:Type1)-[]->(a:Type2) since 'a' is used in both node patterns,
          //   it means a must be Type1 AND Type2.
          // In our case (single label per entity) no row will be ever returned.
          constraints.push_back(mkFilterTypesConstraint(*typeFilter, *columnNameForType));
        }
          
        ++patternIndex;
      }

      if(!idFilters.empty())
      {
        std::map<Variable, std::map<PropertyKeyName, std::string>> propertyMappingCypherToSQL;
        for(const auto & [var, idField] : variableToIDField)
          propertyMappingCypherToSQL[var][m_idProperty] = idField;
        std::string sqlFilter;
        if(!toEquivalentSQLFilter(idFilters, {m_idProperty}, propertyMappingCypherToSQL, sqlFilter))
          throw std::logic_error("[Unexpected] Expressions in idFilters are all equi-property with property m_idProperty");
        if(!sqlFilter.empty())
          constraints.push_back("( " + sqlFilter + " )");
      }
      
      s << " FROM";

      {
        bool first = true;
        for(const auto & relationshipSelfJoin : relationshipSelfJoins)
        {
          if(first)
            first = false;
          else
            s << ",";
          s << " " << relationshipSelfJoin;
        }
      }

      for(const auto & nodeJoin : nodeJoins)
        s << nodeJoin;

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

      for(const auto & constraint : constraints)
      {
        addWhereTerm();
        s << constraint;
      }
    }

    char*msg{};
    if(auto res = sqlite3_exec(s.str(), [](void *p_queryInfo, int argc, char **argv, char **column) {
      const auto t1 = std::chrono::system_clock::now();
      
      auto & queryInfo = *static_cast<RelationshipQueryInfo*>(p_queryInfo);
      
      for(size_t i{}, sz = queryInfo.countDistinctVariables; i<sz; ++i)
      {
        const auto & indexID = queryInfo.indexIDs[i];
        const auto & indexType = queryInfo.indexTypes[i];
        if(indexID.has_value() || indexType.has_value())
        {
          queryInfo.candidateRows[i].push_back(IDAndType{
            indexID.has_value() ? argv[*indexID] : std::string{},
            indexType.has_value() ? static_cast<size_t>(std::atoll(argv[*indexType])) : 0ull
          });
        }
      }
      
      const auto duration = std::chrono::system_clock::now() - t1;
      queryInfo.totalSystemRelationshipCbDuration += duration;
      return 0;
    }, &queryInfo, &msg))
      throw std::logic_error(msg);
  }

  // 2. Query the labeled node/entity property tables if needed.

  // split nodes, dualNodes, relationships by types (if needed)
  
  // indexed by varToVarIdx[var]
  std::vector<std::vector<PropertyKeyName>> strPropertiesByVar(countDistinctVariables);
  // indexed by varToVarIdx[var]
  std::vector<std::unordered_map<ID, std::vector<std::optional<std::string>>>> propertiesByVar(countDistinctVariables);

  {
    const auto endElementType = getEndElementType();
    // indexed by element type
    std::vector<std::unordered_set<ID> /* element ids*/> elementsByType(endElementType);
    for(const auto & [var, returnedProperties] : variablesI)
    {
      const size_t i = varToVarIdx[var];
      auto & props = strPropertiesByVar[i];
      for(const auto & rct : returnedProperties)
        props.push_back(rct.propertyName);
      const auto & info = varInfo[var];
      if(info.needsTypeInfo && info.lookupProperties)
      {
        const auto & candidateRow = candidateRows[i];
        for(auto & v : elementsByType)
          v.clear();
        for(const auto & idAndType : candidateRow)
          elementsByType[idAndType.type].insert(idAndType.id);
        // TODO: when we query the same labeled entity/relationship property tables for several variables,
        // instead of doing several queries we should do a single UNION ALL query
        // with an extra column containing the index of the variable.
        gatherPropertyValues(var, elementsByType, varToElement[var], props, postFilters, propertiesByVar[i]);
      }
    }
  }

  // Return results according to callerRows

  VecColumnNames vecColumnNames;
  for(const auto & strProperties: strPropertiesByVar)
    vecColumnNames.push_back(&strProperties);
  
  // indexed by varToVarIdx[var]
  VecValues vecValues(countDistinctVariables);
  std::vector<const std::vector<ReturnClauseTerm>*> vecReturnClauses(countDistinctVariables);
  std::vector<size_t> countReturnedPropsByVar(countDistinctVariables);
  std::vector<std::vector<std::optional<std::string>>> propertyValues(countDistinctVariables);
  std::vector<bool> varOnlyReturnsId(countDistinctVariables);
  std::vector<bool> lookupProperties(countDistinctVariables);

  for(const auto & [var, returnedProperties] : variablesI)
  {
    const size_t i = varToVarIdx[var];
    vecReturnClauses[i] = &returnedProperties;
    countReturnedPropsByVar[i] = returnedProperties.size();
    propertyValues[i] = std::vector<std::optional<std::string>>{returnedProperties.size()};
    vecValues[i] = &propertyValues[i];

    const auto & info = varInfo[var];
    lookupProperties[i] = info.lookupProperties;
    const bool onlyReturnsID = !info.needsTypeInfo && info.lookupProperties;
    varOnlyReturnsId[i] = onlyReturnsID;
    if(onlyReturnsID)
    {
      // means we return only the id (sanity check this) and no post filtering occurs.
      if(returnedProperties.empty())
        throw std::logic_error("[Unexpected] !nodeNeedsTypeInfo && lookupNodesProperties but has no id property returned.");
      for(const auto & p : returnedProperties)
        if(p.propertyName != m_idProperty)
          throw std::logic_error("[Unexpected] !nodeNeedsTypeInfo but has some non-id property returned.");
    }
  }
  
  const ResultOrder resultOrder = computeResultOrder(vecReturnClauses);
  
  size_t countRows{};
  for(const auto & candidateRow : candidateRows)
    countRows = std::max(countRows, candidateRow.size());

  std::vector<Variable> orderedVariables;
  orderedVariables.reserve(countDistinctVariables);
  for(size_t i{}; i<countDistinctVariables; ++i)
    orderedVariables.push_back(varIdxToVar[i]);

  for(size_t row{}; row<countRows; ++row)
  {
    for(size_t i{}; i<countDistinctVariables; ++i)
    {
      if(lookupProperties[i])
      {
        if(varOnlyReturnsId[i])
        {
          auto & propValues = propertyValues[i];
          std::fill(propValues.begin(), propValues.end(), candidateRows[i][row].id);
        }
        else
        {
          const auto & properties = propertiesByVar[i];
          const auto itNodeProperties = properties.find(candidateRows[i][row].id);
          if(itNodeProperties == properties.end())
            // The candidate has been discarded by one of the queries on labeled node/entity property tables.
            goto nextRow;
          vecValues[i] = &itNodeProperties->second;
        }
      }
    }
    f(resultOrder,
      orderedVariables,
      vecColumnNames,
      vecValues);
  nextRow:;
  }
}


void GraphDB::forEachNodeAndRelatedRelationship(const TraversalDirection traversalDirection,
                                                const VariableAndReturnedProperties* nodeVar,
                                                const VariableAndReturnedProperties* relVar,
                                                const VariableAndReturnedProperties* dualNodeVar,
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
      forEachNodeAndRelatedRelationship(td, nodeVar, relVar, dualNodeVar,
                                        nodeLabelsStr, relLabelsStr, dualNodeLabelsStr,
                                        allFilters, f);
    return;
  }

  // These constraints are only on ids so we can apply them while querying the relationships system table.
  std::vector<const Expression*> idFilters;
  
  // These constraints contain non-id properties so we apply them while querying the non-system relationship/entity tables.
  std::map<Variable, VariablePostFilters> postFilters;
  
  analyzeFilters(allFilters, idFilters, postFilters);

  // Whether to return the type information in the relationships system table query.
  const bool nodeNeedsTypeInfo = nodeVar && varRequiresTypeInfo(nodeVar->var, nodeVar->returnedProperties, postFilters);
  const bool relNeedsTypeInfo = relVar && varRequiresTypeInfo(relVar->var, relVar->returnedProperties, postFilters);
  const bool dualNodeNeedsTypeInfo = dualNodeVar && varRequiresTypeInfo(dualNodeVar->var, dualNodeVar->returnedProperties, postFilters);

  // if the node/dualNode/rel is not post filtered, and no property is returned for the node/dualNode/rel,
  // we don't lookup the properties.
  const bool lookupNodesProperties = nodeVar && (postFilters.count(nodeVar->var) || !nodeVar->returnedProperties.empty());
  const bool lookupRelsProperties = relVar && (postFilters.count(relVar->var) || !relVar->returnedProperties.empty());
  const bool lookupDualNodesProperties = dualNodeVar && (postFilters.count(dualNodeVar->var) || !dualNodeVar->returnedProperties.empty());

  const std::optional<std::set<size_t>> nodeTypesFilter = computeTypeFilter(Element::Node, nodeLabelsStr);
  const std::optional<std::set<size_t>> relTypesFilter = computeTypeFilter(Element::Relationship, relLabelsStr);
  const std::optional<std::set<size_t>> dualNodeTypesFilter = computeTypeFilter(Element::Node, dualNodeLabelsStr);
  
  const std::string relatedNodeID = (traversalDirection == TraversalDirection::Forward) ? "OriginID" : "DestinationID";
  const std::string relatedDualNodeID = (traversalDirection == TraversalDirection::Backward) ? "OriginID" : "DestinationID";

  std::map<Variable, std::map<PropertyKeyName, std::string>> propertyMappingCypherToSQL;
  // Note: when *nodeVar == *dualNodeVar, the code below overwrites relatedNodeID with relatedDualNodeID
  // which is OK because when *nodeVar == *dualNodeVar we add a query constraint "relatedDualNodeID = relatedNodeID".
  if(nodeVar)
    propertyMappingCypherToSQL[nodeVar->var][m_idProperty] = relatedNodeID;
  if(dualNodeVar)
    propertyMappingCypherToSQL[dualNodeVar->var][m_idProperty] = relatedDualNodeID;
  if(relVar)
    propertyMappingCypherToSQL[relVar->var][m_idProperty] = "relationships.SYS__ID";
  
  // 1. Query the system table of relationships (and JOIN the system table of nodes up to 2 times) to gather candidate rows

  // We filter relationships based on
  // - nodeLabelsStr
  // - relLabelsStr
  // - dualNodeLabelsStr

  // We also filter on ids of rel, node, dualNode when possible.
  //
  // For example when ther WHERE clause contains:
  //
  //   id(n) in <IDS> AND n.weight > 3
  //
  // we filter on IDS now and will filter on the weight property later.
  // But in this case:
  //
  //   id(n) in <IDS> OR n.weight > 3
  //
  // We cannot filter on IDS because some nodes with n.weight > 3 may not have their ids in IDS.
  //
  // TODO: To handle this case we have 2 possibilities:
  // - (A) ignore the constraint when querying the relationships system table,
  //       take this constraint into account only during Post filtering (when querying labeled nodes/relationships property tables)
  // - (B) first run queries against labeled nodes/relationships property tables to gather ids for which n.weight > 3,
  //       then use this information to specify the constraint when querying the relationships system table.
  //
  // We could dynamically chose between (A) and (B) by comparing the count of scanned rows.
  //
  // Note on estimating the count of scanned rows:
  //   if there are other filters on ids (on the other node of the pattern, or on the relationship)
  //   the count of rows scanned in the relationships system table will be reduced.
  
  // TODO: try having the node & dualNode type info duplicated inline in the relationship table
  //   to avoid the JOIN on the system nodes table, and measure perf improvement.

  // TODO: optionally use pagination to handle very large graphs.
  // https://stackoverflow.com/a/65647734/3940228
  // This will be needed when the cypher query has a LIMIT clause.

  struct CandidateRow{
    // 0 : node, 1 : relationship, 2 : dual node
    std::array<IDAndType, 3> idsAndTypes;
  };
  using CandidateRows = std::vector<CandidateRow>;
  
  CandidateRows candidateRows;

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
      if(!toEquivalentSQLFilter(idFilters, {m_idProperty}, propertyMappingCypherToSQL, sqlFilter))
        throw std::logic_error("Should never happen: expressions in *FilterIDs are all equi-property with property m_idProperty");
      if(!sqlFilter.empty())
      {
        addWhereTerm();
        s << "( " << sqlFilter << " )";
      }
    }
    
    if(nodeVar && dualNodeVar && (nodeVar->var == dualNodeVar->var))
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

    char*msg{};
    if(auto res = sqlite3_exec(s.str(), [](void *p_queryInfo, int argc, char **argv, char **column) {
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
  
  // 2. Query the labeled node/entity property tables if needed.

  // split nodes, dualNodes, relationships by types (if needed)
  
  const auto endElementType = getEndElementType();

  // indexed by element type
  std::vector<std::unordered_set<ID> /* node ids*/> nodesByTypes(endElementType);
  std::vector<std::unordered_set<ID> /* node ids*/> dualNodesByTypes(endElementType);
  std::vector<std::vector<ID> /* rel ids*/> relsByTypes(endElementType);

  for(const auto & candidateRow : candidateRows)
  {
    if(nodeNeedsTypeInfo && lookupNodesProperties)
      nodesByTypes[candidateRow.idsAndTypes[0].type].insert(candidateRow.idsAndTypes[0].id);

    if(relNeedsTypeInfo && lookupRelsProperties)
      relsByTypes[candidateRow.idsAndTypes[1].type].push_back(candidateRow.idsAndTypes[1].id);

    if(dualNodeNeedsTypeInfo && lookupDualNodesProperties)
      dualNodesByTypes[candidateRow.idsAndTypes[2].type].insert(candidateRow.idsAndTypes[2].id);
  }

  // Get needed property values of nodes, dual nodes and relationships
  //   and filter.

  std::vector<PropertyKeyName> strPropertiesNode;
  if(nodeVar)
    for(const auto & rct : nodeVar->returnedProperties)
      strPropertiesNode.push_back(rct.propertyName);

  std::vector<PropertyKeyName> strPropertiesDualNode;
  if(dualNodeVar)
    for(const auto & rct : dualNodeVar->returnedProperties)
      strPropertiesDualNode.push_back(rct.propertyName);

  std::vector<PropertyKeyName> strPropertiesRel;
  if(relVar)
    for(const auto & rct : relVar->returnedProperties)
      strPropertiesRel.push_back(rct.propertyName);

  std::unordered_map<ID, std::vector<std::optional<std::string>>> nodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> dualNodeProperties;
  std::unordered_map<ID, std::vector<std::optional<std::string>>> relProperties;

  // TODO: when we query the same property tables for node and dual node, instead of doing 2 queries we should do a single UNION ALL query
  // with an extra column saying whether the row is for node or dualNode.
  if(nodeNeedsTypeInfo && lookupNodesProperties)
    gatherPropertyValues(nodeVar->var, nodesByTypes, Element::Node, strPropertiesNode, postFilters, nodeProperties);
  if(dualNodeNeedsTypeInfo && lookupDualNodesProperties)
    // TODO: fix perf bug: if *nodeVar == *dualNodeVar, then we are doing redundant computations
    gatherPropertyValues(dualNodeVar->var, dualNodesByTypes, Element::Node, strPropertiesDualNode, postFilters, dualNodeProperties);
  if(relNeedsTypeInfo && lookupRelsProperties)
    gatherPropertyValues(relVar->var, relsByTypes, Element::Relationship, strPropertiesRel, postFilters, relProperties);

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
  
  vecReturnClauses.push_back(nodeVar? &nodeVar->returnedProperties : nullptr);
  vecReturnClauses.push_back(relVar? &relVar->returnedProperties : nullptr);
  vecReturnClauses.push_back(dualNodeVar? &dualNodeVar->returnedProperties : nullptr);

  const auto countNodeReturnedProps = nodeVar? nodeVar->returnedProperties.size() : 0ull;
  const auto countDualNodeReturnedProps = dualNodeVar? dualNodeVar->returnedProperties.size() : 0ull;
  const auto countRelReturnedProps = relVar? relVar->returnedProperties.size() : 0ull;
  
  std::vector<std::optional<std::string>> nodePropertyValues(countNodeReturnedProps);
  std::vector<std::optional<std::string>> relPropertyValues(countRelReturnedProps);
  std::vector<std::optional<std::string>> dualNodePropertyValues(countDualNodeReturnedProps);

  vecValues.push_back(&nodePropertyValues);
  vecValues.push_back(&relPropertyValues);
  vecValues.push_back(&dualNodePropertyValues);

  const ResultOrder resultOrder = computeResultOrder(vecReturnClauses);

  const bool nodeOnlyReturnsId = !nodeNeedsTypeInfo && lookupNodesProperties;
  const bool relOnlyReturnsId = !relNeedsTypeInfo && lookupRelsProperties;
  const bool dualNodeOnlyReturnsId = !dualNodeNeedsTypeInfo && lookupDualNodesProperties;
  
  if(nodeOnlyReturnsId)
  {
    // means we return only the id (sanity check this) and no post filtering occurs.
    if(countNodeReturnedProps == 0)
      throw std::logic_error("[Unexpected] !nodeNeedsTypeInfo && lookupNodesProperties but has no id property returned.");
    for(const auto & p : nodeVar->returnedProperties)
      if(p.propertyName != m_idProperty)
        throw std::logic_error("[Unexpected] !nodeNeedsTypeInfo but has some non-id property returned.");
  }
  if(relOnlyReturnsId)
  {
    // means we return only the id (sanity check this) and no post filtering occurs.
    if(countRelReturnedProps == 0)
      throw std::logic_error("[Unexpected] !relNeedsTypeInfo && lookupRelsProperties but has no id property returned.");
    for(const auto & p : relVar->returnedProperties)
      if(p.propertyName != m_idProperty)
        throw std::logic_error("[Unexpected] !relNeedsTypeInfo but has some non-id property returned.");
  }
  if(dualNodeOnlyReturnsId)
  {
    // means we return only the id (sanity check this) and no post filtering occurs.
    if(countDualNodeReturnedProps == 0)
      throw std::logic_error("[Unexpected] !dualNodeNeedsTypeInfo && lookupDualNodesProperties but has no id property returned.");
    for(const auto & p : dualNodeVar->returnedProperties)
      if(p.propertyName != m_idProperty)
        throw std::logic_error("[Unexpected] !dualNodeNeedsTypeInfo but has some non-id property returned.");
  }

  std::vector<Variable> orderedVariables;
  orderedVariables.push_back(nodeVar ? nodeVar->var : Variable{});
  orderedVariables.push_back(relVar ? relVar->var : Variable{});
  orderedVariables.push_back(dualNodeVar ? dualNodeVar->var : Variable{});

  for(const auto & candidateRow : candidateRows)
  {
    if(lookupNodesProperties)
    {
      if(nodeOnlyReturnsId)
        std::fill(nodePropertyValues.begin(), nodePropertyValues.end(), candidateRow.idsAndTypes[0].id);
      else
      {
        const auto itNodeProperties = nodeProperties.find(candidateRow.idsAndTypes[0].id);
        if(itNodeProperties == nodeProperties.end())
          continue;
        vecValues[0] = &itNodeProperties->second;
      }
    }

    if(lookupRelsProperties)
    {
      if(relOnlyReturnsId)
        std::fill(relPropertyValues.begin(), relPropertyValues.end(), candidateRow.idsAndTypes[1].id);
      else
      {
        const auto itRelProperties = relProperties.find(candidateRow.idsAndTypes[1].id);
        if(itRelProperties == relProperties.end())
          continue;
        vecValues[1] = &itRelProperties->second;
      }
    }

    if(lookupDualNodesProperties)
    {
      if(dualNodeOnlyReturnsId)
        std::fill(dualNodePropertyValues.begin(), dualNodePropertyValues.end(), candidateRow.idsAndTypes[2].id);
      else{
        const auto itDualNodeProperties = dualNodeProperties.find(candidateRow.idsAndTypes[2].id);
        if (itDualNodeProperties == dualNodeProperties.end())
          continue;
        else
          vecValues[2] = &itDualNodeProperties->second;
      }
    }
    f(resultOrder,
      orderedVariables,
      vecColumnNames,
      vecValues);
  }
}

void GraphDB::forEachElementPropertyWithLabelsIn(const Variable & var,
                                                 const Element elem,
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
    Results(ResultOrder&&ro, VecColumnNames&& vecColumnNames, const FuncResults& func, const Variable& var)
    : m_resultsOrder(std::move(ro))
    , m_vecColumnNames(std::move(vecColumnNames))
    , m_f(func)
    , m_vecValues{&m_values}
    , m_orderedVariables{var}
    {
      m_values.resize(m_vecColumnNames[0]->size());
    }

    const ResultOrder m_resultsOrder;
    const VecColumnNames m_vecColumnNames;
    std::vector<std::optional<std::string>> m_values;
    const FuncResults& m_f;
    const std::vector<Variable> m_orderedVariables;
    const std::vector<const std::vector<std::optional<std::string>>*> m_vecValues;
  } results {
    computeResultOrder({&returnClauseTerms}),
    {&propertyNames},
    f,
    var
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
      results.m_f(results.m_resultsOrder, results.m_orderedVariables, results.m_vecColumnNames, results.m_vecValues);
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
    return acc + (pVecReturnClauseTerms ? pVecReturnClauseTerms->size() : 0ull);
  });
  
  ResultOrder resultOrder(resultsSize);
  
  for(size_t i=0, szI = vecReturnClauses.size(); i<szI; ++i)
  {
    if(vecReturnClauses[i])
    {
      const auto & properties = *vecReturnClauses[i];
      for(size_t j=0, szJ = properties.size(); j<szJ; ++j)
      {
        const auto& p = properties[j];
        resultOrder[p.returnClausePosition] = {i, j};
      }
    }
  }
  
  return resultOrder;
}

void GraphDB::analyzeFilters(const ExpressionsByVarAndProperties& allFilters,
                             std::vector<const Expression*>& idFilters,
                             std::map<Variable, VariablePostFilters>& postFilters) const
{
  for(const auto & [varsAndProperties, expressions] : allFilters)
  {
    if(varsAndProperties.size() >= 2)
    {
      // 'expressions' use 2 or more variables
      
      if(countPropertiesNotEqual(m_idProperty, varsAndProperties) > 0)
        // At least one non-id property is used.
        // We could support this in the future by evaluating these expressions at the end
        // of this function when returning the results.
        throw std::logic_error("[Not supported] A non-equi-var expression is using non-id properties.");
      
      // only id properties are used so we will use these expressions to filter the system relationships table.
      idFilters.insert(idFilters.end(), expressions.begin(), expressions.end());
    }
    else if(varsAndProperties.size() == 1)
    {
      // 'expressions' uses a single variable
      if(countPropertiesNotEqual(m_idProperty, varsAndProperties) > 0)
      {
        // At least one non-id property is used.
        VariablePostFilters & postFiltersForVar = postFilters[varsAndProperties.begin()->first];
        for(const PropertyKeyName & property : varsAndProperties.begin()->second)
          postFiltersForVar.properties.insert(property);
        postFiltersForVar.filters.insert(postFiltersForVar.filters.end(), expressions.begin(), expressions.end());
      }
      else if(varsAndProperties.begin()->second.count(m_idProperty))
      {
        // only id properties are used
        idFilters.insert(idFilters.end(), expressions.begin(), expressions.end());
      }
      else
        throw std::logic_error("[Unexpected] A filter expression has no property.");
    }
    else
      throw std::logic_error("[Unexpected] A filter expression has no variable.");
  }
}

bool GraphDB::varRequiresTypeInfo(const Variable& var,
                                  const std::vector<ReturnClauseTerm>& returnedProperties,
                                  const std::map<Variable, VariablePostFilters>& postFilters) const
{
  // If the return clause contains some non-id properties,
  // we need to know the type of the element because
  // we may need to query property tables to find the value of these properties
  // (unless these properties are not valid for the property table but this will be checked later,
  //  once we know the type of the element)
  for(const auto & p : returnedProperties)
    if(p.propertyName != m_idProperty)
      return true;
  
  // If there are some constraints on non-id properties,
  // we will have to query the property tables to know if the element is discarded.
  if(auto it = postFilters.find(var); it != postFilters.end())
  {
    // By construction of post filters, a non-id property will be used by this post-filter.
    
    // Sanity check
    bool hasNonIdProperty{};
    for(const auto & prop : it->second.properties)
      if(prop != m_idProperty)
      {
        hasNonIdProperty = true;
        break;
      }
    if(!hasNonIdProperty)
      throw std::logic_error("[Unexpected] A post-filter has no non-id property.");
    
    return true;
  }

  return false;
}

std::string GraphDB::mkFilterTypesConstraint(const std::set<size_t>& typesFilter, std::string const& typeColumn)
{
  std::ostringstream s;
  s << " " << typeColumn << " IN (";
  bool first = true;
  for(const auto typeIdx : typesFilter)
  {
    if(first)
      first = false;
    else
      s << ",";
    s << typeIdx;
  }
  s << ")";
  return s.str();
}

// TODO: The UNION ALL on different types only works because all properties have the same type (int),
// but in the future this will likely break.
// For example with entity types
// - Person, properties: age(int)
// - BottleOfWine, properties: age(string)
// In this query, age is either an int property or a string property:
// MATCH (a)-[r]->(b) WHERE id(b) IN (...) RETURN a.age
// We should probably use different queries instead of a UNION ALL approach then.
template<typename ElementIDsContainer>
void GraphDB::gatherPropertyValues(const Variable& var,
                                   // indexed by element type
                                   const std::vector<ElementIDsContainer>& elemsByType,
                                   const Element elem,
                                   const std::vector<PropertyKeyName>& strProperties,
                                   const std::map<Variable, VariablePostFilters>& postFilters,
                                   std::unordered_map<ID, std::vector<std::optional<std::string>>>& properties) const
{
  bool firstOutter = true;
  std::ostringstream s;
  
  const VariablePostFilters * postFilterForVar{};
  
  if(const auto it = postFilters.find(var); it != postFilters.end())
    postFilterForVar = &it->second;

  for(size_t typeIdx{}, sz = elemsByType.size(); typeIdx < sz; ++typeIdx)
  {
    const auto & ids = elemsByType[typeIdx];
    if(ids.empty())
      continue;
    // typeIdx is guaranteed to be an existing type so we know getIfExists will return a value.
    const auto label = (elem == Element::Node)
    ? *m_indexedNodeTypes.getIfExists(typeIdx)
    : *m_indexedRelationshipTypes.getIfExists(typeIdx);
    
    std::vector<bool> validProperty;
    if(!findValidProperties(label, strProperties, validProperty))
      throw std::logic_error("[Unexpected] Label does not exist.");
    std::string sqlFilter{};
    
    if(postFilterForVar && !postFilterForVar->filters.empty())
    {
      auto it = m_properties.find(label);
      if(it == m_properties.end())
        throw std::logic_error("[Unexpected] Label not found in properties.");
      if(!toEquivalentSQLFilter(postFilterForVar->filters, it->second, {}, sqlFilter))
        // These items are excluded by the filter.
        continue;
    }
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
            const bool isIDProperty = strProperties[i] == m_idProperty;
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
}

size_t GraphDB::getEndElementType() const
{
  auto max1 = m_indexedRelationshipTypes.getMaxIndex();
  auto max2 = m_indexedNodeTypes.getMaxIndex();
  if(!max1.has_value() && !max2.has_value())
    return 0ull;
  size_t end = 0;
  end = std::max(end, max1.value_or(std::numeric_limits<size_t>::lowest()));
  end = std::max(end, max2.value_or(std::numeric_limits<size_t>::lowest()));
  return end + 1ull;
}

int GraphDB::sqlite3_exec(const std::string& queryStr,
                          int (*callback)(void*,int,char**,char**),
                          void * cbParam,
                          char **errmsg) const
{
  m_fOnSQLQuery(queryStr);

  const auto t1 = std::chrono::system_clock::now();

  const auto res = ::sqlite3_exec(m_db, queryStr.c_str(), callback, cbParam, errmsg);

  const auto duration = std::chrono::system_clock::now() - t1;
  m_totalSQLQueryExecutionDuration += duration;

  m_fOnSQLQueryDuration(duration);

  return res;
}
