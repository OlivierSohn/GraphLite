// - allow declaring hash indices, comparison indices
// The query planner will chose the right indices.
//
// - use a DB with a mysql backend.
// - one "system" table holds the relationships

#pragma once

#include "sqlite3.h"

#include "CypherAST.h"

#include <string>
#include <chrono>
#include <unordered_map>
#include <unordered_set>


template<typename Index>
struct IndexedTypes
{
  std::optional<Index> getIfExists(std::string const & type) const
  {
    auto it = typeToIndex.find(type);
    if(it == typeToIndex.end())
      return {};
    return it->second;
  }
  const std::string* getIfExists(Index const type) const
  {
    auto it = indexToType.find(type);
    if(it == indexToType.end())
      return {};
    return &it->second;
  }

  void add(Index idx, std::string const & name)
  {
    auto it = typeToIndex.find(name);
    if(it != typeToIndex.end())
      throw std::logic_error("duplicate type");
    typeToIndex[name] = idx;
    indexToType[idx] = name;
  }

  const std::unordered_map<std::string, Index>& getTypeToIndex() const { return typeToIndex; }

private:
  std::unordered_map<std::string, Index> typeToIndex;
  std::unordered_map<Index, std::string> indexToType;
};

using ID = std::string;

enum class Element{
  Node,
  Relationship
};

struct ReturnClauseTerm
{
  // position of the term in the return clause.
  size_t returnClausePosition;

  openCypher::PropertyKeyName propertyName; // TODO support more later...
};


struct GraphDB
{
  using Variable = openCypher::Variable;
  using Expression = openCypher::Expression;
  using SymbolicName = openCypher::SymbolicName;
  using PropertyKeyName = openCypher::PropertyKeyName;
  using TraversalDirection = openCypher::TraversalDirection;
  using ExpressionsByVarAndProperties = openCypher::ExpressionsByVarAndProperties;
  
  // Contains information to order results in the same order as they were specified in the return clause.
  using ResultOrder = std::vector<std::pair<
  unsigned /* i = index into VecValues, VecColumnNames*/,
  unsigned /* j = index into *VecValues[i], *VecColumnNames[i] */>>;
  
  using VecColumnNames = std::vector<const std::vector<PropertyKeyName>*>;
  using VecValues = std::vector<const std::vector<std::optional<std::string>>*>;
  
  using FuncResults = std::function<void(const ResultOrder&, const VecColumnNames&, const VecValues&)>;
  
  using FuncOnSQLQuery = std::function<void(std::string const & sqlQuery)>;
  using FuncOnSQLQueryDuration = std::function<void(const std::chrono::steady_clock::duration)>;
  using FuncOnDBDiagnosticContent = std::function<void(int argc, char **argv, char **column)>;
  
  GraphDB(const FuncOnSQLQuery& fOnSQLQuery, const FuncOnSQLQueryDuration& fOnSQLQueryDuration, const FuncOnDBDiagnosticContent& fOnDiagnostic);
  ~GraphDB();
  
  // Creates a sql table.
  // todo support typed properties.
  void addType(std::string const& typeName,
               bool isNode,
               std::vector<PropertyKeyName> const& properties);
  
  // When doing several inserts, it is best to have a transaction for many inserts.
  // TODO redesign API to remove this.
  void beginTransaction();
  void endTransaction();
  
  ID addNode(const std::string& type,
             const std::vector<std::pair<PropertyKeyName, std::string>>& propValues);
  ID addRelationship(const std::string& type,
                     const ID& originEntity,
                     const ID& destinationEntity,
                     const std::vector<std::pair<PropertyKeyName, std::string>>& propValues);
  
  // The property of entities and relationships that represents their ID.
  // It is a "system" property.
  std::string const & idProperty() const { return m_idProperty; }
  
  // |labels| is the list of possible labels. When empty, all labels are allowed.
  void forEachElementPropertyWithLabelsIn(const Element,
                                          const std::vector<ReturnClauseTerm>& propertyNames,
                                          const std::vector<std::string>& labels,
                                          const std::vector<const Expression*>* filter,
                                          FuncResults& f);
  
  // |nonEquiVarIDPropertyFilter| corresponds to constraints on id properties of _different_ variables.
  void forEachNodeAndRelatedRelationship(const TraversalDirection,
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
                                         FuncResults& f);
  
  // Time to run the SQL queries.
  std::chrono::steady_clock::duration m_totalSQLQueryExecutionDuration{};
  // Time spent in the results callback of the query on the sytem relationship table.
  std::chrono::steady_clock::duration m_totalSystemRelationshipCbDuration{};
  // Time spent in the results callback of the query on the property tables.
  std::chrono::steady_clock::duration m_totalPropertyTablesCbDuration{};

  void print();
private:
  std::string m_idProperty{"SYS__ID"};

  sqlite3* m_db{};
  IndexedTypes<size_t> m_indexedNodeTypes;
  IndexedTypes<size_t> m_indexedRelationshipTypes;
  // key : namedType.
  std::unordered_map<std::string, std::set<PropertyKeyName>> m_properties;
  
  const FuncOnSQLQuery m_fOnSQLQuery;
  const FuncOnSQLQueryDuration m_fOnSQLQueryDuration;
  const FuncOnDBDiagnosticContent m_fOnDiagnostic;

  std::vector<std::string> computeLabels(const Element, const std::vector<std::string>& inputLabels) const;
  std::vector<size_t> labelsToTypeIndices(const Element elem, const std::vector<std::string>& inputLabels) const;
  
  [[nodiscard]]
  bool prepareProperties(const std::string& typeName, const std::vector<std::pair<PropertyKeyName, std::string>>& propValues, std::vector<PropertyKeyName>& propertyNames, std::vector<std::string>& propertyValues);
  
  [[nodiscard]]
  bool findValidProperties(const std::string& typeName, const std::vector<PropertyKeyName>& propNames, std::vector<bool>& valid) const;
  
  void addElement(const std::string& typeName,
                  const ID& id,
                  const std::vector<std::pair<PropertyKeyName, std::string>>& propValues);

  static ResultOrder computeResultOrder(const std::vector<const std::vector<ReturnClauseTerm>*>& vecReturnClauses);

  std::optional<std::set<size_t>> computeTypeFilter(const Element e, std::vector<std::string> const & nodeLabelsStr);
  
  int sqlite3_exec(const std::string& queryStr,
                   int (*callback)(void*,int,char**,char**),
                   void *,
                   char **errmsg);
};

