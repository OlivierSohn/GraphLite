// - allow declaring hash indices, comparison indices
// The query planner will chose the right indices.
//
// - use a DB with a mysql backend.
// - one "system" table holds the relationships

#pragma once

#include "sqlite3.h"

#include "CypherAST.h"

#include <string>
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

// Todo: move this notion to AST for simplicity.
struct ReturnClauseTerm
{
  // position of the term in the return clause.
  size_t returnClausePosition;

  std::string propertyName; // TODO support more later...
};

struct SimplePartialComparisonExpression{
  Comparison comp;
  Literal literal;
};
struct PropertyAndPCE {
  std::string property;
  SimplePartialComparisonExpression pce;
};

struct DB
{
  DB(bool printSQLRequests);
  ~DB();
  
  // Creates a sql table.
  // todo support typed properties.
  void addType(std::string const& typeName, bool isNode, std::vector<std::string> const& properties);
  
  ID addNode(const std::string& type, const std::vector<std::pair<std::string, std::string>>& propValues);
  ID addRelationship(const std::string& type, const ID& originEntity, const ID& destinationEntity, const std::vector<std::pair<std::string, std::string>>& propValues);
  
  // The property of entities and relationships that represents their ID.
  // It is a "system" property.
  std::string const & idProperty() const { return m_idProperty; }
  
  using FuncProp = std::function<void(int argc, char **argv, char **column)>;
  
  using FuncProp2 = std::function<void(const std::vector<std::optional<std::string>>& nodeProperties,
                                       const std::vector<std::optional<std::string>>& relationshipProperties,
                                       const std::vector<std::optional<std::string>>& dualNodeProperties,
                                       const std::vector<std::string>& nodePropertiesNames,
                                       const std::vector<std::string>& relationshipPropertiesNames,
                                       const std::vector<std::string>& dualNodePropertiesNames)>;

  // |labels| is the list of possible labels. When empty, all labels are allowed.
  void forEachElementPropertyWithLabelsIn(const Element,
                                          const std::vector<ReturnClauseTerm>& propertyNames,
                                          const std::vector<std::string>& labels,
                                          const std::optional<PropertyAndPCE>& filter,
                                          FuncProp&f);

  void forEachNodeAndRelatedRelationship(const TraversalDirection,
                                         const std::vector<ReturnClauseTerm>& propertiesNode,
                                         const std::vector<ReturnClauseTerm>& propertiesRel,
                                         const std::vector<ReturnClauseTerm>& propertiesDualNode,
                                         const std::vector<std::string>& nodeLabelsStr,
                                         const std::vector<std::string>& relLabelsStr,
                                         const std::vector<std::string>& dualNodeLabelsStr,
                                         const std::optional<PropertyAndPCE>& nodeFilter,
                                         const std::optional<PropertyAndPCE>& relFilter,
                                         const std::optional<PropertyAndPCE>& dualNodeFilter,
                                         FuncProp2&f);

  void print();
private:
  std::string m_idProperty{"SYS__ID"};
  
  bool m_printSQLRequests;
  
  sqlite3* m_db{};
  IndexedTypes<size_t> m_indexedNodeTypes;
  IndexedTypes<size_t> m_indexedRelationshipTypes;
  // key : namedType.
  std::unordered_map<std::string, std::unordered_set<std::string>> m_properties;
  
  std::vector<std::string> computeLabels(const Element, const std::vector<std::string>& inputLabels) const;
  std::vector<size_t> labelsToTypeIndices(const Element elem, const std::vector<std::string>& inputLabels) const;

  [[nodiscard]]
  bool prepareProperties(const std::string& typeName, const std::vector<std::pair<std::string, std::string>>& propValues, std::vector<std::string>& propertyNames, std::vector<std::string>& propertyValues);
  
  [[nodiscard]]
  bool findValidProperties(const std::string& typeName, const std::vector<std::string>& propNames, std::vector<bool>& valid) const;
  
  void addElement(const std::string& typeName, const ID& id, const std::vector<std::pair<std::string, std::string>>& propValues);

  void printReq(const std::string& req) const;
};

