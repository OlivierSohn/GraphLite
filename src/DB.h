// - allow declaring hash indices, comparison indices
// The query planner will chose the right indices.
//
// - use a DB with a mysql backend.
// - one "system" table holds the relationships

#pragma once

#include <string>
#include <set>
#include <unordered_map>

#include "arrow/api.h"

struct RowIdx
{
  RowIdx(uint64_t i)
  : id(i)
  {}
  
  uint64_t unsafeGet() const { return id; }
private:
  uint64_t id;
};

template<typename T>
struct Column
{
  // indexed by RowIdx
  std::vector<T> values;
};

// Unique among nodes.
// Unique among relationships.
// Not unique among nodes + relationships.
struct ID
{
  ID(uint64_t i)
  : id(i)
  {}

  uint64_t unsafeGet() const { return id; }
private:
  uint64_t id;
};


namespace std
{
template<>
struct hash<ID>
{
  size_t operator ()(ID const & id) const noexcept
  {
    return id.unsafeGet();
  }
};
}


template<typename Index>
struct IndexedTypes
{
  Index getIndexOfType(std::string const & type)
  {
    if(auto i = getIfExists(type))
      return *i;
    Index res{static_cast<uint32_t>(typeToIndex.size())};
    typeToIndex.emplace(type, res);
    types.push_back(type);
    return res;
  }

  std::optional<Index> getIfExists(std::string const & type) const
  {
    auto it = typeToIndex.find(type);
    if(it == typeToIndex.end())
      return {};
    return it->second;
  }
  
  const std::set<Index> all() const {
    std::set<Index> s;
    for(uint32_t i=0, sz = static_cast<uint32_t>(types.size()); i<sz; ++i)
      s.emplace(Index{i});
    return s;
  }

private:
  std::unordered_map<std::string, Index> typeToIndex;
  // indexed by Index
  std::vector<std::string> types;
};


struct PropertyIdx
{
  PropertyIdx(uint32_t i)
  : propertyIdx(i)
  {}
  
  uint32_t unsafeGet() const { return propertyIdx; }
  
  auto operator<=>(const PropertyIdx&) const = default;
private:
  uint32_t propertyIdx;
};


struct Table
{
  RowIdx addElement(const ID id);

  void setElementProperty(const RowIdx rowIdx, const std::string& name, const double value);

  template<typename F>
  void forEachElement(F&&f);

  template<typename F>
  void forEachElementGetPropertyValue(const std::string& propertyName, F&&f);

private:
  IndexedTypes<PropertyIdx> propertyNames;
  
  // indexed by RowIdx
  std::vector<ID> ids;

  // indexed by PropertyIdx
  std::vector<Column<double>> columns;
};


struct RelationshipTypeIdx
{
  RelationshipTypeIdx(uint32_t i)
  : relTypeIdx(i)
  {}

  uint32_t unsafeGet() const { return relTypeIdx; }

  auto operator<=>(const RelationshipTypeIdx&) const = default;
private:
  uint32_t relTypeIdx;
};

struct NodeTypeIdx
{
  NodeTypeIdx(uint32_t i)
  : nodeTypeIdx(i)
  {}

  uint32_t unsafeGet() const { return nodeTypeIdx; }

  auto operator<=>(const NodeTypeIdx&) const = default;
private:
  uint32_t nodeTypeIdx;
};

struct NodeKey{
  NodeTypeIdx nodeTypeIdx;
  RowIdx rowIdx;
};
struct RelationshipKey{
  RelationshipTypeIdx relationshipTypeIdx;
  RowIdx rowIdx;
};

struct RelationshipIDAndKey
{
  ID relId;
  RelationshipKey relKey;
};


struct DB
{
  DB()
  {}
  
  ID addNode(const std::string& type);

  bool setNodeProperty(ID, std::string const& name, const double value);
  
  arrow::Status writeToDisk();

  template<typename F>
  void forEachNodeWithLabelsIn(const std::vector<std::string>&, F&&f);
  template<typename F>
  void forEachNodePropertyWithLabelsIn(const std::string& propertyName, const std::vector<std::string>&, F&&f);
private:
  std::vector<NodeKey> nodeIdToKey;
  std::vector<RelationshipKey> relationshipIdToKey;
  
  std::unordered_multimap<uint64_t, RelationshipIDAndKey> originNodeIdToRelationshipIds;
  std::unordered_multimap<uint64_t, RelationshipIDAndKey> destinationNodeIdToRelationshipIds;
  
  IndexedTypes<NodeTypeIdx> nodeTypes;
  IndexedTypes<RelationshipTypeIdx> relationshipTypes;
  
  // indexed by NodeTypeIdx
  std::vector<Table> nodesTables;
  // indexed by RelationshipTypeIdx
  std::vector<Table> relationshipsTables;
  
  NodeTypeIdx addNodeType(const std::string& type);
  RelationshipTypeIdx addRelationshipType(const std::string& type);
  
  static arrow::Status writeTable(std::string const& prefixName, const arrow::Table& table);
};



template<typename F>
void DB::forEachNodeWithLabelsIn(const std::vector<std::string>& labels, F&&f)
{
  std::set<NodeTypeIdx> nodeTypeIdxs;
  if(labels.empty())
    nodeTypeIdxs = nodeTypes.all();
  else
    for(const auto & label : labels)
      if(auto l = nodeTypes.getIfExists(label))
        nodeTypeIdxs.emplace(*l);
  for(const auto typeIdx : nodeTypeIdxs)
  {
    auto & table = nodesTables[typeIdx.unsafeGet()];
    table.forEachElement(std::forward<F&&>(f));
  }
}

template<typename F>
void DB::forEachNodePropertyWithLabelsIn(const std::string& propertyName, const std::vector<std::string>& labels, F&&f)
{
  std::set<NodeTypeIdx> nodeTypeIdxs;
  if(labels.empty())
    nodeTypeIdxs = nodeTypes.all();
  else
    for(const auto & label : labels)
      if(auto l = nodeTypes.getIfExists(label))
        nodeTypeIdxs.emplace(*l);
  for(const auto typeIdx : nodeTypeIdxs)
  {
    auto & table = nodesTables[typeIdx.unsafeGet()];
    table.forEachElementGetPropertyValue(propertyName, std::forward<F&&>(f));
  }
}

template<typename F>
void Table::forEachElement(F&&f)
{
  const auto sz = ids.size();
  for(size_t i=0; i<sz; ++i)
    f(ids[i]);
}

template<typename F>
void Table::forEachElementGetPropertyValue(const std::string& propertyName, F&&f)
{
  const auto propertyIdx = propertyNames.getIfExists(propertyName);
  Column<double> * col{};
  if(propertyIdx.has_value())
    col = &columns[propertyIdx->unsafeGet()];
  const auto sz = ids.size();
  const auto countValues = col ? col->values.size() : 0ull;
  for(size_t i=0; i<sz; ++i)
  {
    if(i < countValues)
      f(ids[i], col->values[i]);
    else
      f(ids[i], {});
  }
}
