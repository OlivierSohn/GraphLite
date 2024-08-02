// - allow declaring hash indices, comparison indices
// The query planner will chose the right indices.
//
// - use a DB with a mysql backend.
// - one "system" table holds the relationships

#pragma once

#include "sqlite3.h"

#include "CypherAST.h"
#include "SQLPreparedStatement.h"

#include <string>
#include <filesystem>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <set>


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
    m_maxIndex = std::max(idx, m_maxIndex.value_or(std::numeric_limits<Index>::lowest()));
  }

  const std::unordered_map<std::string, Index>& getTypeToIndex() const { return typeToIndex; }

  const std::optional<Index> getMaxIndex() const { return m_maxIndex; }
private:
  std::unordered_map<std::string, Index> typeToIndex;
  std::unordered_map<Index, std::string> indexToType;
  std::optional<Index> m_maxIndex;
};

// Node and relationship IDs.
// Internally they are converted to int64 so if the string doesn't represent an int64
// there will be exceptions thrown.
using ID = std::string;

enum class Element{
  Node,
  Relationship
};

struct ReturnClauseTerm
{
  // position of the term in the return clause.
  size_t returnClausePosition;

  // TODO support more later.
  openCypher::PropertyKeyName propertyName;
};

struct PathPatternElement
{
  PathPatternElement(const std::optional<openCypher::Variable>& var,
                     const std::vector<std::string>& labels)
  : var(var)
  , labels(labels)
  {}

  std::optional<openCypher::Variable> var;
  std::vector<std::string> labels;
};


struct GraphDB
{
  using Limit = openCypher::Limit;
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
  
  using FuncResults = std::function<void(const ResultOrder&, const std::vector<Variable>&, const VecColumnNames&, const VecValues&)>;
  
  using FuncOnSQLQuery = std::function<void(std::string const & sqlQuery)>;
  using FuncOnSQLQueryDuration = std::function<void(const std::chrono::steady_clock::duration)>;
  using FuncOnDBDiagnosticContent = std::function<void(int argc, char **argv, char **column)>;
  
  static constexpr const char* c_defaultDBPath{"default.sqlite3db"};

  // If |dbPath| has no value or points to a non-existing DB file,
  //   we create (overwrite) the DB at the default location c_defaultDBPath and create system tables in it.
  // Else (i.e when |dbPath| has a value and points to an existing DB file),
  //   we preserve the existing DB file and infer the graph schema from the tables of the existing DB file.
  GraphDB(const FuncOnSQLQuery& fOnSQLQuery,
          const FuncOnSQLQueryDuration& fOnSQLQueryDuration,
          const FuncOnDBDiagnosticContent& fOnDiagnostic,
          const std::optional<std::filesystem::path>& dbPath = std::nullopt);
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
                     const std::vector<std::pair<PropertyKeyName, std::string>>& propValues,
                     bool verifyNodesExist=false);
  
  // The property of entities and relationships that represents their ID.
  // It is a "system" property.
  PropertyKeyName const & idProperty() const { return m_idProperty; }
  
  // |labels| is the list of possible labels. When empty, all labels are allowed.
  void forEachElementPropertyWithLabelsIn(const Variable& var,
                                          const Element,
                                          const std::vector<ReturnClauseTerm>& propertyNames,
                                          const std::vector<std::string>& labels,
                                          const std::vector<const Expression*>* filter,
                                          const std::optional<Limit>& limit,
                                          FuncResults& f);

  struct VariableInfo {
    bool needsTypeInfo{};
    bool lookupProperties{};
  };
  void forEachPath(const std::vector<TraversalDirection>& traversalDirections,
                   const std::map<Variable, std::vector<ReturnClauseTerm>>& variablesI,
                   const std::vector<PathPatternElement>& pathPattern,
                   const ExpressionsByVarAndProperties& allFilters,
                   const std::optional<Limit>& limit,
                   FuncResults& f);
  
  // Time to run the SQL queries.
  mutable std::chrono::steady_clock::duration m_totalSQLQueryExecutionDuration{};
  // Time spent in the callback of the query on the sytem relationship table.
  mutable std::chrono::steady_clock::duration m_totalSystemRelationshipCbDuration{};
  // Time spent in the callback of the query on the labeled entities/relationships property tables.
  mutable std::chrono::steady_clock::duration m_totalPropertyTablesCbDuration{};
  
  void print();
  
  const auto& typesAndProperties() const { return m_properties; }

private:
  PropertyKeyName m_idProperty{openCypher::mkProperty("SYS__ID")};
  
  sqlite3* m_db{};
  IndexedTypes<size_t> m_indexedNodeTypes;
  IndexedTypes<size_t> m_indexedRelationshipTypes;
  // key : namedType.
  std::unordered_map<std::string, std::set<PropertyKeyName>> m_properties;
  
  const FuncOnSQLQuery m_fOnSQLQuery;
  const FuncOnSQLQueryDuration m_fOnSQLQueryDuration;
  const FuncOnDBDiagnosticContent m_fOnDiagnostic;
    
  std::unique_ptr<SQLPreparedStatement> m_addRelationshipPreparedStatement;
  std::unique_ptr<SQLPreparedStatement> m_addNodePreparedStatement;

  using AddElementPreparedStatementKey = std::pair<std::string /* type name */, std::vector<PropertyKeyName>>;
  std::map<AddElementPreparedStatementKey, std::unique_ptr<SQLPreparedStatement>> m_addElementPreparedStatements;

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
  
  struct VariablePostFilters{
    // These are the properties used in filters.
    std::set<PropertyKeyName> properties;
    
    std::vector<const Expression*> filters;
  };
  
  void analyzeFilters(const ExpressionsByVarAndProperties& allFilters,
                      std::vector<const Expression*>& idFilters,
                      std::map<Variable, VariablePostFilters>& postFilters) const;
  
  // Whether the type (aka label) information of a node or relationship needs to be returned from the relationships system table query.
  bool varRequiresTypeInfo(const Variable& var,
                           const std::vector<ReturnClauseTerm>& returnedProperties,
                           const std::map<Variable, VariablePostFilters>& postFilters) const;
  
  std::optional<std::set<size_t>> computeTypeFilter(const Element e, std::vector<std::string> const & nodeLabelsStr);
  
  static std::string mkFilterTypesConstraint(const std::set<size_t>& typesFilter, std::string const& typeColumn);
  
  template<typename ElementIDsContainer>
  void gatherPropertyValues(const Variable& var,
                            const std::vector<ElementIDsContainer>& elemsByType,
                            const Element elem,
                            const std::vector<PropertyKeyName>& strProperties,
                            const std::map<Variable, VariablePostFilters>& postFilters,
                            std::unordered_map<ID, std::vector<std::optional<std::string>>>& properties) const;
  
  // this method is timed
  int sqlite3_exec(const std::string& queryStr,
                   int (*callback)(void*,int,char**,char**),
                   void *,
                   const char **errmsg,
                   const sql::QueryVars& sqlVars = {}) const;
  
  int sqlite3_exec_notime(const std::string& queryStr,
                          const sql::QueryVars& arrayVariables,
                          int (*callback)(void*,int,char**,char**),
                          void *,
                          const char **errmsg) const;
  
  std::unique_ptr<SQLPreparedStatement> sqlite3_prepare(const std::string& queryStr);
  int sqlite3_step(int (*callback)(void*,int,char**,char**),
                   void *,
                   const char **errmsg,
                   const sql::QueryVars& sqlVars = {}) const;
  
  size_t getEndElementType() const;
  
  template<typename PreparedStatementKey>
  void runCachedStatement(std::map<PreparedStatementKey, std::unique_ptr<SQLPreparedStatement>>& cachedStatements,
                          PreparedStatementKey && key,
                          auto && buildQueryString,
                          auto && bindVars,
                          int (*callback)(void*,int,char**,char**) = nullptr,
                          void * cbParam = nullptr)
  {
    auto it = cachedStatements.find(key);
    if(it == cachedStatements.end())
      it = cachedStatements.emplace(std::move(key), mkPreparesStatement(buildQueryString)).first;
    else
      it->second->reset();
    runPreparedStatement(*it->second, bindVars, callback, cbParam);
  }

  void runCachedStatement(std::unique_ptr<SQLPreparedStatement>& ps,
                          auto && buildQueryString,
                          auto && bindVars,
                          int (*callback)(void*,int,char**,char**) = nullptr,
                          void * cbParam = nullptr)
  {
    if(!ps)
      ps = mkPreparesStatement(buildQueryString);
    else
      ps->reset();
    runPreparedStatement(*ps, bindVars, callback, cbParam);
  }

  std::unique_ptr<SQLPreparedStatement> mkPreparesStatement(auto && buildQueryString)
  {
    SQLBoundVarIndex var;
    std::ostringstream s;
    
    buildQueryString(var, s);
    
    auto ps = std::make_unique<SQLPreparedStatement>();
    if(ps->prepare(m_db, s.str()))
      throw std::logic_error("Prepare: " + std::string{sqlite3_errmsg(m_db)});
    return ps;
  }

  void runPreparedStatement(SQLPreparedStatement& ps, auto && bindVars,
                            int (*callback)(void*,int,char**,char**),
                            void * cbParam)
  {
    SQLBoundVarIndex var;
    bindVars(var, ps);
    if(ps.run(callback, cbParam, nullptr))
      throw std::logic_error("Run: " + std::string{sqlite3_errmsg(m_db)});
  }
};
