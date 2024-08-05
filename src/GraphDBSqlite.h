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
#include <type_traits>

#include "Metaprog.h"
#include "GraphDBSqliteTypes.h"


// Node and relationship IDs.
template<typename ID_T = int64_t>
struct GraphDB
{
  static_assert(isVariantMember<ID_T, Value>::value);

  // In the future we may support multiple labels per element if we store labels in a json property, in
  // the system (nodes and relationships) tables.
  static constexpr auto c_labelsPerElement = CountLabelsPerElement::One;

  using ID = ID_T;

  using Limit = openCypher::Limit;
  using Variable = openCypher::Variable;
  using Expression = openCypher::Expression;
  using SymbolicName = openCypher::SymbolicName;
  using PropertyKeyName = openCypher::PropertyKeyName;
  using TraversalDirection = openCypher::TraversalDirection;
  using ExpressionsByVarAndProperties = openCypher::ExpressionsByVarAndProperties;

  // @param dbPath : DB file path. If this parameter is std::nullopt, the DB file path is c_defaultDBPath.
  // @param overwrite :
  //   when |overwrite| is std::nullopt, the DB file is overwritten iff |dbPath| is std::nullopt
  //   when |overwrite| is NOT std::nullopt, the DB file is overwritten iff *overwrite == Overwrite::Yes
  //   Note: If the DB file is not overwritten, we infer the graph schema from it.
  GraphDB(const FuncOnSQLQuery& fOnSQLQuery,
          const FuncOnSQLQueryDuration& fOnSQLQueryDuration,
          const FuncOnDBDiagnosticContent& fOnDiagnostic,
          const std::optional<std::filesystem::path>& dbPath = std::nullopt,
          const std::optional<Overwrite> overwrite = std::nullopt);
  ~GraphDB();
  
  // Creates a sql table.
  // todo support typed properties.
  void addType(std::string const& typeName,
               bool isNode,
               std::vector<PropertySchema> const& properties);
  
  // When doing several inserts, it is best to have a transaction for many inserts.
  // TODO redesign API to remove this.
  void beginTransaction();
  void endTransaction();
  
  ID addNode(const std::string& type,
             const std::vector<std::pair<PropertyKeyName, Value>>& propValues);
  ID addRelationship(const std::string& type,
                     const ID& originEntity,
                     const ID& destinationEntity,
                     const std::vector<std::pair<PropertyKeyName, Value>>& propValues,
                     bool verifyNodesExist=false);
  
  // The property of entities and relationships that represents their ID.
  // It is a "system" property.
  PropertySchema const & idProperty() const { return m_idProperty; }
  
  // |labels| is the list of possible labels. When empty, all labels are allowed.
  void forEachElementPropertyWithLabelsIn(const Variable& var,
                                          const Element,
                                          const std::vector<ReturnClauseTerm>& propertyNames,
                                          const std::set<std::string>& labels,
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
  PropertySchema m_idProperty{
    openCypher::mkProperty("SYS__ID"),
    Traits<ID>::correspondingValueType,
    IsNullable::No
  };
  
  sqlite3* m_db{};
  IndexedTypes<size_t> m_indexedNodeTypes;
  IndexedTypes<size_t> m_indexedRelationshipTypes;
  // auto-increment integer table columns start at 1 in sqlite.
  static constexpr size_t c_noType = 0ull;

  // key : namedType.
  std::unordered_map<std::string, std::set<PropertySchema>> m_properties;
  
  const FuncOnSQLQuery m_fOnSQLQuery;
  const FuncOnSQLQueryDuration m_fOnSQLQueryDuration;
  const FuncOnDBDiagnosticContent m_fOnDiagnostic;
    
  std::unique_ptr<SQLPreparedStatement> m_addRelationshipPreparedStatement;
  std::unique_ptr<SQLPreparedStatement> m_addRelationshipWithIDPreparedStatement;
  std::unique_ptr<SQLPreparedStatement> m_addNodePreparedStatement;
  std::unique_ptr<SQLPreparedStatement> m_addNodeWithIDPreparedStatement;

  using AddElementPreparedStatementKey = std::pair<std::string /* type name */, std::vector<PropertyKeyName>>;
  std::map<AddElementPreparedStatementKey, std::unique_ptr<SQLPreparedStatement>> m_addElementPreparedStatements;

  std::set<std::string> computeLabels(const Element, const std::set<std::string>& inputLabels) const;
  std::vector<size_t> labelsToTypeIndices(const Element elem, const std::vector<std::string>& inputLabels) const;
  
  void validatePropertyValues(const std::string& typeName,
                              const std::vector<std::pair<PropertyKeyName, Value>>& propValues) const;
  
  [[nodiscard]]
  bool findValidProperties(const std::string& typeName, const std::vector<PropertyKeyName>& propNames, std::vector<bool>& valid) const;
  
  void addElement(const std::string& typeName,
                  const ID& id,
                  const std::vector<std::pair<PropertyKeyName, Value>>& propValues);
  
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
  
  enum class LabelAssociation{AND, OR};

  std::optional<std::set<size_t>> computeTypeFilter(const Element e,
                                                    const LabelAssociation,
                                                    std::set<std::string> const & nodeLabelsStr);
  
  static std::string mkFilterTypesConstraint(const std::set<size_t>& typesFilter, std::string const& typeColumn);

  void gatherPropertyValues(const Variable& var,
                            std::vector<std::unordered_set<ID>>&& elemsByType,
                            const Element elem,
                            const std::vector<PropertyKeyName>& propertyNames,
                            const std::map<Variable, VariablePostFilters>& postFilters,
                            std::unordered_map<ID, std::vector<Value>>& properties) const;
  
  // this method is timed
  int sqlite3_exec(const std::string& queryStr,
                   int (*callback)(void*, int, Value*, char**),
                   void *,
                   const char **errmsg,
                   const sql::QueryVars& sqlVars = {}) const;
  
  int sqlite3_exec_notime(const std::string& queryStr,
                          const sql::QueryVars& arrayVariables,
                          int (*callback)(void*, int, Value*, char**),
                          void *,
                          const char **errmsg) const;
  
  std::unique_ptr<SQLPreparedStatement> sqlite3_prepare(const std::string& queryStr);
  int sqlite3_step(int (*callback)(void*, int, Value*, char**),
                   void *,
                   const char **errmsg,
                   const sql::QueryVars& sqlVars = {}) const;
  
  size_t getEndElementType() const;
  
  void runVolatileStatement(auto && buildQueryString,
                            auto && bindVars,
                            int (*callback)(void*, int, Value*, char**) = nullptr,
                            void * cbParam = nullptr)
  {
    std::map<int, std::unique_ptr<SQLPreparedStatement>> map;
    runCachedStatement(map, 0, buildQueryString, bindVars, callback, cbParam);
  }

  template<typename PreparedStatementKey>
  void runCachedStatement(std::map<PreparedStatementKey, std::unique_ptr<SQLPreparedStatement>>& cachedStatements,
                          PreparedStatementKey && key,
                          auto && buildQueryString,
                          auto && bindVars,
                          int (*callback)(void*, int, Value*, char**) = nullptr,
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
                          int (*callback)(void*, int, Value*, char**) = nullptr,
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
                            int (*callback)(void*, int, Value*, char**),
                            void * cbParam)
  {
    SQLBoundVarIndex var;
    bindVars(var, ps);
    if(ps.run(callback, cbParam, nullptr))
      throw std::logic_error("Run: " + std::string{sqlite3_errmsg(m_db)});
  }
};
