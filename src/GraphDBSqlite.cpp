#include "GraphDBSqlite.h"
#include "Logs.h"
#include "SqlAST.h"


#include <iostream>
#include <sstream>
#include <numeric>

#define GRAPHDBSQLITE_STATICALLY_LINK_CARRAY_EXTENSION 1

#if GRAPHDBSQLITE_STATICALLY_LINK_CARRAY_EXTENSION
extern "C"
{
SQLITE_API int sqlite3_carray_init(
                        sqlite3 *db,
                        char **pzErrMsg,
                        const sqlite3_api_routines *pApi
                        );
}
#endif // GRAPHDBSQLITE_STATICALLY_LINK_CARRAY_EXTENSION

template<typename ID>
struct IDAndType
{
  ID id{};
  size_t type{};
  
  bool operator == (IDAndType const & o) const { return id == o.id; }
};

namespace std
{
template<typename ID>
struct hash<IDAndType<ID>>
{
  size_t operator()(const IDAndType<ID>& i) const
  {
    return std::hash<ID>()(i.id);
  }
};
}

namespace
{

template<typename T>
requires std::copyable<T>
T cloneIfNeeded(T const & a){
  return a;
}

template<typename T>
requires (!std::copyable<T>)
T cloneIfNeeded(T const & a){
  return a.clone();
}

const char * valueTypeToSQLliteTypeAffinity(ValueType t)
{
  switch(t)
  {
    case ValueType::String:
      return "TEXT";
    case ValueType::Float:
      return "REAL";
    case ValueType::Integer:
      return "INTEGER";
    case ValueType::ByteArray:
      return "BLOB";
    default:
      throw std::logic_error("valueTypeToSQLliteTypeAffinity: type is not supported");
  }
}

bool isNullSQLKeyword(std::string const & str)
{
  if(str.size() != 4)
    return false;
  auto str2 = str;
  std::transform(str2.begin(), str2.end(), str2.begin(),
                 [](unsigned char c){ return std::tolower(c); });
  return str2 == "null";
}

ValueType SQLiteTypeToValueType(const char* sqliteColumnType)
{
  std::string str{sqliteColumnType};
  
  std::transform(str.begin(), str.end(), str.begin(),
                 [](unsigned char c){ return std::tolower(c); });
  
  if(str.starts_with("int"))
    return ValueType::Integer;
  if(str.starts_with("bigint"))
    return ValueType::Integer;
  
  if(str.starts_with("num"))
    return ValueType::Float;
  if(str.starts_with("real"))
    return ValueType::Float;
  if(str.starts_with("flo"))
    return ValueType::Float;
  
  if(str.starts_with("text"))
    return ValueType::String;
  if(str.starts_with("str"))
    return ValueType::String;
  if(str.starts_with("var"))
    return ValueType::String;
  
  if(str.starts_with("blob"))
    return ValueType::ByteArray;
  
  throw std::logic_error("Could not infer property type from SQLite data type: '" + str + "'");
}

std::string sqlliteQuote(const std::string& s)
{
  std::string res;
  res.reserve(s.size() + 2);
  res.push_back('\'');
  for(const auto & c : s)
  {
    if(c == '\'')
      res.push_back('\'');
    res.push_back(c);
  }
  res.push_back('\'');
  return res;
}


std::string sqlliteUnquote(const std::string& s)
{
  if(s.size() < 2 || s[0] != '\'' || s[s.size()-1] != '\'')
    throw std::logic_error("invalid quoted string value:" + s);
  std::string res;
  res.reserve(s.size());
  bool skipNextQuote = false;
  for(size_t i=1, end = s.size()-1; i<end; ++i)
  {
    const bool isQuote = s[i] == '\'';
    if(skipNextQuote)
    {
      if(!isQuote)
        throw std::logic_error("invalid quoted string value:" + s);
      skipNextQuote = false;
    }
    else
    {
      res.push_back(s[i]);
      if(isQuote)
        skipNextQuote = true;
    }
  }
  if(skipNextQuote)
    throw std::logic_error("invalid quoted string value:" + s);
  return res;
}

void verifyTypeConsistency(const Value& value, const PropertySchema& propertySchema)
{
  std::visit([&](auto && arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, int64_t>)
    {
      if(propertySchema.type != ValueType::Integer)
        throw std::logic_error("Integer value for non-Integer property.");
    }
    else if constexpr (std::is_same_v<T, double>)
    {
      if(propertySchema.type != ValueType::Float)
        throw std::logic_error("Float value for non-Float property.");
    }
    else if constexpr (std::is_same_v<T, StringPtr>)
    {
      if(propertySchema.type != ValueType::String)
        throw std::logic_error("String value for non-String property.");
    }
    else if constexpr (std::is_same_v<T, ByteArrayPtr>)
    {
      if(propertySchema.type != ValueType::ByteArray)
        throw std::logic_error("ByteArray value for non-ByteArray property.");
    }
    else if constexpr (std::is_same_v<T, Nothing>)
    {
      if(propertySchema.isNullable == IsNullable::No)
        throw std::logic_error("Null value for non-nullable property");
    }
    else
      static_assert(c_false<T>, "non-exhaustive visitor!");
  }, value);
}

void toSQLQueryStringValue(const int64_t & arg, std::ostream& os)
{
  os << arg;
}
void toSQLQueryStringValue(const double & arg, std::ostream& os)
{
  os << arg;
}
void toSQLQueryStringValue(const Nothing &, std::ostream& os)
{
  os << "NULL";
}
void toSQLQueryStringValue(const StringPtr & arg, std::ostream& os)
{
  os << sqlliteQuote({arg.string.get()});
}
void toSQLQueryStringValue(const ByteArrayPtr & arg, std::ostream& os)
{
  os << arg.toHexStr();
}

void toSQLQueryStringValue(const Value & value, std::ostream& os)
{
  std::visit([&](auto && arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, int64_t>)
      toSQLQueryStringValue(arg, os);
    else if constexpr (std::is_same_v<T, double>)
      toSQLQueryStringValue(arg, os);
    else if constexpr (std::is_same_v<T, StringPtr>)
      toSQLQueryStringValue(arg, os);
    else if constexpr (std::is_same_v<T, ByteArrayPtr>)
      toSQLQueryStringValue(arg, os);
    else if constexpr (std::is_same_v<T, Nothing>)
      toSQLQueryStringValue(arg, os);
    else
      static_assert(c_false<T>, "non-exhaustive visitor!");
  }, value);
}

}  // NS

template<typename ID>
GraphDB<ID>::GraphDB(const FuncOnSQLQuery& fOnSQLQuery,
                     const FuncOnSQLQueryDuration& fOnSQLQueryDuration,
                     const FuncOnDBDiagnosticContent& fOnDiagnostic,
                     const std::optional<std::filesystem::path>& dbPath,
                     const std::optional<Overwrite> overwrite)
: m_fOnSQLQuery(fOnSQLQuery)
, m_fOnSQLQueryDuration(fOnSQLQueryDuration)
, m_fOnDiagnostic(fOnDiagnostic)
{
  LogIndentScope _ = logScope(std::cout, "Creating System tables...");
  
  const bool useIndices = true;
  
  const Overwrite canOverwriteDB = [&]()
  {
    if(overwrite.has_value())
      return *overwrite;
    if(dbPath.has_value())
      // The caller has specified a path, in this case de default is to NOT overwrite the DB file.
      return Overwrite::No;
    else
      // No path was specified by the caller, in this case de default is to overwrite the DB file.
      return Overwrite::Yes;
  }();

  const bool reinitDB = (canOverwriteDB == Overwrite::Yes) || !std::filesystem::exists(*dbPath);
  const auto dbFile = dbPath.value_or(std::filesystem::path{c_defaultDBPath});

  if(reinitDB)
    std::filesystem::remove(dbFile);

  if(auto res = sqlite3_open(dbFile.string().c_str(), &m_db))
    throw std::logic_error(sqlite3_errstr(res));
  char* msg{};

#if GRAPHDBSQLITE_STATICALLY_LINK_CARRAY_EXTENSION

  // When we statically link the carray extension we need to initialize
  // the extension manually.
  if(auto res = sqlite3_carray_init(m_db, &msg, nullptr))
    throw std::logic_error(msg);

#else

  if(auto res = sqlite3_db_config(m_db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, 0))
     throw std::logic_error(sqlite3_errstr(res));
  if(auto res = sqlite3_load_extension(m_db, "carray", 0, &msg))
     throw std::logic_error(msg);

#endif  // GRAPHDBSQLITE_STATICALLY_LINK_CARRAY_EXTENSION

  if(reinitDB)
  {
    {
      LogIndentScope _ = logScope(std::cout, "Creating Nodes System table...");
      // This table avoids having to lookup into all nodes tables when looking for an entity
      // for which we don't have type information.
      std::string tableName = "nodes";
      {
        const std::string req = "DROP TABLE " + tableName + ";";
        // ignore error
        auto res = sqlite3_exec(req, 0, 0, 0);
      }
      {
        std::ostringstream s;
        s << "CREATE TABLE " << tableName << " (";
        {
          s << m_idProperty.name << " ";
          s << valueTypeToSQLliteTypeAffinity(m_idProperty.type) << " NOT NULL PRIMARY KEY, ";
          s << "NodeType INTEGER";
        }
        s << ");";
        if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
          throw std::logic_error(sqlite3_errstr(res));
      }
      if(useIndices)
      {
        const std::string req = "CREATE INDEX NodeTypeIndex ON " + tableName + "(NodeType);";
        if(auto res = sqlite3_exec(req, 0, 0, 0))
          throw std::logic_error(sqlite3_errstr(res));
      }
    }
    {
      LogIndentScope _ = logScope(std::cout, "Creating Relationships System table...");
      std::string tableName = "relationships";
      {
        const std::string req = "DROP TABLE " + tableName + ";";
        // ignore error
        auto res = sqlite3_exec(req, 0, 0, 0);
      }
      {
        std::ostringstream s;
        s << "CREATE TABLE " << tableName << " (";
        {
          s << m_idProperty.name << " ";
          s << valueTypeToSQLliteTypeAffinity(m_idProperty.type) << " NOT NULL PRIMARY KEY, ";
          s << "RelationshipType ";
          s << "INTEGER NOT NULL, ";
          s << "OriginID ";
          s << valueTypeToSQLliteTypeAffinity(m_idProperty.type) << " NOT NULL, ";
          s << "DestinationID ";
          s << valueTypeToSQLliteTypeAffinity(m_idProperty.type) << " NOT NULL";
        }
        s << ");";
        if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
          throw std::logic_error(sqlite3_errstr(res));
      }
      if(useIndices)
      {
        const std::string req = "CREATE INDEX RelationshipTypeIndex ON " + tableName + "(RelationshipType);";
        if(auto res = sqlite3_exec(req, 0, 0, 0))
          throw std::logic_error(sqlite3_errstr(res));
      }
      if(useIndices)
      {
        const std::string req = "CREATE INDEX originIDIndex ON " + tableName + "(OriginID);";
        if(auto res = sqlite3_exec(req, 0, 0, 0))
          throw std::logic_error(sqlite3_errstr(res));
      }
      if(useIndices)
      {
        const std::string req = "CREATE INDEX destinationIDIndex ON " + tableName + "(DestinationID);";
        if(auto res = sqlite3_exec(req, 0, 0, 0))
          throw std::logic_error(sqlite3_errstr(res));
      }
    }
    {
      LogIndentScope _ = logScope(std::cout, "Creating Types System table...");
      std::string tableName = "namedTypes";
      {
        const std::string req = "DROP TABLE " + tableName + ";";
        // ignore error
        auto res = sqlite3_exec(req, 0, 0, 0);
      }
      std::ostringstream s;
      s << "CREATE TABLE " << tableName << " (";
      {
        s << "TypeIdx INTEGER NOT NULL PRIMARY KEY, ";
        s << "Kind INTEGER NOT NULL, ";
        s << "NamedType TEXT NOT NULL";
      }
      s << ");";
      if(auto res = sqlite3_exec(s.str(), 0, 0, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
  else
  {
    // Infer the graph schema from the DB
    
    // Throw if the IDs types do not match the template parameter of this class defining the ID type.
    {
      struct Data
      {
        PropertySchema expectedIDPropertySchema;
        std::optional<PropertySchema> inferredIDPropertySchema;
      } data{m_idProperty};
      
      if(auto res = sqlite3_exec("PRAGMA table_info('nodes')", [](void *p_Data, int argc, Value *argv, char **column) {
        auto & data = *static_cast<Data*>(p_Data);
        const std::string columnName = std::get<StringPtr>(argv[1]).string.get();
        if(columnName == data.expectedIDPropertySchema.name.symbolicName.str)
        {
          const char * sqliteType = std::get<StringPtr>(argv[2]).string.get();
          const auto propertyType = SQLiteTypeToValueType(sqliteType);
          
          const bool notNull = std::holds_alternative<int64_t>(argv[3]) && (std::get<int64_t>(argv[3]) == 1);
          const auto isNullable = notNull ? IsNullable::No : IsNullable::Yes;
          data.inferredIDPropertySchema = PropertySchema{
            openCypher::mkProperty(columnName),
            propertyType
          };
        }
        return 0;
      }, &data, 0))
        throw std::logic_error(sqlite3_errstr(res));
      if(!data.inferredIDPropertySchema.has_value())
        throw std::invalid_argument("Could not find ID field '" + m_idProperty.name.symbolicName.str + "' in nodes table.");
      if(data.inferredIDPropertySchema->type != m_idProperty.type)
        throw std::invalid_argument("ID type mismatch, expected " + toStr(m_idProperty.type) + " but have " + toStr(data.inferredIDPropertySchema->type));
    }

    const char* msg{};
    if(auto res = sqlite3_exec("SELECT NamedType, Kind, TypeIdx FROM namedTypes;", [](void *p_This, int argc, Value *argv, char **column) {
      auto & This = *static_cast<GraphDB*>(p_This);
      size_t typeIdx = std::get<int64_t>(argv[2]);
      const std::string kind{ std::get<StringPtr>(argv[1]).string.get() };
      const bool isNode = kind == std::string{"E"};
      const bool isRela = kind == std::string{"R"};
      if(!isNode && !isRela)
        throw std::logic_error("Expected E or R, got:" + kind);
      const std::string namedType = std::get<StringPtr>(argv[0]).string.get();
      if(isNode)
        This.m_indexedNodeTypes.add(typeIdx, namedType);
      else
        This.m_indexedRelationshipTypes.add(typeIdx, namedType);
      return 0;
    }, this, &msg))
      throw std::logic_error(std::string{msg});

    std::vector<std::string> typeNames;
    for(const auto & [typeName, _] : m_indexedNodeTypes.getTypeToIndex())
      typeNames.push_back(typeName);
    for(const auto & [typeName, _] : m_indexedRelationshipTypes.getTypeToIndex())
      typeNames.push_back(typeName);
    for(const auto & typeName : typeNames)
    {
      if(auto it = m_properties.find(typeName); it != m_properties.end())
        throw std::logic_error("Invalid DB, type already exists:" + typeName);

      std::set<PropertySchema>& set = m_properties[typeName];

      std::ostringstream s;
      s << "PRAGMA table_info('" << typeName << "')";
      if(auto res = sqlite3_exec(s.str(), [](void *p_Set, int argc, Value *argv, char **column) {
        auto & set = *static_cast<std::set<PropertySchema>*>(p_Set);
        const std::string columnName = std::get<StringPtr>(argv[1]).string.get();
        const char * sqliteType = std::get<StringPtr>(argv[2]).string.get();
        const auto propertyType = SQLiteTypeToValueType(sqliteType);
        
        const bool notNull = std::holds_alternative<int64_t>(argv[3]) && (std::get<int64_t>(argv[3]) == 1);
        const auto isNullable = notNull ? IsNullable::No : IsNullable::Yes;

        const bool hasDefaultValue = !std::holds_alternative<Nothing>(argv[4]);
        std::shared_ptr<Value> defaultValue;
        if(hasDefaultValue)
        {
          // The default value is returned as a Value containing a StringPtr,
          // we convert it to the property type here.
          
          const std::string defaultValueStr{std::get<StringPtr>(argv[4]).string.get()};
          
          if((isNullable == IsNullable::Yes) && isNullSQLKeyword(defaultValueStr))
            defaultValue = std::make_shared<Value>(Nothing{});
          else
          {
            switch(propertyType)
            {
              case ValueType::Integer:
              {
                // may throw
                const auto i64 = strToInt64(defaultValueStr);
                defaultValue = std::make_shared<Value>(i64);
                break;
              }
              case ValueType::Float:
              {
                // may throw
                const auto dbl = strToDouble(defaultValueStr);
                defaultValue = std::make_shared<Value>(dbl);
                break;
              }
              case ValueType::String:
              {
                auto unQuotedString = sqlliteUnquote(defaultValueStr);
                defaultValue = std::make_shared<Value>(StringPtr::fromCStr(unQuotedString.c_str()));
                break;
              }
              case ValueType::ByteArray:
              {
                defaultValue = std::make_shared<Value>(ByteArrayPtr::fromHexStr(defaultValueStr));
                break;
              }
            }
          }
        }
        
        set.insert(PropertySchema{
          openCypher::mkProperty(columnName),
          propertyType,
          isNullable,
          defaultValue
        });
        
        return 0;
      }, &set, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
  }
}

template<typename ID>
GraphDB<ID>::~GraphDB()
{
  sqlite3_close(m_db);
}

template<typename ID>
void GraphDB<ID>::addType(const std::string &typeName, bool isNode, const std::vector<PropertySchema> &properties)
{
  if(auto it = m_properties.find(typeName); it != m_properties.end())
    throw std::logic_error("CREATE TABLE, type already exists.");

  {
    // It is not necessary to cache the prepared statement: types are added very infrequently
    // so we don't need to optimize the query compilation time.
    runVolatileStatement([&](SQLBoundVarIndex & var, std::ostringstream& s) {
      s << "CREATE TABLE " << typeName << " (";
      {
        s << m_idProperty.name << " ";
        s << valueTypeToSQLliteTypeAffinity(m_idProperty.type) << " NOT NULL PRIMARY KEY";
        for(const auto & property : properties)
        {
          s << ", " << property.name << " " << valueTypeToSQLliteTypeAffinity(property.type);
          if(property.isNullable == IsNullable::No)
            s << " NOT NULL";
          if(property.defaultValue)
          {
            verifyTypeConsistency(*property.defaultValue, property);
            
            s << " DEFAULT ";
            toSQLQueryStringValue(*property.defaultValue, s);
          }
        }
      }
      s << ")";
    },
                       [&](SQLBoundVarIndex& var, SQLPreparedStatement& ps) {
    });
  }
  // record type
  {
    std::ostringstream s;
    s << "INSERT INTO namedTypes (NamedType, Kind) Values('"
    << typeName << "', '"
    << (isNode ? "E" : "R")
    << "') RETURNING TypeIdx";
    size_t typeIdx{std::numeric_limits<size_t>::max()};
    const char* msg{};
    if(auto res = sqlite3_exec(s.str(), [](void *p_typeIdx, int argc, Value *argv, char **column) {
      auto & typeIdx = *static_cast<size_t*>(p_typeIdx);
      typeIdx = std::get<int64_t>(argv[0]);
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
    set.insert(m_idProperty);
  }
  // todo use a transaction, rollback if there is an error.
}

template<typename ID>
void GraphDB<ID>::validatePropertyValues(const std::string& typeName,
                                     const std::vector<std::pair<PropertyKeyName, Value>>& propValues) const
{
  auto it = m_properties.find(typeName);
  if(it == m_properties.end())
    throw std::logic_error("The element type doesn't exist.");
  for(const auto & [name, value] : propValues)
  {
    auto it2 = it->second.find(name);
    if(it2 == it->second.end())
      throw std::logic_error(std::string{"The property '"} + name.symbolicName.str + "' doesn't exist for the type '" + typeName + "'");
    const auto & propertySchema = *it2;
    verifyTypeConsistency(value, propertySchema);
  }
  return true;
}

template<typename ID>
bool GraphDB<ID>::findValidProperties(const std::string& typeName,
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

template<typename ID>
void GraphDB<ID>::beginTransaction()
{
  if(auto res = sqlite3_exec("BEGIN TRANSACTION", 0, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
}
template<typename ID>
void GraphDB<ID>::endTransaction()
{
  if(auto res = sqlite3_exec("END TRANSACTION", 0, 0, 0))
    throw std::logic_error(sqlite3_errstr(res));
}

template<typename ID>
ID GraphDB<ID>::addNode(const std::string& typeName,
                    const std::vector<std::pair<PropertyKeyName, Value>>& propValues)
{
  const auto typeIdx = m_indexedNodeTypes.getIfExists(typeName);
  if(!typeIdx.has_value())
    throw std::logic_error("unknown node type: " + typeName);

  std::optional<ID> nodeId;
  
  for(const auto & [name, value] : propValues)
  {
    if(name == m_idProperty.name)
    {
      // an ID was specified
      runCachedStatement(m_addNodeWithIDPreparedStatement,
                         [&](SQLBoundVarIndex & var, std::ostringstream& s) {
        s << "INSERT INTO nodes (" << m_idProperty.name << ", NodeType) Values("
        << var.nextAsStr() << ", "
        << var.nextAsStr()
        <<") RETURNING " << m_idProperty.name;
      },
                         [&, valuePtr=&value](SQLBoundVarIndex& var, SQLPreparedStatement& ps) {
        ps.bindVariable(var.next(), *valuePtr);
        ps.bindVariable(var.next(), static_cast<int64_t>(*typeIdx));
      },
                         [](void *p_nodeId, int argc, Value *argv, char **column) {
        auto & nodeId = *static_cast<std::optional<ID>*>(p_nodeId);
        nodeId = std::move(std::get<ID>(argv[0]));
        return 0;
      },
                         &nodeId);
      goto idDone;
    }
  }

  // no ID was specified. It will be generated by the DB if the ID type is integer, if not an error will be returned.
  runCachedStatement(m_addNodePreparedStatement,
                     [&](SQLBoundVarIndex & var, std::ostringstream& s) {
    s << "INSERT INTO nodes (NodeType) Values("
    << var.nextAsStr()
    <<") RETURNING " << m_idProperty.name;
  },
                     [&](SQLBoundVarIndex& var, SQLPreparedStatement& ps) {
    ps.bindVariable(var.next(), static_cast<int64_t>(*typeIdx));
  },
                     [](void *p_nodeId, int argc, Value *argv, char **column) {
    auto & nodeId = *static_cast<std::optional<ID>*>(p_nodeId);
    nodeId = std::move(std::get<ID>(argv[0]));
    return 0;
  },
                     &nodeId);

idDone:;
  if(!nodeId.has_value())
    throw std::logic_error("no result for nodeId.");
  
  addElement(typeName, *nodeId, propValues);
  return std::move(*nodeId);
}

// There is a system table to generate relationship ids.
template<typename ID>
ID GraphDB<ID>::addRelationship(const std::string& typeName,
                            const ID& originEntity,
                            const ID& destinationEntity,
                            const std::vector<std::pair<PropertyKeyName, Value>>& propValues,
                            bool verifyNodesExist)
{
  const auto typeIdx = m_indexedRelationshipTypes.getIfExists(typeName);
  if(!typeIdx.has_value())
    throw std::logic_error("unknown relationship type: " + typeName);

  // Verify the origin & destination node ids exist
  if(verifyNodesExist)
  {
    size_t countMatches{};
    size_t expectedCountMatches{1};
    {
      std::ostringstream s;
      s << "SELECT " << m_idProperty.name << " from nodes WHERE " << m_idProperty.name << " IN (";
      toSQLQueryStringValue(originEntity, s);
      if(originEntity != destinationEntity)
      {
        s << ", ";
        toSQLQueryStringValue(destinationEntity, s);
        ++expectedCountMatches;
      }
      s << ")";
      if(auto res = sqlite3_exec(s.str(), [](void *p_countMatches, int argc, Value *argv, char **column) {
        auto & countMatches = *static_cast<size_t*>(p_countMatches);
        ++countMatches;
        return 0;
      }, &countMatches, 0))
        throw std::logic_error(sqlite3_errstr(res));
    }
    if(countMatches != expectedCountMatches)
      throw std::logic_error("origin or destination node not found.");
  }

  std::optional<ID> relId;

  for(const auto & [name, value] : propValues)
  {
    if(name == m_idProperty.name)
    {
      // an ID was specified
      runCachedStatement(m_addRelationshipWithIDPreparedStatement,
                         [&](SQLBoundVarIndex & var, std::ostringstream& s) {
        s << "INSERT INTO relationships (" << m_idProperty.name << ", RelationshipType, OriginID, DestinationID) Values("
        << var.nextAsStr()
        << ", " << var.nextAsStr()
        << ", " << var.nextAsStr()
        << ", " << var.nextAsStr()
        <<") RETURNING " << m_idProperty.name;
      },
                         [&, valuePtr=&value](SQLBoundVarIndex& var, SQLPreparedStatement& ps) {
        ps.bindVariable(var.next(), *valuePtr);
        ps.bindVariable(var.next(), static_cast<int64_t>(*typeIdx));
        ps.bindVariable(var.next(), originEntity);
        ps.bindVariable(var.next(), destinationEntity);
      },
                         [](void *p_relId, int argc, Value *argv, char **column) {
        auto & relId = *static_cast<std::optional<ID>*>(p_relId);
        relId = std::move(std::get<ID>(argv[0]));
        return 0;
      },
                         &relId);
      goto idDone;
    }
  }
  
  // no ID was specified. It will be generated by the DB if the ID type is integer, if not an error will be returned.
  runCachedStatement(m_addRelationshipPreparedStatement,
                     [&](SQLBoundVarIndex & var, std::ostringstream& s) {
    s << "INSERT INTO relationships (RelationshipType, OriginID, DestinationID) Values("
    << var.nextAsStr()
    << ", " << var.nextAsStr()
    << ", " << var.nextAsStr()
    <<") RETURNING " << m_idProperty.name;
  },
                     [&](SQLBoundVarIndex& var, SQLPreparedStatement& ps) {
    ps.bindVariable(var.next(), static_cast<int64_t>(*typeIdx));
    ps.bindVariable(var.next(), originEntity);
    ps.bindVariable(var.next(), destinationEntity);
  },
                     [](void *p_relId, int argc, Value *argv, char **column) {
    auto & relId = *static_cast<std::optional<ID>*>(p_relId);
    relId = std::move(std::get<ID>(argv[0]));
    return 0;
  },
                     &relId);

idDone:;
  if(!relId.has_value())
    throw std::logic_error("no result for relId.");
  addElement(typeName, *relId, propValues);
  return std::move(*relId);
}

template<typename ID>
void GraphDB<ID>::addElement(const std::string& typeName,
                         const ID& id,
                         const std::vector<std::pair<PropertyKeyName, Value>>& propValues)
{
  validatePropertyValues(typeName, propValues);
  
  std::vector<PropertyKeyName> allPropertyNames;
  allPropertyNames.reserve(propValues.size());
  for(const auto & [propertyName, _] : propValues)
    allPropertyNames.push_back(propertyName);

  runCachedStatement(m_addElementPreparedStatements,
                     AddElementPreparedStatementKey{typeName, allPropertyNames},
                     [&](SQLBoundVarIndex & var, std::ostringstream& s) {
    s << "INSERT INTO " << typeName << " (" << m_idProperty.name;
    for(const auto & [propertyName, _] : propValues)
    {
      if(propertyName == m_idProperty.name)
        continue;
      s << ", " << propertyName;
    }
    s << ") VALUES (";
    
    s << var.nextAsStr(); // id
    for(const auto & [propertyName, _] : propValues)
    {
      if(propertyName == m_idProperty.name)
        continue;
      s << ", " << var.nextAsStr();
    }
    
    s << ");";
  },
                     [&](SQLBoundVarIndex& var, SQLPreparedStatement& ps) {
    ps.bindVariable(var.next(), id);
    for(const auto & [propertyName, value] : propValues)
    {
      if(propertyName == m_idProperty.name)
        continue;
      ps.bindVariable(var.next(), value);
    }
  });
}

template<typename ID>
void GraphDB<ID>::print()
{
  std::vector<std::string> names;
  if(auto res = sqlite3_exec("SELECT name FROM sqlite_master WHERE type='table';",
                             [](void *p_names, int argc, Value *argv, char **column) {
    auto & names = *static_cast<std::vector<std::string>*>(p_names);
    for (int i=0; i< argc; i++)
      names.emplace_back(std::get<StringPtr>(argv[i]).string.get());
    return 0;
  }, &names, 0))
    throw std::logic_error(sqlite3_errstr(res));
  
  for(const auto & name: names)
  {
    auto diagFunc = [](void *p_This, int argc, Value *argv, char **column) {
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

template<typename ID>
std::vector<std::string> GraphDB<ID>::computeLabels(const Element elem, const std::vector<std::string>& inputLabels) const
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

template<typename ID>
std::vector<size_t> GraphDB<ID>::labelsToTypeIndices(const Element elem, const std::vector<std::string>& inputLabels) const
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
                           const std::set<PropertySchema>& sqlFields,
                           const std::map<openCypher::Variable, std::map<openCypher::PropertyKeyName, std::string>>& propertyMappingCypherToSQL,
                           std::string& sqlFilter,
                           sql::QueryVars & vars)
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
  sqlExpr->toString(s, vars);
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
template<typename ID>
std::optional<std::set<size_t>>
GraphDB<ID>::computeTypeFilter(const Element e,
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


template<typename ID>
void GraphDB<ID>::forEachPath(const std::vector<TraversalDirection>& traversalDirections,
                          const std::map<Variable, std::vector<ReturnClauseTerm>>& variablesI,
                          const std::vector<PathPatternElement>& pathPattern,
                          const ExpressionsByVarAndProperties& allFilters,
                          const std::optional<Limit>& limit,
                          FuncResults& f)
{
  const bool hasTraversalDirectionAny =
  std::find(traversalDirections.begin(),
            traversalDirections.end(),
            TraversalDirection::Any) != traversalDirections.end();
  
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
  using CandidateRows = std::vector<std::vector<IDAndType<ID>>>;
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
    sql::QueryVars sqlVars;

    {
      if(hasTraversalDirectionAny)
      {
        // TODO replace undirectedRelationships by a VIEW,
        // verify that performance is the same on large graph.
        s <<R"V0G0N(WITH undirectedRelationships(SYS__ID, RelationshipType, OriginID, DestinationID) as NOT MATERIALIZED(
  SELECT A.SYS__ID, A.RelationshipType, A.OriginID, A.DestinationID FROM relationships A
  UNION ALL
  SELECT B.SYS__ID, B.RelationshipType, B.DestinationID, B.OriginID FROM relationships B)
)V0G0N";
      }
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
        {
          if(traversalDirection == TraversalDirection::Any)
            relationshipSelfJoins.push_back("undirectedRelationships " + relationshipTableJoinAlias);
          else
            relationshipSelfJoins.push_back("relationships " + relationshipTableJoinAlias);
        }

        std::string columnNameForID(relationshipTableJoinAlias);
        std::optional<std::string> columnNameForType;
        if(elem == Element::Node)
        {
          // For the TraversalDirection::Any case, we use a view on relationships table that duplicates
          // the relationships to have the symmetrical relationships as well.
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
          const std::string curFromField = relationshipTableJoinAlias + ((traversalDirection == TraversalDirection::Backward) ? ".DestinationID" : ".OriginID");
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
          propertyMappingCypherToSQL[var][m_idProperty.name] = idField;
        std::string sqlFilter;
        if(!toEquivalentSQLFilter(idFilters, {m_idProperty}, propertyMappingCypherToSQL, sqlFilter, sqlVars))
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

    const char*msg{};
    if(auto res = sqlite3_exec(s.str(), [](void *p_queryInfo, int argc, Value *argv, char **column) {
      const auto t1 = std::chrono::system_clock::now();
      
      auto & queryInfo = *static_cast<RelationshipQueryInfo*>(p_queryInfo);
      
      for(size_t i{}, sz = queryInfo.countDistinctVariables; i<sz; ++i)
      {
        const auto & indexID = queryInfo.indexIDs[i];
        const auto & indexType = queryInfo.indexTypes[i];
        if(indexID.has_value() || indexType.has_value())
        {
          queryInfo.candidateRows[i].push_back(IDAndType<ID>{
            indexID.has_value() ? std::move(std::get<ID>(argv[*indexID])) : ID{},
            indexType.has_value() ? std::get<int64_t>(argv[*indexType]) : c_noType
          });
        }
      }
      
      const auto duration = std::chrono::system_clock::now() - t1;
      queryInfo.totalSystemRelationshipCbDuration += duration;
      return 0;
    }, &queryInfo, &msg, sqlVars))
      throw std::logic_error(msg);
  }

  // 2. Query the labeled node/entity property tables if needed.

  // split nodes, dualNodes, relationships by types (if needed)
  
  // indexed by varToVarIdx[var]
  std::vector<std::vector<PropertyKeyName>> strPropertiesByVar(countDistinctVariables);
  // indexed by varToVarIdx[var]
  std::vector<std::unordered_map<ID, std::vector<Value>>> propertiesByVar(countDistinctVariables);
  {
    const auto endElementType = getEndElementType();
    // indexed by element type
    std::vector<std::unordered_set<ID> /* element ids*/> elementsByType;
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
        elementsByType.resize(endElementType);
        for(auto & v : elementsByType)
          v.clear();
        for(const auto & idAndType : candidateRow)
        {
          if(idAndType.type == c_noType)
            continue;
          if constexpr (std::copyable<ID>)
            elementsByType[idAndType.type].insert(idAndType.id);
          else
            elementsByType[idAndType.type].insert(idAndType.id.clone());
        }
        // TODO: when we query the same labeled entity/relationship property tables for several variables,
        // instead of doing several queries we should do a single UNION ALL query
        // with an extra column containing the index of the variable.
        gatherPropertyValues(var, std::move(elementsByType), varToElement[var], props, postFilters, propertiesByVar[i]);
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
  std::vector<std::vector<Value>> propertyValues(countDistinctVariables);
  std::vector<bool> varOnlyReturnsId(countDistinctVariables);
  std::vector<bool> lookupProperties(countDistinctVariables);

  for(const auto & [var, returnedProperties] : variablesI)
  {
    const size_t i = varToVarIdx[var];
    vecReturnClauses[i] = &returnedProperties;
    propertyValues[i].resize(returnedProperties.size());
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
        if(p.propertyName != m_idProperty.name)
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

  size_t countReturnedRows{};
  for(size_t row{}; row<countRows; ++row)
  {
    if(limit.has_value() && countReturnedRows >= limit->maxCountRows)
      break;
    for(size_t i{}; i<countDistinctVariables; ++i)
    {
      if(lookupProperties[i])
      {
        if(varOnlyReturnsId[i])
        {
          auto & propValues = propertyValues[i];

          auto & id = candidateRows[i][row].id;
          if constexpr (std::copyable<ID>)
            std::fill(propValues.begin(), propValues.end(), id);
          else
          {
            for(auto & propValue : propValues)
            {
              // Potential optimization: we could move the last value instead of copying it.
              // candidateRows[i][row].id will not be used in this function anymore
              propValue = id.clone();
            }
          }
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
    ++countReturnedRows;
  nextRow:;
  }
}


template<typename ID>
void GraphDB<ID>::forEachElementPropertyWithLabelsIn(const Variable & var,
                                                 const Element elem,
                                                 const std::vector<ReturnClauseTerm>& returnClauseTerms,
                                                 const std::vector<std::string>& inputLabels,
                                                 const std::vector<const Expression*>* filter,
                                                 const std::optional<Limit>& limit,
                                                 FuncResults& f)
{
  sql::QueryVars sqlVars;

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
    std::vector<Value> m_values;
    const FuncResults& m_f;
    const std::vector<Variable> m_orderedVariables;
    const std::vector<const std::vector<Value>*> m_vecValues;
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
      if(!toEquivalentSQLFilter(*filter, m_properties[label], {}, sqlFilter, sqlVars))
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

  std::string req = s.str();
  if(!req.empty())
  {
    if(limit.has_value())
      req += " LIMIT " + std::to_string(limit->maxCountRows);
    const char*msg{};
    if(auto res = sqlite3_exec(req, [](void *p_results, int argc, Value *argv, char **column) {
      auto & results = *static_cast<Results*>(p_results);
      for(int i=0; i<argc; ++i)
        results.m_values[i] = std::move(argv[i]);
      results.m_f(results.m_resultsOrder, results.m_orderedVariables, results.m_vecColumnNames, results.m_vecValues);
      return 0;
    }, &results, &msg, sqlVars))
      throw std::logic_error(msg);
  }
}

template<typename ID>
auto GraphDB<ID>::computeResultOrder(const std::vector<const std::vector<ReturnClauseTerm>*>& vecReturnClauses) -> ResultOrder
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

template<typename ID>
void GraphDB<ID>::analyzeFilters(const ExpressionsByVarAndProperties& allFilters,
                             std::vector<const Expression*>& idFilters,
                             std::map<Variable, VariablePostFilters>& postFilters) const
{
  for(const auto & [varsAndProperties, expressions] : allFilters)
  {
    if(varsAndProperties.size() >= 2)
    {
      // 'expressions' use 2 or more variables
      
      if(countPropertiesNotEqual(m_idProperty.name, varsAndProperties) > 0)
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
      if(countPropertiesNotEqual(m_idProperty.name, varsAndProperties) > 0)
      {
        // At least one non-id property is used.
        VariablePostFilters & postFiltersForVar = postFilters[varsAndProperties.begin()->first];
        for(const PropertyKeyName & property : varsAndProperties.begin()->second)
          postFiltersForVar.properties.insert(property);
        postFiltersForVar.filters.insert(postFiltersForVar.filters.end(), expressions.begin(), expressions.end());
      }
      else if(varsAndProperties.begin()->second.count(m_idProperty.name))
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

template<typename ID>
bool GraphDB<ID>::varRequiresTypeInfo(const Variable& var,
                                  const std::vector<ReturnClauseTerm>& returnedProperties,
                                  const std::map<Variable, VariablePostFilters>& postFilters) const
{
  // If the return clause contains some non-id properties,
  // we need to know the type of the element because
  // we may need to query property tables to find the value of these properties
  // (unless these properties are not valid for the property table but this will be checked later,
  //  once we know the type of the element)
  for(const auto & p : returnedProperties)
    if(p.propertyName != m_idProperty.name)
      return true;
  
  // If there are some constraints on non-id properties,
  // we will have to query the property tables to know if the element is discarded.
  if(auto it = postFilters.find(var); it != postFilters.end())
  {
    // By construction of post filters, a non-id property will be used by this post-filter.
    
    // Sanity check
    bool hasNonIdProperty{};
    for(const auto & prop : it->second.properties)
      if(prop != m_idProperty.name)
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

template<typename ID>
std::string GraphDB<ID>::mkFilterTypesConstraint(const std::set<size_t>& typesFilter, std::string const& typeColumn)
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
template<typename ID>
void GraphDB<ID>::gatherPropertyValues(const Variable& var,
                                       // indexed by element type
                                       std::vector<std::unordered_set<ID>>&& elemsByType,
                                       const Element elem,
                                       const std::vector<PropertyKeyName>& propertyNames,
                                       const std::map<Variable, VariablePostFilters>& postFilters,
                                       std::unordered_map<ID, std::vector<Value>>& properties) const
{
  bool firstOutter = true;
  std::ostringstream s;
  sql::QueryVars sqlVars;

  const VariablePostFilters * postFilterForVar{};
  
  if(const auto it = postFilters.find(var); it != postFilters.end())
    postFilterForVar = &it->second;

  for(size_t typeIdx{}, sz = elemsByType.size(); typeIdx < sz; ++typeIdx)
  {
    auto & ids = elemsByType[typeIdx];
    if(ids.empty())
      continue;
    // typeIdx is guaranteed to be an existing type so we know getIfExists will return a value.
    const auto label = (elem == Element::Node)
    ? *m_indexedNodeTypes.getIfExists(typeIdx)
    : *m_indexedRelationshipTypes.getIfExists(typeIdx);
    
    std::vector<bool> validProperty;
    if(!findValidProperties(label, propertyNames, validProperty))
      throw std::logic_error("[Unexpected] Label does not exist.");
    std::string sqlFilter{};
    
    if(postFilterForVar && !postFilterForVar->filters.empty())
    {
      auto it = m_properties.find(label);
      if(it == m_properties.end())
        throw std::logic_error("[Unexpected] Label not found in properties.");
      if(!toEquivalentSQLFilter(postFilterForVar->filters, it->second, {}, sqlFilter, sqlVars))
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
            const bool isIDProperty = propertyNames[i] == m_idProperty.name;
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
        const auto countProperties = propertyNames.size();
        // no property is valid (except id properties), and we don't do any filtering
        // so we manually compute the results.
        for(auto & id : ids)
        {
          auto & vec = properties[cloneIfNeeded(id)];
          vec.resize(countProperties);
          const size_t sz2 = indicesValidIDProperties.size();
          for(size_t i{}; i<sz2; ++i)
          {
            const auto idxValidIDProperty = indicesValidIDProperties[i];
            if constexpr (std::copyable<ID>)
              vec[idxValidIDProperty] = id;
            else if (i == sz2 - 1)
              // Move the id for the last element
              vec[idxValidIDProperty] = std::move(const_cast<ID&>(id));   // Not sure why the const_cast is needed here...
            else
              vec[idxValidIDProperty] = id.clone();
          }
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
      const auto & propertyName = propertyNames[i];
      s << ", ";
      if(!validProperty[i])
        s << "NULL as ";
      s << propertyName;
    }
    s << " FROM " << label;

    auto vecIds = std::make_shared<typename CorrespondingVectorType<ID>::type>();
    vecIds->reserve(ids.size());
    for(auto & id : ids)
      vecIds->push_back(std::move(const_cast<ID&>(id)));  // Not sure why the const_cast is needed here...
    s << " WHERE SYS__ID IN " << sqlVars.addVar(std::move(vecIds));
    if(!sqlFilter.empty())
      s << " AND " << sqlFilter;
  }
  
  const std::string queryStr = s.str();
  if(!queryStr.empty())
  {
    struct QueryData{
      std::chrono::steady_clock::duration& totalPropertyTablesCbDuration;
      std::unordered_map<ID, std::vector<Value>> & properties;
    } queryData{m_totalPropertyTablesCbDuration, properties};
    
    if(auto res = sqlite3_exec(queryStr.c_str(), [](void *p_queryData, int argc, Value *argv, char **column) {
      const auto t1 = std::chrono::system_clock::now();
      
      auto & queryData = *static_cast<QueryData*>(p_queryData);
      {
        auto & props = queryData.properties[std::move(std::get<ID>(argv[0]))];
        for(int i=1; i<argc; ++i)
          props.push_back(std::move(argv[i]));
      }
      
      const auto duration = std::chrono::system_clock::now() - t1;
      queryData.totalPropertyTablesCbDuration += duration;
      return 0;
    }, &queryData, 0, sqlVars))
      throw std::logic_error(sqlite3_errstr(res));
  }
}

template<typename ID>
size_t GraphDB<ID>::getEndElementType() const
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

template<typename ID>
int GraphDB<ID>::sqlite3_exec(const std::string& queryStr,
                          int (*callback)(void*, int, Value*, char**),
                          void * cbParam,
                          const char **errmsg,
                          const sql::QueryVars& sqlVars) const
{
  m_fOnSQLQuery(queryStr);

  const auto t1 = std::chrono::system_clock::now();

  const auto res = sqlite3_exec_notime(queryStr, sqlVars, callback, cbParam, errmsg);

  const auto duration = std::chrono::system_clock::now() - t1;
  m_totalSQLQueryExecutionDuration += duration;

  m_fOnSQLQueryDuration(duration);

  return res;
}

template<typename ID>
int GraphDB<ID>::sqlite3_exec_notime(const std::string& queryStr,
                                 const sql::QueryVars& sqlVars,
                                 int (*callback)(void*, int, Value*, char**),
                                 void * cbParam,
                                 const char **errmsg) const
{
  SQLPreparedStatement stmt{};
  if(auto res = stmt.prepare(m_db, queryStr))
  {
    if(errmsg)
      *errmsg = sqlite3_errmsg(m_db);
    return res;
  }
  stmt.bindVariables(sqlVars);
  return stmt.run(callback, cbParam, errmsg);
}

template class GraphDB<int64_t>;
template class GraphDB<double>;
template class GraphDB<StringPtr>;
template class GraphDB<ByteArrayPtr>;
