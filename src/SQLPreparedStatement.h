#pragma once

#include "sqlite3.h"
#include "SqlAST.h"

#include "Value.h"
#include <string>



struct SQLBoundVarIndex
{
  int next() { return m_boundVarIndex++; }
  std::string nextAsStr() { return "?" + std::to_string(next()); }
  
private:
  // in SQL, bound variables indices start at 1.
  int m_boundVarIndex{1};
};


struct SQLPreparedStatement{
  SQLPreparedStatement() = default;
  SQLPreparedStatement(SQLPreparedStatement const &) = delete;
  SQLPreparedStatement(SQLPreparedStatement&&) = delete;
  SQLPreparedStatement const & operator =(SQLPreparedStatement const &) = delete;
  SQLPreparedStatement const & operator =(SQLPreparedStatement &&) = delete;
  
  // queryStr may contain ?1 ?2 ... i.e bound variables placeholders.
  int prepare(sqlite3* db, const std::string& queryStr);

  // |sqliteIndex| starts at 1
  void bindVariable(int sqliteIndex, Value const& value) const;
  void bindVariable(int sqliteIndex, const int64_t) const;
  void bindVariable(int sqliteIndex, const double) const;
  void bindVariable(int sqliteIndex, const Nothing) const;
  void bindVariable(int sqliteIndex, const StringPtr&) const;
  void bindVariable(int sqliteIndex, const ByteArrayPtr&) const;
  void bindVariables(const sql::QueryVars& sqlVars) const;
  void reset();

  // if propertyTypes is not null, it contains the type of each column.
  // When propertyTypes is null, all values are converted to strings.
  int run(int (*callback)(void*,int, Value*,char**),
          void * cbParam,
          const char **errmsg);
  ~SQLPreparedStatement();
  
private:
  // Owner.
  sqlite3_stmt* m_stmt{};
  // Not owner.
  sqlite3* m_db{};

  int m_nCols;
  
  std::vector<Value> m_rowResults;
  std::vector<const char*> m_colNames;

  int countColumns() const { return m_nCols; }
  
  int step() const;
  
  const char* column_name(int i)
  {
    return sqlite3_column_name(m_stmt, i);
  }
  Value columnToValue(int i)
  {
    const auto type = sqlite3_column_type(m_stmt, i);
    switch(type)
    {
      case SQLITE_NULL:
        return Nothing{};
      case SQLITE_INTEGER:
        return sqlite3_column_int64(m_stmt, i);
      case SQLITE_FLOAT:
        return sqlite3_column_double(m_stmt, i);
      case SQLITE_TEXT:
      {
        const auto * s = sqlite3_column_text(m_stmt, i);
        const int sz = sqlite3_column_bytes(m_stmt, i);
        // sz is the number of bytes excluding the terminating character,
        // so for "ABC", sz = 3.
        return StringPtr::fromCStrAndCountBytes(s, sz);
      }
      case SQLITE_BLOB:
      {
        const auto * b = sqlite3_column_blob(m_stmt, i);
        const int sz = sqlite3_column_bytes(m_stmt, i);
        return ByteArrayPtr::fromByteArray(b, sz);
      }
      default:
        throw std::logic_error("unexpected column type " + std::to_string(type));
    }
  }

};
