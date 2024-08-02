#pragma once

#include "sqlite3.h"
#include "SqlAST.h"

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
  void bindVariable(int sqliteIndex, const std::string& value) const;
  void bindVariable(int sqliteIndex, int64_t value) const;
  void bindVariables(const sql::QueryVars& sqlVars) const;
  void reset();

  int run(int (*callback)(void*,int,char**,char**),
          void * cbParam,
          const char **errmsg);
  ~SQLPreparedStatement();
  
private:
  // Owner.
  sqlite3_stmt* m_stmt{};
  // Not owner.
  sqlite3* m_db{};

  int m_nCols;
  
  std::vector<const char*> m_rowResults;
  std::vector<const char*> m_colNames;

  int countColumns() const { return m_nCols; }
  
  int step() const;
  
  const char* column_name(int i)
  {
    return sqlite3_column_name(m_stmt, i);
  }
  const unsigned char* column_text(int i)
  {
    return sqlite3_column_text(m_stmt, i);
  }
  
};
