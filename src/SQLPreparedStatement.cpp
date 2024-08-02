#include "SQLPreparedStatement.h"
#include "sqlext/carray.h"


int SQLPreparedStatement::prepare(sqlite3* db, const std::string& queryStr)
{
  m_db = db;
  auto res = sqlite3_prepare_v2(db, queryStr.c_str(), static_cast<int>(queryStr.size() + 1ull), &m_stmt, nullptr);
  if(!res)
    m_nCols = sqlite3_column_count(m_stmt);
  return res;
}

void SQLPreparedStatement::bindVariable(int sqliteIndex, const std::string& value) const
{
  if(sqlite3_bind_text(m_stmt, sqliteIndex, value.c_str(), value.size(), SQLITE_STATIC))
    throw std::logic_error("BindTxt: " + std::string{sqlite3_errmsg(m_db)});
}
void SQLPreparedStatement::bindVariable(int sqliteIndex, const int64_t value) const
{
  if(sqlite3_bind_int64(m_stmt, sqliteIndex, value))
    throw std::logic_error("BindI64: " + std::string{sqlite3_errmsg(m_db)});
}

void SQLPreparedStatement::bindVariables(const sql::QueryVars& sqlVars) const
{
  for(const auto & [i, v] : sqlVars.vars())
    if(sqlite3_carray_bind(m_stmt, i, const_cast<int64_t*>(v.data()), static_cast<int>(v.size()), CARRAY_INT64, SQLITE_STATIC))
      throw std::logic_error("Bind: " + std::string{sqlite3_errmsg(m_db)});
}

int SQLPreparedStatement::run(int (*callback)(void*,int,char**,char**),
                              void * cbParam,
                              const char **errmsg)
{
  if(errmsg)
    *errmsg = nullptr;

  bool firstRow = true;
  
nextRow:
  int res = step();
  if(res == SQLITE_ROW)
  {
    if(callback)
    {
      if(firstRow)
      {
        firstRow = false;
        m_rowResults.resize(m_nCols);
        m_colNames.resize(m_nCols);
        for(int i=0; i<m_nCols; ++i)
          m_colNames[i] = column_name(i);
      }
      for(int i=0; i<m_nCols; ++i)
        m_rowResults[i] = reinterpret_cast<const char*>(column_text(i));
      if(auto cbRes = callback(cbParam,
                               m_nCols,
                               const_cast<char**>(m_rowResults.data()),
                               const_cast<char**>(m_colNames.data())))
      {
        return SQLITE_ABORT;
      }
    }
    goto nextRow;
  }
  else if(res == SQLITE_DONE)
    return SQLITE_OK;
  else
  {
    if(errmsg)
      *errmsg = sqlite3_errmsg(m_db);
  }
  return res;
}

void SQLPreparedStatement::reset() {
  if(sqlite3_reset(m_stmt))
    throw std::logic_error("Reset: " + std::string{sqlite3_errmsg(m_db)});
}

int SQLPreparedStatement::step() const
{
  return sqlite3_step(m_stmt);
}

SQLPreparedStatement::~SQLPreparedStatement()
{
  sqlite3_finalize(m_stmt);
}
