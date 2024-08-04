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

void SQLPreparedStatement::bindVariable(int sqliteIndex, Value const& value) const
{
  std::visit([&](auto && arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, int64_t>)
    {
      if(sqlite3_bind_int64(m_stmt, sqliteIndex, arg))
        throw std::logic_error("sqlite3_bind_text: " + std::string{sqlite3_errmsg(m_db)});
    }
    else if constexpr (std::is_same_v<T, double>)
    {
      if(sqlite3_bind_double(m_stmt, sqliteIndex, arg))
        throw std::logic_error("sqlite3_bind_text: " + std::string{sqlite3_errmsg(m_db)});
    }
    else if constexpr (std::is_same_v<T, StringPtr>)
    {
      if(sqlite3_bind_text(m_stmt, sqliteIndex, arg.string.get(), static_cast<int>(arg.m_bufSz), SQLITE_STATIC))
        throw std::logic_error("sqlite3_bind_text: " + std::string{sqlite3_errmsg(m_db)});
    }
    else if constexpr (std::is_same_v<T, ByteArrayPtr>)
    {
      if(sqlite3_bind_blob(m_stmt, sqliteIndex, arg.bytes.get(), static_cast<int>(arg.m_bufSz), SQLITE_STATIC))
        throw std::logic_error("sqlite3_bind_text: " + std::string{sqlite3_errmsg(m_db)});
    }
    else if constexpr (std::is_same_v<T, Nothing>)
    {
      if(sqlite3_bind_null(m_stmt, sqliteIndex))
        throw std::logic_error("sqlite3_bind_null: " + std::string{sqlite3_errmsg(m_db)});
    }
    else
      static_assert(c_false<T>, "non-exhaustive visitor!");
  }, value);
}

void SQLPreparedStatement::bindVariables(const sql::QueryVars& sqlVars) const
{
  for(const auto & [i, v] : sqlVars.vars())
  {
    std::visit([&, i=i](auto && arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, std::monostate>)
      {
        // empty list
        if(sqlite3_carray_bind(m_stmt, i, 0, 0, CARRAY_INT64, SQLITE_STATIC))
          throw std::logic_error("Bind: " + std::string{sqlite3_errmsg(m_db)});
      }
      else if constexpr (std::is_same_v<T, std::shared_ptr<std::vector<int64_t>>>)
      {
        if(sqlite3_carray_bind(m_stmt, i, const_cast<int64_t*>(arg->data()), static_cast<int>(arg->size()), CARRAY_INT64, SQLITE_STATIC))
          throw std::logic_error("Bind: " + std::string{sqlite3_errmsg(m_db)});
      }
      else if constexpr (std::is_same_v<T, std::shared_ptr<std::vector<double>>>)
      {
        if(sqlite3_carray_bind(m_stmt, i, const_cast<double*>(arg->data()), static_cast<int>(arg->size()), CARRAY_DOUBLE, SQLITE_STATIC))
          throw std::logic_error("Bind: " + std::string{sqlite3_errmsg(m_db)});
      }
      else if constexpr (std::is_same_v<T, std::shared_ptr<Strings>>)
      {
        // expects an array of char*
        if(sqlite3_carray_bind(m_stmt, i, const_cast<char**>(arg->stringsArray.data()), static_cast<int>(arg->stringsArray.size()), CARRAY_TEXT, SQLITE_STATIC))
          throw std::logic_error("Bind: " + std::string{sqlite3_errmsg(m_db)});
      }
      else if constexpr (std::is_same_v<T, std::shared_ptr<ByteArrays>>)
      {
        // expects an array of iovec
        if(sqlite3_carray_bind(m_stmt, i, const_cast<iovec*>(arg->iovecs.data()), static_cast<int>(arg->iovecs.size()), CARRAY_BLOB, SQLITE_STATIC))
          throw std::logic_error("Bind: " + std::string{sqlite3_errmsg(m_db)});
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, v);
  }
}

int SQLPreparedStatement::run(int (*callback)(void*, int, Value*, char**),
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
        m_rowResults[i] = columnToValue(i);
      if(auto cbRes = callback(cbParam,
                               m_nCols,
                               m_rowResults.data(),
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
