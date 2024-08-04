
namespace openCypher::test
{
template<typename ID>
GraphWithStats<ID>::GraphWithStats(const std::optional<std::filesystem::path>& dbPath,
                               std::optional<Overwrite> overwrite)
{
  auto onSQLQuery = [&](const std::string& req)
  {
    m_queryStats.push_back(SQLQueryStat{req});
    if(m_printSQLRequests)
    {
      //auto req = reqLarge.substr(0, 700);
      bool first = true;
      for(const auto & part1 : splitOn("UNION ALL ", req))
        for(const auto & part : splitOn("INNER JOIN ", part1))
        {
          std::cout << LogIndent{};
          if(first)
          {
            first = false;
            std::cout << "[SQL] ";
          }
          else
            std::cout << "      ";
          std::cout << part << std::endl;
        }
    }
  };
  auto onSQLQueryDuration = [&](const std::chrono::steady_clock::duration d)
  {
    m_queryStats.back().duration = d;
    if(m_printSQLRequestsDuration)
    {
      std::cout << std::chrono::duration_cast<std::chrono::microseconds>(d).count() << " us" << std::endl;
    }
  };
  auto onDBDiagnosticContent = [&](int argc, Value *argv, char **column)
  {
    if(m_printSQLRequests)
    {
      auto _ = LogIndentScope{};
      std::cout << LogIndent{};
      for (int i=0; i < argc; i++)
        std::cout << argv[i] << ",\t";
      std::cout << std::endl;
    }
    return 0;
  };

  m_graph = std::make_unique<GraphDB<ID>>(onSQLQuery, onSQLQueryDuration, onDBDiagnosticContent, dbPath, overwrite);
}

template<typename ID>
void QueryResultsHandler<ID>::run(const std::string &cypherQuery,
                              const std::map<SymbolicName, HomogeneousNonNullableValues>& Params)
{
  m_db.m_queryStats.clear();
  
  auto sqlDuration1 = m_db.getDB().m_totalSQLQueryExecutionDuration;
  auto relCbDuration1 = m_db.getDB().m_totalSystemRelationshipCbDuration;
  auto propCbDuration1 = m_db.getDB().m_totalPropertyTablesCbDuration;
  
  m_tCallRunCypher = std::chrono::steady_clock::now();
  
  openCypher::runCypher(cypherQuery, Params, m_db.getDB(), *this);
  
  m_cypherQueryDuration = (std::chrono::steady_clock::now() - m_tCallRunCypher) - m_cypherToASTDuration;
  
  auto sqlDuration2 = m_db.getDB().m_totalSQLQueryExecutionDuration;
  auto relCbDuration2 = m_db.getDB().m_totalSystemRelationshipCbDuration;
  auto propCbDuration2 = m_db.getDB().m_totalPropertyTablesCbDuration;
  
  m_sqlQueriesExecutionDuration = sqlDuration2 - sqlDuration1;
  m_sqlRelCbDuration = relCbDuration2 - relCbDuration1;
  m_sqlPropCbDuration = propCbDuration2 - propCbDuration1;
}

template<typename ID>
void QueryResultsHandler<ID>::onCypherQueryStarts(std::string const & cypherQuery)
{
  m_cypherToASTDuration = std::chrono::steady_clock::now() - m_tCallRunCypher;
  
  m_rows.clear();
  
  if(m_printCypherQueryText)
  {
    std::cout << std::endl;
    std::cout << "[openCypher] " << cypherQuery << std::endl;
    m_logIndentScope = std::make_unique<LogIndentScope>();
  }
}

template<typename ID>
void QueryResultsHandler<ID>::onOrderAndColumnNames(const ResultOrder& ro, const std::vector<openCypher::Variable>& vars, const VecColumnNames& colNames) {
  m_resultOrder = ro;
  m_variables = vars;
  m_columnNames = colNames;
}

template<typename ID>
void QueryResultsHandler<ID>::onRow(const VecValues& values)
{
  if(m_printCypherRows)
  {
    auto _ = LogIndentScope();
    std::cout << LogIndent{};
    for(const auto & [i, j] : m_resultOrder)
      std::cout << m_variables[i] << "." << (*m_columnNames[i])[j] << " = " << (*values[i])[j] << '|';
    std::cout << std::endl;
  }
  auto & row = m_rows.emplace_back();
  row.reserve(m_resultOrder.size());
  for(const auto & [i, j] : m_resultOrder)
    row.push_back(copy((*values[i])[j]));
}

template<typename ID>
void QueryResultsHandler<ID>::onCypherQueryEnds()
{
  m_logIndentScope.release();
}


}
