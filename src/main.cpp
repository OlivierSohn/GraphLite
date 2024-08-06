#include <string>
#include <exception>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"

int main()
{
  struct PrettyPrintQueryResults
  {
    bool printCypherAST() const { return false; }

    void onCypherQueryStarts(std::string const & cypherQuery)
    {
      std::cout << std::endl;
      std::cout << "[openCypher] " << cypherQuery << std::endl;
      m_logIndentScope = std::make_unique<LogIndentScope>();
    }
    void onOrderAndColumnNames(const ResultOrder& ro, const std::vector<openCypher::Variable>& vars, const VecColumnNames& colNames) {
      m_resultOrder = ro;
      m_variables = vars;
      m_columnNames = colNames;
    }
    
    void onRow(const VecValues& values)
    {
      auto _ = LogIndentScope();
      std::cout << LogIndent{};
      for(const auto & [i, j] : m_resultOrder)
        std::cout << m_variables[i] << "." << (*m_columnNames[i])[j] << " = " << (*values[i])[j] << '|';
      std::cout << std::endl;
    }
    void onCypherQueryEnds()
    {
      m_logIndentScope.reset();
    }

  private:
    std::unique_ptr<LogIndentScope> m_logIndentScope;
    ResultOrder m_resultOrder;
    std::vector<openCypher::Variable> m_variables;
    VecColumnNames m_columnNames;
  };
  
  const bool printSQLRequests{true};
  auto onSQLQuery = [&](const std::string& req)
  {
    if(printSQLRequests)
    {
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
  auto onSQLQueryDuration = [&](const std::chrono::steady_clock::duration& )
  {
  };
  auto onDBDiagnosticContent = [&](int argc, Value *argv, char **column)
  {
    if(printSQLRequests)
    {
      auto _ = LogIndentScope{};
      std::cout << LogIndent{};
      for (int i=0; i < argc; i++)
        std::cout << argv[i] << ",\t";
      std::cout << std::endl;
    }
    return 0;
  };

  
  GraphDB db(onSQLQuery, onSQLQueryDuration, onDBDiagnosticContent);

  using openCypher::mkProperty;
  const auto p_test = mkProperty("test");
  const auto p_what = mkProperty("what");
  const auto p_testRel = mkProperty("testRel");
  const auto p_whatRel = mkProperty("whatRel");

  {
    LogIndentScope _ = logScope(std::cout, "Creating Entity and Relationship types...");
    db.addType("Node1", true, {p_test});
    db.addType("Node2", true, {p_test, p_what});
    db.addType("Rel1", false, {p_testRel, p_whatRel});
    db.addType("Rel2", false, {p_testRel, p_whatRel});
  }
  LogIndentScope sER = logScope(std::cout, "Creating Entities and Relationships...");
  auto n1 = db.addNode("Node1", mkVec(std::pair{p_test, Value(3)}));
  auto n2 = db.addNode("Node2", mkVec(std::pair{p_test, Value(4)}, std::pair{p_what, Value(55)}));
  auto r12 = db.addRelationship("Rel1", n1, n2, mkVec(std::pair{p_whatRel, Value(0)}));
  auto r22 = db.addRelationship("Rel2", n2, n2, mkVec(std::pair{p_testRel, Value(2)}, std::pair{p_whatRel, Value(1)}));
  sER.endScope();

  {
    LogIndentScope _ = logScope(std::cout, "Printing SQL DB content...");
    db.print();
  }

  auto runCypher = [&](const std::string &cypherQuery)
  {
    PrettyPrintQueryResults ppHandler;
    openCypher::runCypher(cypherQuery, {}, db, ppHandler);
  };

  try
  {
    runCypher("MATCH (`n`) WHERE n:Node1      RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`n`) WHERE n:Node1 AND n:Node2     RETURN id(`n`), `n`.test, `n`.`what`;");
    // this is ok
    runCypher("MATCH (`n`) WHERE n:Node1 OR n.test = 2     RETURN id(`n`), `n`.test, `n`.`what`;");
    
    // but verify this throws
    runCypher("MATCH ((n)-[r]->(m)) WHERE n:Node1 OR m.test = 2     RETURN id(`n`), `n`.test, `n`.`what`;");

    runCypher("MATCH (`n`)       RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`n`:Node1) RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`n`:Node2) RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH ()-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`;");

    runCypher("MATCH (`n`:Node1)-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");
    runCypher("MATCH ()<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");

    runCypher("MATCH (:Node2)<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");
    runCypher("MATCH (:Node2)<-[]-(`n`:Node1) RETURN `n`.test;");

    // returns nothing because of the 'Test' label constraint in the anonymous part.
    runCypher("MATCH (:Test)<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");

    // node and dual node properties
    runCypher("MATCH (`m`:Node2)<-[`r`]-(`n`:Node1) RETURN id(`m`), id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");
    runCypher("MATCH (`m`:Node2)<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test, `m`.test;");
    runCypher("MATCH (`m`:Node2)<-[]-(`n`:Node1) RETURN id(`m`), `n`.test;");

    // Where clause with id or property lookup
    runCypher("MATCH (`n`)       WHERE n.test = 3   RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`m`)<-[`r`]-(`n`) WHERE id(n) = 1 RETURN id(m), id(n), id(`r`), `m`.test;");
    runCypher("MATCH (`m`)<-[`r`]-(`n`) WHERE id(m) = 1 RETURN id(m), id(n), id(`r`), `n`.test;");

    // where clause with multiple terms.
    runCypher("MATCH (`n`)       WHERE n.test >= 2.5 AND n.test <= 3.5   RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`n`)       WHERE n.test >= 2.5 OR n.test <= 3.5   RETURN id(`n`), `n`.test, `n`.`what`;");
    // Here, the SQL query is not done against the table of Node2 because it doesn't have the 'what' property.
    runCypher("MATCH (`n`)       WHERE n.what >= 50 AND n.what <= 60   RETURN id(`n`), `n`.test, `n`.`what`;");

    // Here, (n.test >= 2.5 AND n.test <= 3.5) matches the node of type Node1,
    // and (n.what >= 50 AND n.what <= 60) matches the node of type Node2
    runCypher("MATCH (`n`)       WHERE (n.test >= 2.5 AND n.test <= 3.5) OR (n.what >= 50 AND n.what <= 60) OR n.who = 2  RETURN id(`n`), `n`.test, `n`.`what`;");

    runCypher("MATCH (`n`)-[r]-(`m`)       WHERE (n.test >= 2.5 AND n.test <= 3.5) OR (n.what >= 50 AND n.what <= 60) AND n.who = 2  RETURN id(`n`), `n`.test, `n`.`what`, id(m), id(r);");

    runCypher("MATCH (e1)-[r1]->(e2)-[r2]->(e3) WHERE (e1.test >= 2.5 AND e1.test <= 3.5) RETURN id(e1), id(e2), id(e3);");
    runCypher("MATCH (e1)-[r1]->(e2)-[r2]->(e2) WHERE (e1.test >= 2.5 AND e1.test <= 3.5) RETURN id(e1), id(e2);");
    runCypher("MATCH (e1)-[]->()-[r2]->(e2) WHERE (e1.test >= 2.5 AND e1.test <= 3.5) RETURN id(e1), id(e2);");

    runCypher("MATCH (`n`)  RETURN id(`n`) LIMIT 1;");
    runCypher("MATCH ()-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel` LIMIT 1;");
    runCypher("MATCH ()-[`r`]->(a) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, id(a) LIMIT 1;");
    runCypher("MATCH ()-[`r`]->()-[]->(a) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, id(a) LIMIT 1;");
    runCypher("MATCH ()-[`r`]->()-[]->(a) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, id(a) LIMIT 0;");

    // todo deduce labels from where clause:
    //runCypher("MATCH (`n`) WHERE n:Node1 OR n:Node2 RETURN id(`n`), `n`.test, `n`.`what`;");
    // - and move the top such expressions out of the AND maximal aggregation to convert them to label constraints
    // and AND them with path pattern labels (if the AND results in an empty intersection, return)
    // so that they can be applied during the system relationships query.
    // - We have the guarantee that idFilters won't contain any of them so we don't need an element type

    // todo suport creating an index on a property type.

    // todo optimize LIMIT implementation for path patterns, to reduce the numbers of SQL rows fetched:
    // when we may post-filter we could use pagination with exponential size increase:
    //     page_size = std::max(10000, 10 * limit->maxCountRows);
    //     then at each iteration
    //       page_size *= 2;
    //   the worst case is if there are many relationships (~100 millions) and the post filtering only allows one:
    //     MATCH (a)-[r]->(b) WHERE b.name = 'Albert Einstein' RETURN id(r) LIMIT 1
    //     MATCH (a)-[r]->(b) WHERE b.name = 'Albert Einstein' AND a.name = 'xyz' RETURN id(r) LIMIT 1
    //         in this case we should rather start querying for b
    //         (if there is an index on 'name', OR if the number of rows is much smaller than the number of relationships),
    //         using the post filter constraint, and then only query
    //         the system relationships table with id(B) IN (...)
    //       Generalization: we could first query ids of nodes and relationships that are constrained only by their own properties,
    //         then inject this information in the system relationships query that assembles the paths.
    //         Maybe we could be smart and guess how many rows would be returned for different node & relationship types,
    //         to only pre-filter those that will potentially return few rows,
    //         and post-filter the rest.

    // todo variable-length relationships: (a)-[r1:*..3]->(b)

    // todo RETURN entire elements

    // todo support non-equi-var expressions, by evaluating them manually before returning results.
    // i.e: WHERE n.weight > 3 OR r.status = 2

    // TODO support UNION

    // todo in MATCH (`n`)-[r]-(`m`) WHERE ... RETURN ...,
    //   and the where clause contains no filtering on ids:
    //   if there is no constraint on relationships, and there are constraints on nodes types and/or properties,
    //     it may be faster to start by querying the non-system nodes tables, deduce which nodes ids are relevant, and then
    //     use these ids to filter the relationships table.
    //   -> create a test example that shows the perf issue before trying to fix it.
    
    // todo (property value in the node pattern)
    //runCypher("MATCH (`n`:Node1{test=2})-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");
    //runCypher("MATCH (`n`:{test=2})-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");

    return 0;
  }
  catch(const std::exception& e)
  {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
