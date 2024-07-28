#include <string>
#include <exception>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"

int main()
{
  struct PrettyPrintQueryResults
  {
    PrettyPrintQueryResults(std::string const & cypherQuery)
    {
      std::cout << std::endl;
      std::cout << "[openCypher] " << cypherQuery << std::endl;
      m_logIndentScope = std::make_unique<LogIndentScope>();
    }
    void onOrderAndColumnNames(const GraphDB::ResultOrder& ro, const std::vector<std::string>& varNames, const GraphDB::VecColumnNames& colNames) {
      m_resultOrder = ro;
      m_variablesNames = varNames;
      m_columnNames = colNames;
    }
    
    void onRow(const GraphDB::VecValues& values)
    {
      auto _ = LogIndentScope();
      std::cout << LogIndent{};
      for(const auto & [i, j] : m_resultOrder)
        std::cout << m_variablesNames[i] << "." << (*m_columnNames[i])[j] << " = " << (*values[i])[j].value_or("<null>") << '|';
      std::cout << std::endl;
    }
    
  private:
    std::unique_ptr<LogIndentScope> m_logIndentScope;
    GraphDB::ResultOrder m_resultOrder;
    std::vector<std::string> m_variablesNames;
    GraphDB::VecColumnNames m_columnNames;
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
  auto onDBDiagnosticContent = [](int argc, char **argv, char **column)
  {
    if(printSQLRequests)
    {
      auto _ = LogIndentScope{};
      std::cout << LogIndent{};
      for (int i=0; i < argc; i++)
        printf("%s,\t", argv[i]);
      printf("\n");
    }
    return 0;
  };

  
  GraphDB db(onSQLQuery, onDBDiagnosticContent);

  {
    LogIndentScope _ = logScope(std::cout, "Creating Entity and Relationship types...");
    db.addType("Node1", true, {"test"});
    db.addType("Node2", true, {"test", "what"});
    db.addType("Rel1", false, {"testRel", "whatRel"});
    db.addType("Rel2", false, {"testRel", "whatRel"});
  }
  LogIndentScope sER = logScope(std::cout, "Creating Entities and Relationships...");
  auto n1 = db.addNode("Node1", {{"test", "3"}});
  auto n2 = db.addNode("Node2", {{"test", "4"}, {"what", "55"}});
  auto r12 = db.addRelationship("Rel1", n1, n2, {{"whatRel", ".44"}});
  auto r22 = db.addRelationship("Rel2", n2, n2, {{"testRel", ".1"}, {"whatRel", ".55"}});
  sER.endScope();

  //db.writeToDisk();
  {
    LogIndentScope _ = logScope(std::cout, "Printing SQL DB content...");
    db.print();
  }

  auto runCypher = [&](const std::string &cypherQuery)
  {
    openCypher::runCypher<PrettyPrintQueryResults>(cypherQuery, db);
  };

  try
  {
    // Where clause with id or property lookup
    runCypher("MATCH (`n`)       WHERE n.test = 3   RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`m`)<-[`r`]-(`n`) WHERE id(n) = 1 RETURN id(m), id(n), id(`r`), `m`.test;");
    runCypher("MATCH (`m`)<-[`r`]-(`n`) WHERE id(m) = 1 RETURN id(m), id(n), id(`r`), `n`.test;");
    
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

    // where clause with multiple terms.
    runCypher("MATCH (`n`)       WHERE n.test >= 2.5 AND n.test <= 3.5   RETURN id(`n`), `n`.test, `n`.`what`;");
    runCypher("MATCH (`n`)       WHERE n.test >= 2.5 OR n.test <= 3.5   RETURN id(`n`), `n`.test, `n`.`what`;");
    // Here, the SQL query is not done against the table of Node2 because it doesn't have the 'what' property.
    runCypher("MATCH (`n`)       WHERE n.what >= 50 AND n.what <= 60   RETURN id(`n`), `n`.test, `n`.`what`;");

    // Here, (n.test >= 2.5 AND n.test <= 3.5) matches the node of type Node1,
    // and (n.what >= 50 AND n.what <= 60) matches the node of type Node2
    runCypher("MATCH (`n`)       WHERE (n.test >= 2.5 AND n.test <= 3.5) OR (n.what >= 50 AND n.what <= 60) OR n.who = 2  RETURN id(`n`), `n`.test, `n`.`what`;");

    runCypher("MATCH (`n`)-[r]-(`m`)       WHERE (n.test >= 2.5 AND n.test <= 3.5) OR (n.what >= 50 AND n.what <= 60) AND n.who = 2  RETURN id(`n`), `n`.test, `n`.`what`, id(m), id(r);");
    
    // todo write some unit tests
    
    // todo write some performance tests
    // - verify filtering on ids works as intended:
    //     using a very large graph, find nodes one hop away from a given node and compare with/without prefiltering on ids.

    // TODO support UNION

    // todo in MATCH (`n`)-[r]-(`m`) WHERE ... RETURN ...,
    //   and the where clause contains no filtering on ids:
    //   if there is no constraint on relationships, and there are constraints on nodes types and/or properties,
    //     it may be faster to start by querying the non-system nodes tables, deduce which nodes ids are relevant, and then
    //     use these ids to filter the relationships table.
    //   -> create a test example that shows the perf issue before trying to fix it.

    // todo longer path patterns: (a)-[r1]->(b)-[r2]->(c)
    // todo longer path patterns: (a)-[r1:*..3]->(b)
    
    // todo deduce labels from where clause (used in FFP):
    //runCypher("MATCH (`n`) WHERE n:Node1 OR n:Node2 RETURN id(`n`), `n`.test, `n`.`what`;");

    // todo (property value)
    //runCypher("MATCH (`n`:Node1{test=2})-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");

    // todo same as above, verify we do a union on "entity types that have a test property"
    //runCypher("MATCH (`n`:{test=2})-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;");

    return 0;
  }
  catch(const std::exception& e)
  {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
