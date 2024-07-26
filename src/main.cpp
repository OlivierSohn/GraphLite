#include <string>
#include <exception>
#include "DBSqlite.h"
#include "cypherparser/CypherParser.h"
#include "cypherparser/CypherLexer.h"
#include "cypherparser/CypherListener.h"
#include "MyCypherVisitor.h"
#include "CypherAST.h"
#include "antlr4-runtime.h"

SingleQuery cypherQueryToAST(const std::string& idProperty, const std::string& query)
{
  auto chars = antlr4::ANTLRInputStream(query);
  auto lexer = CypherLexer(&chars);
  auto tokens = antlr4::CommonTokenStream(&lexer);
  auto parser = CypherParser(&tokens);
  //parser.setBuildParseTree(true);
  CypherParser::OC_CypherContext* cypherTree = parser.oC_Cypher();
  
  bool printAST = true;
  auto visitor = MyCypherVisitor(idProperty, printAST);
  auto resVisit = visitor.visit(cypherTree);
  
  if(!visitor.getErrors().empty())
  {
    for(const auto& err : visitor.getErrors())
      std::cerr << err << std::endl;
    throw std::logic_error("Visitor errored.");
  }
  
  if(resVisit.type() != typeid(SingleQuery))
    throw std::logic_error("No SingleQuery was returned.");
  return std::any_cast<SingleQuery>(resVisit);;
}


std::unordered_map<std::string, std::vector<ReturnClauseTerm>>
extractProperties(const std::vector<NonArithmeticOperatorExpression>& naoExps)
{
  std::unordered_map<std::string, std::vector<ReturnClauseTerm>> props;
  size_t i{};
  for(const auto & nao : naoExps)
  {
    const auto& mayPropertyName = nao.mayPropertyName;
    if(!mayPropertyName.has_value())
      throw std::logic_error("Not Implemented (todo return 'entire node'?)");
    // TODO support Literal in return clauses.
    const auto & var = std::get<Variable>(nao.atom.var);
    auto & elem = props[var.symbolicName.str].emplace_back();
    elem.returnClausePosition = i;
    elem.propertyName = mayPropertyName->symbolicName.str;
    ++i;
  }
  return props;
}

std::vector<std::string> asStringVec(Labels const & labels)
{
  std::vector<std::string> labelsStr;
  labelsStr.reserve(labels.labels.size());
  for(const auto & label : labels.labels)
    labelsStr.push_back(label.symbolicName.str);
  return labelsStr;
}

void runSingleQuery(const SingleQuery& q, DB& db)
{
  const auto & spq = q.singlePartQuery;
  if(!spq.mayReadingClause.has_value())
    throw std::logic_error("Not Implemented (Expected a reading clause)");
  const auto & matchPatternParts = spq.mayReadingClause->match.pattern.patternParts;
  if(matchPatternParts.size() != 1)
    throw std::logic_error("Not Implemented (Expected a single pattern part)");
  const auto & mpp = matchPatternParts[0];
  if(mpp.mayVariable.has_value())
    throw std::logic_error("Not Implemented (Expected no variable before match pattern)");

  // Verify the clause has this form:
  //   <node or dualNode or rel>.<property> cmp literal
  std::unordered_map<std::string /*node or dualNode or rel*/, PropertyAndPCE> whereTermsByVar;
  if(spq.mayReadingClause->match.where.has_value())
  {
    const auto& ce = spq.mayReadingClause->match.where->exp;
    if(!ce.leftExp.mayPropertyName.has_value())
      throw std::logic_error("Expected a property name");

    SimplePartialComparisonExpression spce;
    spce.comp = ce.partial.comp;
    if(ce.partial.rightExp.mayPropertyName.has_value())
      // cases:
      // - WHERE a.test1 < a.test2
      // - WHERE a.test < b.test
      throw std::logic_error("Property name not supported yet");
    // Cannot be a Variable (todo support this later, see comment above)
    spce.literal = std::get<Literal>(ce.partial.rightExp.atom.var);

    // Canot be a Literal
    auto res = whereTermsByVar.emplace(std::get<Variable>(ce.leftExp.atom.var).symbolicName.str,
                                       PropertyAndPCE{ce.leftExp.mayPropertyName->symbolicName.str, std::move(spce)});
    if(!res.second)
      throw std::logic_error("Could not insert term");
  }

  const auto & app = mpp.anonymousPatternPart;

  const std::unordered_map<std::string, std::vector<ReturnClauseTerm>> props =
  extractProperties(spq.returnClause.naoExps);

  auto nodePatternIsActive = [&](const NodePattern& np)
  {
    if(np.mayVariable.has_value() && props.count(np.mayVariable->symbolicName.str))
      // the node pattern has a variable AND this variable is used in the return clause.
      return true;
    return !np.labels.labels.empty();
  };
  
  const bool leftNodeVarIsActive = nodePatternIsActive(app.firstNodePattern);
  const bool rightNodeVarIsActive = (app.patternElementChains.size() == 1) && nodePatternIsActive(app.patternElementChains[0].nodePattern);

  if((app.patternElementChains.size() == 1) && (leftNodeVarIsActive || rightNodeVarIsActive))
  {
    // In this branch we support triplets (Node)-[Rel]-(DualNode) where:
    // - Node, DualNode may be:
    //   - a variable name, or
    //   - labels, or
    //   - both, or
    //   - nothing (but we cannot have both Node and DualNode be nothing)
    // - Rel may be
    //   - a variable name, or
    //   - labels, or
    //   - both, or
    //   - nothing
    // - Relationship direction can be anything.

    // The first SQL query will be on the system relationships table, joined with the system nodes table
    // because we need to know some nodes types (or nodes properties).

    const auto& leftNodePattern = app.firstNodePattern;
    const auto& rightNodePattern = app.patternElementChains[0].nodePattern;

    const auto& nodePattern = leftNodeVarIsActive ? leftNodePattern : rightNodePattern;
    const auto& dualNodePattern = leftNodeVarIsActive ? rightNodePattern : leftNodePattern;

    const auto& nodeVariable = nodePattern.mayVariable;
    const auto& relVariable = app.patternElementChains[0].relPattern.mayVariable;
    const auto& dualNodeVariable = dualNodePattern.mayVariable;

    const auto& nodeLabels = nodePattern.labels;
    const auto& relLabels = app.patternElementChains[0].relPattern.labels;
    const auto& dualNodeLabels = dualNodePattern.labels;

    std::vector<ReturnClauseTerm> propertiesNode;
    std::vector<ReturnClauseTerm> propertiesRel;
    std::vector<ReturnClauseTerm> propertiesDualNode;

    if(nodeVariable.has_value())
      if(auto it = props.find(nodeVariable->symbolicName.str); it != props.end())
        propertiesNode = it->second;
    if(relVariable.has_value())
      if(auto it = props.find(relVariable->symbolicName.str); it != props.end())
        propertiesRel = it->second;
    if(dualNodeVariable.has_value())
      if(auto it = props.find(dualNodeVariable->symbolicName.str); it != props.end())
        propertiesDualNode = it->second;
    
    {
      // Sanity check.
      
      std::unordered_set<std::string> allVariables;
      if(nodeVariable.has_value())
        allVariables.insert(nodeVariable->symbolicName.str);
      if(relVariable.has_value())
        allVariables.insert(relVariable->symbolicName.str);
      if(dualNodeVariable.has_value())
        allVariables.insert(dualNodeVariable->symbolicName.str);

      for(const auto & [varName, _] : props)
        if(0 == allVariables.count(varName))
          throw std::logic_error("Variable '" + varName + "' not found.");
    }

    std::optional<PropertyAndPCE> nodeFilter;
    std::optional<PropertyAndPCE> relFilter;
    std::optional<PropertyAndPCE> dualNodeFilter;
    if(nodeVariable.has_value())
      if(const auto it = whereTermsByVar.find(nodeVariable->symbolicName.str); it != whereTermsByVar.end())
        nodeFilter = it->second;
    if(relVariable.has_value())
      if(const auto it = whereTermsByVar.find(relVariable->symbolicName.str); it != whereTermsByVar.end())
        relFilter = it->second;
    if(dualNodeVariable.has_value())
      if(const auto it = whereTermsByVar.find(dualNodeVariable->symbolicName.str); it != whereTermsByVar.end())
        dualNodeFilter = it->second;

    {
      auto f = std::function{[&](const std::vector<std::optional<std::string>>& nodePropertiesValues,
                                 const std::vector<std::optional<std::string>>& relationshipPropertiesValues,
                                 const std::vector<std::optional<std::string>>& dualNodePropertiesValues,
                                 const std::vector<std::string>& nodePropertiesNames,
                                 const std::vector<std::string>& relationshipPropertiesNames,
                                 const std::vector<std::string>& dualNodePropertiesNames){
        auto _ = LogIndentScope();
        std::cout << LogIndent{};
        // Slightly more work would be needed if we want to honor the order of items
        // specified in the return clause.
        for(size_t i=0, sz=nodePropertiesValues.size(); i<sz; ++i)
          std::cout << nodeVariable->symbolicName.str << "." << nodePropertiesNames[i] << " = " << (nodePropertiesValues[i].value_or("<null>")) << '|';
        for(size_t i=0, sz=relationshipPropertiesValues.size(); i<sz; ++i)
          std::cout << relVariable->symbolicName.str << "." << relationshipPropertiesNames[i] << " = " << (relationshipPropertiesValues[i].value_or("<null>")) << '|';
        for(size_t i=0, sz=dualNodePropertiesValues.size(); i<sz; ++i)
          std::cout << dualNodeVariable->symbolicName.str << "." << dualNodePropertiesNames[i] << " = " << (dualNodePropertiesValues[i].value_or("<null>")) << '|';
        std::cout << std::endl;
      }};
      const TraversalDirection traversalDirection = app.patternElementChains[0].relPattern.traversalDirection;
      db.forEachNodeAndRelatedRelationship(leftNodeVarIsActive ? traversalDirection : mirror(traversalDirection),
                                           propertiesNode,
                                           propertiesRel,
                                           propertiesDualNode,
                                           asStringVec(nodeLabels),
                                           asStringVec(relLabels),
                                           asStringVec(dualNodeLabels),
                                           nodeFilter,
                                           relFilter,
                                           dualNodeFilter,
                                           f);
    }
    return;
  }

  // In this branch we support:
  // - MATCH (`n`)
  // - MATCH ()-[`r`]->()
  
  // The SQL queries will be on non-system relationships tables and non-system nodes tables.

  const auto& nodePattern = app.firstNodePattern;
  const bool singleNodeVariable = nodePattern.mayVariable.has_value() && app.patternElementChains.empty();
  const bool singleRelationshipVariable =
  !nodePattern.mayVariable.has_value() &&
  (app.patternElementChains.size() == 1) &&
  app.patternElementChains[0].nodePattern.isTrivial() &&
  app.patternElementChains[0].relPattern.mayVariable.has_value();
  if(!singleNodeVariable && !singleRelationshipVariable)
    throw std::logic_error("Not Implemented (Expected a node or relationship variable)");
  if(singleNodeVariable && singleRelationshipVariable)
    throw std::logic_error("Impossible");

  const Element elem = singleNodeVariable ? Element::Node : Element::Relationship;
  
  const auto& variable = singleNodeVariable ? *nodePattern.mayVariable : *app.patternElementChains[0].relPattern.mayVariable;
  const auto& labels = singleNodeVariable ? nodePattern.labels : app.patternElementChains[0].relPattern.labels;

  if(spq.returnClause.naoExps.empty())
    throw std::logic_error("Not Implemented (Expected some non arithmetic expression)");
    
  const auto itProps = props.find(variable.symbolicName.str);
  std::vector<ReturnClauseTerm> properties;
  if(itProps != props.end())
    properties = itProps->second;
  for(const auto & [varName, _] : props)
    if(varName != variable.symbolicName.str)
      throw std::logic_error("Variable '" + varName + "' not found.");
  
  for(const auto & [varName, constraint] : whereTermsByVar)
    if(varName != variable.symbolicName.str)
      throw std::logic_error("Variable '" + varName + "' not found.");

  const std::vector<std::string> labelsStr = asStringVec(labels);
  
  {
    auto f = std::function{[&](int argc, char **argv, char **column){
      auto _ = LogIndentScope();
      std::cout << LogIndent{};
      for(size_t i=0; i<argc; ++i)
        std::cout << variable.symbolicName.str << "." << column[i] << " = " << (argv[i] ? argv[i] : "<null>") << '|';
      std::cout << std::endl;
    }};
    auto it = whereTermsByVar.find(variable.symbolicName.str);
    db.forEachElementPropertyWithLabelsIn(elem,
                                          properties,
                                          labelsStr,
                                          (it == whereTermsByVar.end()) ? std::nullopt : std::optional{it->second},
                                          f);
  }
}

void runCypher(const std::string& cyperQuery, DB&db)
{
  const auto ast = cypherQueryToAST(db.idProperty(), cyperQuery);
  std::cout << std::endl;
  std::cout << "[openCypher] " << cyperQuery << std::endl;
  const auto _ = LogIndentScope{};
  runSingleQuery(ast, db);
}

int main()
{
  const bool printSQLRequests{true};

  DB db(printSQLRequests);

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

  try
  {
    // The where clause can be expressed in this form:
    //   (A) <term applying to the left node properties> AND
    //   (B) <term applying to the right node properties> AND
    //   (C) <term applying to the relationship properties> AND
    //   (D) <term applying to multiple items>
    //
    // in the SQL queries to retrieve properties we can apply the A, B, C parts of the clause and return as an extra field whether the row is valid.
    //
    // We need to evaluate (D) ourselves, at the end, when we return results.

    // Where clause with id or property lookup
    runCypher("MATCH (`n`)       WHERE n.test = 3   RETURN id(`n`), `n`.test, `n`.`what`;", db);
    runCypher("MATCH (`m`)<-[`r`]-(`n`) WHERE id(n) = 1 RETURN id(m), id(n), id(`r`), `m`.test;", db);
    runCypher("MATCH (`m`)<-[`r`]-(`n`) WHERE id(m) = 1 RETURN id(m), id(n), id(`r`), `n`.test;", db);
    
    // todo where clause with multiple terms.
    runCypher("MATCH (`n`)       WHERE n.test >= 2.5 AND n.test <= 3.5   RETURN id(`n`), `n`.test, `n`.`what`;", db);

    
    
    
    runCypher("MATCH (`n`)       RETURN id(`n`), `n`.test, `n`.`what`;", db);
    runCypher("MATCH (`n`:Node1) RETURN id(`n`), `n`.test, `n`.`what`;", db);
    runCypher("MATCH (`n`:Node2) RETURN id(`n`), `n`.test, `n`.`what`;", db);
    runCypher("MATCH ()-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`;", db);

    runCypher("MATCH (`n`:Node1)-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);
    runCypher("MATCH ()<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);

    runCypher("MATCH (:Node2)<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);
    runCypher("MATCH (:Node2)<-[]-(`n`:Node1) RETURN `n`.test;", db);

    // returns nothing because of the 'Test' label constraint in the anonymous part.
    runCypher("MATCH (:Test)<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);

    // node and dual node properties
    runCypher("MATCH (`m`:Node2)<-[`r`]-(`n`:Node1) RETURN id(`m`), id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);
    runCypher("MATCH (`m`:Node2)<-[`r`]-(`n`:Node1) RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test, `m`.test;", db);
    runCypher("MATCH (`m`:Node2)<-[]-(`n`:Node1) RETURN id(`m`), `n`.test;", db);


    
    // todo deduce labels from where clause (used in FFP):
    //runCypher("MATCH (`n`) WHERE n:Node1 OR n:Node2 RETURN id(`n`), `n`.test, `n`.`what`;", db);

    // todo (property value)
    //runCypher("MATCH (`n`:Node1{test=2})-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);

    // todo same as above, but do a union on "entity types that have a test property"
    //runCypher("MATCH (`n`:{test=2})-[`r`]->() RETURN id(`r`), `r`.testRel, `r`.`whatRel`, `n`.test;", db);

    return 0;
  }
  catch(const std::exception& e)
  {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
