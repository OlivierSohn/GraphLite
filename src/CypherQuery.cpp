#include "CypherQuery.h"
#include "antlr4-runtime.h"
#include "cypherparser/CypherParser.h"
#include "cypherparser/CypherLexer.h"
#include "cypherparser/CypherListener.h"
#include "MyCypherVisitor.h"

namespace openCypher::detail
{
SingleQuery cypherQueryToAST(const std::string& idProperty, const std::string& query, const bool printAST)
{
  auto chars = antlr4::ANTLRInputStream(query);
  auto lexer = CypherLexer(&chars);
  auto tokens = antlr4::CommonTokenStream(&lexer);
  auto parser = CypherParser(&tokens);
  //parser.setBuildParseTree(true);
  CypherParser::OC_CypherContext* cypherTree = parser.oC_Cypher();
  
  auto visitor = MyCypherVisitor(idProperty, printAST);
  auto resVisit = visitor.visit(cypherTree);
  
  if(!visitor.getErrors().empty())
  {
    std::ostringstream s;
    for(const auto& err : visitor.getErrors())
      s << "  " << err << std::endl;
    throw std::logic_error("Visitor errors:\n" + s.str());
  }
  
  if(resVisit.type() != typeid(SingleQuery))
    throw std::logic_error("No SingleQuery was returned.");
  return std::any_cast<SingleQuery>(resVisit);;
}


std::map<Variable, std::vector<ReturnClauseTerm>>
extractProperties(const std::vector<NonArithmeticOperatorExpression>& naoExps)
{
  std::map<Variable, std::vector<ReturnClauseTerm>> props;
  size_t i{};
  for(const auto & nao : naoExps)
  {
    const auto& mayPropertyName = nao.mayPropertyName;
    if(!mayPropertyName.has_value())
      throw std::logic_error("Not Implemented (todo return 'entire node'?)");
    // TODO support Literal in return clauses.
    const auto & var = std::get<Variable>(nao.atom.var);
    auto & elem = props[var].emplace_back();
    elem.returnClausePosition = i;
    elem.propertyName = *mayPropertyName;
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


//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
void runSingleQuery(const SingleQuery& q, GraphDB& db, const FOnOrderAndColumnNames& fOnOrderAndColumnNames, const FOnRow& fOnRow)
{
  bool sentColumns{};
  std::vector<std::string> variablesNames;
  auto f = std::function{[&](const GraphDB::ResultOrder& resultOrder,
                             const GraphDB::VecColumnNames& columnNames,
                             const GraphDB::VecValues& values){
    if(!sentColumns)
    {
      // resultOrder and columnNames will always be the same so we send them once only.
      fOnOrderAndColumnNames(resultOrder, variablesNames, columnNames);
      sentColumns = true;
    }
    fOnRow(values);
  }};
  
  const auto & spq = q.singlePartQuery;
  if(!spq.mayReadingClause.has_value())
    throw std::logic_error("Not Implemented (Expected a reading clause)");
  const auto & matchPatternParts = spq.mayReadingClause->match.pattern.patternParts;
  if(matchPatternParts.size() != 1)
    throw std::logic_error("Not Implemented (Expected a single pattern part)");
  const auto & mpp = matchPatternParts[0];
  if(mpp.mayVariable.has_value())
    throw std::logic_error("Not Implemented (Expected no variable before match pattern)");
  
  ExpressionsByVarAndProperties whereExprsByVarsAndproperties;
  
  if(spq.mayReadingClause->match.where.has_value())
    // If the tree is not Equi-var, an exception is thrown.
    // todo: support non-Equi-var trees (i.e: WHERE n.weight > 3 OR r.status = 2)
    spq.mayReadingClause->match.where->exp->asMaximalANDAggregation(whereExprsByVarsAndproperties);
  
  const auto & app = mpp.anonymousPatternPart;
  
  const std::map<Variable, std::vector<ReturnClauseTerm>> props =
  extractProperties(spq.returnClause.naoExps);
  
  // TODO make the code of forEachNodeAndRelatedRelationship more generic/symmetrical
  //   s.t we don't need to detect which pattern is active here.
  auto nodePatternIsActive = [&](const NodePattern& np)
  {
    if(np.mayVariable.has_value())
    {
      // return true if the node pattern has a variable AND this variable
      // is used in the return clause or in the where clause.
      if(props.count(*np.mayVariable))
        return true;
      for(const auto & [varAndProperties, _] : whereExprsByVarsAndproperties)
        for(const auto & [var, _] : varAndProperties)
          if(var == *np.mayVariable)
            return true;
    }
    // return true if there are associated labels constraints.
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
      if(auto it = props.find(*nodeVariable); it != props.end())
        propertiesNode = it->second;
    if(relVariable.has_value())
      if(auto it = props.find(*relVariable); it != props.end())
        propertiesRel = it->second;
    if(dualNodeVariable.has_value())
      if(auto it = props.find(*dualNodeVariable); it != props.end())
        propertiesDualNode = it->second;
    
    {
      // Sanity check.
      
      std::set<Variable> allVariables;
      if(nodeVariable.has_value())
        allVariables.insert(*nodeVariable);
      if(relVariable.has_value())
        allVariables.insert(*relVariable);
      if(dualNodeVariable.has_value())
        allVariables.insert(*dualNodeVariable);
      
      for(const auto & [varName, _] : props)
        if(0 == allVariables.count(varName))
          throw std::logic_error("A variable used in the return clause was not defined.");
      
      for(const auto& [varAndProperties, _]: whereExprsByVarsAndproperties)
        for(const auto& [var, _]: varAndProperties)
          if(0 == allVariables.count(var))
            throw std::logic_error("A variable used in the where clause was not defined.");
    }
    
    variablesNames.push_back(nodeVariable.has_value() ? nodeVariable->symbolicName.str : "");
    variablesNames.push_back(relVariable.has_value() ? relVariable->symbolicName.str : "");
    variablesNames.push_back(dualNodeVariable.has_value() ? dualNodeVariable->symbolicName.str : "");
    
    const TraversalDirection traversalDirection = app.patternElementChains[0].relPattern.traversalDirection;
    db.forEachNodeAndRelatedRelationship(leftNodeVarIsActive ? traversalDirection : mirror(traversalDirection),
                                         nodeVariable.has_value() ? &*nodeVariable : nullptr,
                                         relVariable.has_value() ? &*relVariable : nullptr,
                                         dualNodeVariable.has_value() ? &*dualNodeVariable : nullptr,
                                         propertiesNode,
                                         propertiesRel,
                                         propertiesDualNode,
                                         asStringVec(nodeLabels),
                                         asStringVec(relLabels),
                                         asStringVec(dualNodeLabels),
                                         whereExprsByVarsAndproperties,
                                         f);
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
  
  variablesNames.push_back(variable.symbolicName.str);
  
  if(spq.returnClause.naoExps.empty())
    throw std::logic_error("Not Implemented (Expected some non arithmetic expression)");
  
  const auto itProps = props.find(variable);
  std::vector<ReturnClauseTerm> properties;
  if(itProps != props.end())
    properties = itProps->second;
  for(const auto & [var, _] : props)
    if(var != variable)
      throw std::logic_error("A variable used in the return clause was not defined.");
  
  std::vector<const Expression*> filter;
  for(const auto& [varAndProperties, exprs]: whereExprsByVarsAndproperties)
  {
    for(const auto& [var, _]: varAndProperties)
      if(var != variable)
        throw std::logic_error("A variable used in the where clause was not defined.");
    filter.insert(filter.end(), exprs.begin(), exprs.end());
  }
  
  const std::vector<std::string> labelsStr = asStringVec(labels);
  
  db.forEachElementPropertyWithLabelsIn(elem,
                                        properties,
                                        labelsStr,
                                        &filter,
                                        f);
}
} // NS
