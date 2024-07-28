#include "CypherQuery.h"
#include "antlr4-runtime.h"
#include "cypherparser/CypherParser.h"
#include "cypherparser/CypherLexer.h"
#include "cypherparser/CypherListener.h"
#include "MyCypherVisitor.h"

namespace openCypher::detail
{
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
  
  std::unordered_map<
  std::string /*node or dualNode or rel*/,
  std::vector<const Expression*> /* expressions of the vector are ANDed*/>
  whereTermsByVar;
  
  if(spq.mayReadingClause->match.where.has_value())
    // If the tree is not Equi-var, an exception is thrown.
    // todo: support non-Equi-var trees (i.e: WHERE n.weight > 3 OR r.status = 2)
    spq.mayReadingClause->match.where->exp->asAndEquiVarSubTrees(whereTermsByVar);
  
  const auto & app = mpp.anonymousPatternPart;
  
  const std::unordered_map<std::string, std::vector<ReturnClauseTerm>> props =
  extractProperties(spq.returnClause.naoExps);
  
  // TODO make the code of forEachNodeAndRelatedRelationship more generic/symmetrical
  //   s.t we don't need to detect which pattern is active here.
  auto nodePatternIsActive = [&](const NodePattern& np)
  {
    if(np.mayVariable.has_value() && (props.count(np.mayVariable->symbolicName.str) || whereTermsByVar.count(np.mayVariable->symbolicName.str)))
      // the node pattern has a variable AND this variable is used in the return clause or in the where clause.
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
          throw std::logic_error("Variable '" + varName + "' used in the return clause was not defined.");
      
      for(const auto &[varName, _]: whereTermsByVar)
        if(0 == allVariables.count(varName))
          throw std::logic_error("Variable '" + varName + "' used in the where clause was not defined.");
    }
    
    const std::vector<const Expression*> * nodeFilter{};
    const std::vector<const Expression*> * relFilter{};
    const std::vector<const Expression*> * dualNodeFilter{};
    if(nodeVariable.has_value())
      if(const auto it = whereTermsByVar.find(nodeVariable->symbolicName.str); it != whereTermsByVar.end())
        nodeFilter = &it->second;
    if(relVariable.has_value())
      if(const auto it = whereTermsByVar.find(relVariable->symbolicName.str); it != whereTermsByVar.end())
        relFilter = &it->second;
    if(dualNodeVariable.has_value())
      if(const auto it = whereTermsByVar.find(dualNodeVariable->symbolicName.str); it != whereTermsByVar.end())
        dualNodeFilter = &it->second;
    
    variablesNames.push_back(nodeVariable->symbolicName.str);
    variablesNames.push_back(relVariable->symbolicName.str);
    variablesNames.push_back(dualNodeVariable->symbolicName.str);
    
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
    auto it = whereTermsByVar.find(variable.symbolicName.str);
    const std::vector<const Expression*>* filter{};
    if(it != whereTermsByVar.end())
      filter = &it->second;
    db.forEachElementPropertyWithLabelsIn(elem,
                                          properties,
                                          labelsStr,
                                          filter,
                                          f);
  }
}
} // NS
