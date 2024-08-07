/*
 Copyright 2024-present Olivier Sohn
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

namespace openCypher::detail
{

inline
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
    if(!nao.labels.empty())
      throw std::logic_error("Cannot have labels in a return clause (?)");
    // TODO support Literal in return clauses.
    const auto & var = std::get<Variable>(nao.atom.var);
    auto & elem = props[var].emplace_back();
    elem.returnClausePosition = i;
    elem.propertyName = *mayPropertyName;
    ++i;
  }
  return props;
}



//fOnOrderAndColumnNames is guaranteed to be called before fOnRow;
template<typename ID>
void runSingleQuery(const SingleQuery& q,
                    GraphDB<ID>& db,
                    const FOnOrderAndColumnNames& fOnOrderAndColumnNames,
                    const FOnRow& fOnRow)
{
  bool sentColumns{};
  auto f = std::function{[&](const ResultOrder& resultOrder,
                             const std::vector<Variable>& variables,
                             const VecColumnNames& columnNames,
                             const VecValues& values){
    if(!sentColumns)
    {
      // resultOrder and columnNames will always be the same so we send them once only.
      fOnOrderAndColumnNames(resultOrder, variables, columnNames);
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
  
  ExpressionsByVarsUsages whereExprsByVarsAndproperties;
  
  if(spq.mayReadingClause->match.where.has_value())
    // If the tree is not Equi-var, an exception is thrown.
    spq.mayReadingClause->match.where->exp->asMaximalANDAggregation(whereExprsByVarsAndproperties);
  
  const auto & app = mpp.anonymousPatternPart;
  
  const std::map<Variable, std::vector<ReturnClauseTerm>> props =
  extractProperties(spq.returnClause.items.naoExps);
  
  const auto limit = spq.returnClause.limit;

  auto mkReturnedProperties = [&](const Variable& var) -> std::vector<ReturnClauseTerm>
  {
    if(auto it = props.find(var); it != props.end())
      return it->second;
    return {};
  };
  
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
  
  size_t countActiveNodePaterns{};
  countActiveNodePaterns += nodePatternIsActive(app.firstNodePattern);
  for(const auto & pec : app.patternElementChains)
    countActiveNodePaterns += nodePatternIsActive(pec.nodePattern);

  if(((app.patternElementChains.size() == 1) && (countActiveNodePaterns > 0)) ||
      app.patternElementChains.size() > 1)
  {
    std::map<Variable, std::vector<ReturnClauseTerm>> variables;
    std::vector<PathPatternElement> pathPatternElements;

    if(app.firstNodePattern.mayVariable.has_value())
      variables[*app.firstNodePattern.mayVariable] = mkReturnedProperties(*app.firstNodePattern.mayVariable);
    pathPatternElements.emplace_back(app.firstNodePattern.mayVariable,
                                     app.firstNodePattern.labels);

    std::vector<TraversalDirection> traversalDirections;

    for(const auto & pec : app.patternElementChains)
    {
      traversalDirections.push_back(pec.relPattern.traversalDirection);

      if(pec.relPattern.mayVariable.has_value())
        variables[*pec.relPattern.mayVariable] = mkReturnedProperties(*pec.relPattern.mayVariable);
      pathPatternElements.emplace_back(pec.relPattern.mayVariable,
                                       pec.relPattern.labels);

      if(pec.nodePattern.mayVariable.has_value())
        variables[*pec.nodePattern.mayVariable] = mkReturnedProperties(*pec.nodePattern.mayVariable);
      pathPatternElements.emplace_back(pec.nodePattern.mayVariable,
                                       pec.nodePattern.labels);
    }
    
    {
      // Sanity check.
      
      for(const auto & [varName, _] : props)
        if(0 == variables.count(varName))
          throw std::logic_error("A variable used in the return clause was not defined.");
      
      for(const auto& [varAndProperties, _]: whereExprsByVarsAndproperties)
        for(const auto& [var, _]: varAndProperties)
          if(0 == variables.count(var))
            throw std::logic_error("A variable used in the where clause was not defined.");
    }

    db.forEachPath(traversalDirections,
                   variables,
                   pathPatternElements,
                   whereExprsByVarsAndproperties,
                   limit,
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

  if(spq.returnClause.items.naoExps.empty())
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
    
  db.forEachElementPropertyWithLabelsIn(variable,
                                        elem,
                                        properties,
                                        labels,
                                        &filter,
                                        limit,
                                        f);
}
} // NS
