

#pragma once

#include "Logs.h"
#include "cypherparser/CypherVisitor.h"
#include "CypherAST.h"

namespace openCypher
{
class MyCypherVisitor : public CypherVisitor
{
public:
  MyCypherVisitor(std::string IDProperty, bool print = false)
  : m_print(print)
  , m_IDProperty(PropertyKeyName{SymbolicName{IDProperty}})
  {}
  
  const std::vector<std::string>& getErrors() const { return m_errors; }
  
private:
  void print(const char* str) const;
  
  [[nodiscard]]
  LogIndentScope scope(std::string const& scopeName)
  {
    if(m_print)
      return logScope(std::cout, scopeName);
    else
      return LogIndentScope{};
  }
  
  std::any visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override;
  
  std::any visitOC_Cypher(CypherParser::OC_CypherContext *context) override;
  
  std::any visitOC_Statement(CypherParser::OC_StatementContext *context) override;
  
  std::any visitOC_Query(CypherParser::OC_QueryContext *context) override;
  
  std::any visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *context) override;
  
  std::any visitOC_Union(CypherParser::OC_UnionContext *context) override;
  
  std::any visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *context) override;
  
  std::any visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *context) override;
  
  std::any visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *context) override;
  
  std::any visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *context) override;
  
  std::any visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *context) override;
  
  std::any visitOC_Match(CypherParser::OC_MatchContext *context) override;
  
  std::any visitOC_Unwind(CypherParser::OC_UnwindContext *context) override;
  
  std::any visitOC_Merge(CypherParser::OC_MergeContext *context) override;
  
  std::any visitOC_MergeAction(CypherParser::OC_MergeActionContext *context) override;
  
  std::any visitOC_Create(CypherParser::OC_CreateContext *context) override;
  
  std::any visitOC_Set(CypherParser::OC_SetContext *context) override;
  
  std::any visitOC_SetItem(CypherParser::OC_SetItemContext *context) override;
  
  std::any visitOC_Delete(CypherParser::OC_DeleteContext *context) override;
  
  std::any visitOC_Remove(CypherParser::OC_RemoveContext *context) override;
  
  std::any visitOC_RemoveItem(CypherParser::OC_RemoveItemContext *context) override;
  
  std::any visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *context) override;
  
  std::any visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *context) override;
  
  std::any visitOC_YieldItems(CypherParser::OC_YieldItemsContext *context) override;
  
  std::any visitOC_YieldItem(CypherParser::OC_YieldItemContext *context) override;
  
  std::any visitOC_With(CypherParser::OC_WithContext *context) override;
  
  std::any visitOC_Return(CypherParser::OC_ReturnContext *context) override;
  
  std::any visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *context) override;
  
  std::any visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *context) override;
  
  std::any visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *context) override;
  
  std::any visitOC_Order(CypherParser::OC_OrderContext *context) override;
  
  std::any visitOC_Skip(CypherParser::OC_SkipContext *context) override;
  
  std::any visitOC_Limit(CypherParser::OC_LimitContext *context) override;
  
  std::any visitOC_SortItem(CypherParser::OC_SortItemContext *context) override;
  
  std::any visitOC_Where(CypherParser::OC_WhereContext *context) override;
  
  std::any visitOC_Pattern(CypherParser::OC_PatternContext *context) override;
  
  std::any visitOC_PatternPart(CypherParser::OC_PatternPartContext *context) override;
  
  std::any visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *context) override;
  
  std::any visitOC_PatternElement(CypherParser::OC_PatternElementContext *context) override;
  
  std::any visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *context) override;
  
  std::any visitOC_NodePattern(CypherParser::OC_NodePatternContext *context) override;
  
  std::any visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *context) override;
  
  std::any visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *context) override;
  
  std::any visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *context) override;
  
  std::any visitOC_Properties(CypherParser::OC_PropertiesContext *context) override;
  
  std::any visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *context) override;
  
  std::any visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *context) override;
  
  std::any visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *context) override;
  
  std::any visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *context) override;
  
  std::any visitOC_LabelName(CypherParser::OC_LabelNameContext *context) override;
  
  std::any visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *context) override;
  
  std::any visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *context) override;
  
  std::any visitOC_Expression(CypherParser::OC_ExpressionContext *context) override;
  
  std::any visitOC_OrExpression(CypherParser::OC_OrExpressionContext *context) override;
  
  std::any visitOC_XorExpression(CypherParser::OC_XorExpressionContext *context) override;
  
  std::any visitOC_AndExpression(CypherParser::OC_AndExpressionContext *context) override;
  
  std::any visitOC_NotExpression(CypherParser::OC_NotExpressionContext *context) override;
  
  std::any visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *context) override;
  
  std::any visitOC_PartialComparisonExpression(CypherParser::OC_PartialComparisonExpressionContext *context) override;
  
  std::any visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *context) override;
  
  std::any visitOC_StringPredicateExpression(CypherParser::OC_StringPredicateExpressionContext *context) override;
  
  std::any visitOC_ListPredicateExpression(CypherParser::OC_ListPredicateExpressionContext *context) override;
  
  std::any visitOC_NullPredicateExpression(CypherParser::OC_NullPredicateExpressionContext *context) override;
  
  std::any visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *context) override;
  
  std::any visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *context) override;
  
  std::any visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *context) override;
  
  std::any visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *context) override;
  
  std::any visitOC_NonArithmeticOperatorExpression(CypherParser::OC_NonArithmeticOperatorExpressionContext *context) override;
  
  std::any visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *context) override;
  
  std::any visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *context) override;
  
  std::any visitOC_Atom(CypherParser::OC_AtomContext *context) override;
  
  std::any visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *context) override;
  
  std::any visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *context) override;
  
  std::any visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *context) override;
  
  std::any visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *context) override;
  
  std::any visitOC_Quantifier(CypherParser::OC_QuantifierContext *context) override;
  
  std::any visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *context) override;
  
  std::any visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *context) override;
  
  std::any visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *context) override;
  
  std::any visitOC_IdInColl(CypherParser::OC_IdInCollContext *context) override;
  
  std::any visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *context) override;
  
  std::any visitOC_FunctionName(CypherParser::OC_FunctionNameContext *context) override;
  
  std::any visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *context) override;
  
  std::any visitOC_ExplicitProcedureInvocation(CypherParser::OC_ExplicitProcedureInvocationContext *context) override;
  
  std::any visitOC_ImplicitProcedureInvocation(CypherParser::OC_ImplicitProcedureInvocationContext *context) override;
  
  std::any visitOC_ProcedureResultField(CypherParser::OC_ProcedureResultFieldContext *context) override;
  
  std::any visitOC_ProcedureName(CypherParser::OC_ProcedureNameContext *context) override;
  
  std::any visitOC_Namespace(CypherParser::OC_NamespaceContext *context) override;
  
  std::any visitOC_Variable(CypherParser::OC_VariableContext *context) override;
  
  std::any visitOC_Literal(CypherParser::OC_LiteralContext *context) override;
  
  std::any visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *context) override;
  
  std::any visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *context) override;
  
  std::any visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *context) override;
  
  std::any visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *context) override;
  
  std::any visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *context) override;
  
  std::any visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *context) override;
  
  std::any visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *context) override;
  
  std::any visitOC_Parameter(CypherParser::OC_ParameterContext *context) override;
  
  std::any visitOC_SchemaName(CypherParser::OC_SchemaNameContext *context) override;
  
  std::any visitOC_ReservedWord(CypherParser::OC_ReservedWordContext *context) override;
  
  std::any visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *context) override;
  
  std::any visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *context) override;
  
  std::any visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *context) override;
  
  std::any visitOC_Dash(CypherParser::OC_DashContext *context) override;
  
private:
  std::any defaultVisit(const char* funcName, int line, antlr4::ParserRuleContext* context);
  
  template<typename U>
  std::any aggregate(const Aggregator a, const std::vector<U>& subExpressions);
  
  PropertyKeyName m_IDProperty;
  bool m_print;
  std::vector<std::string> m_errors;
};

namespace detail
{
std::unique_ptr<Expression> tryStealAsExpressionPtr(std::any && res);
}

// subExpressions is expected to have one or more elements.
template<typename U>
std::any MyCypherVisitor::aggregate(const Aggregator a, const std::vector<U>& subExpressions)
{
  const auto scopeName = "Aggregator_" + toStr(a);
  auto _ = scope(scopeName);
  if(subExpressions.empty())
  {
    m_errors.push_back(scopeName + ": has no sub expression");
    return {};
  }
  else if(subExpressions.size() == 1)
    return subExpressions[0]->accept(this);
  AggregateExpression aggregateExpr{a};
  for(const auto & expr : subExpressions)
  {
    auto res = expr->accept(this);
    if(auto expressionPtr = detail::tryStealAsExpressionPtr(std::move(res)))
      aggregateExpr.add(std::move(expressionPtr));
    else
    {
      m_errors.push_back(scopeName + ": encountered non-expression");
      return {};
    }
  }
  return aggregateExpr;
}
} // NS
