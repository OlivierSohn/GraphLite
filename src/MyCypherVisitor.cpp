#include "MyCypherVisitor.h"
#include "CypherAST.h"

namespace openCypher
{

namespace detail
{
std::unique_ptr<Expression> tryStealAsExpressionPtr(std::any && res)
{
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return std::any_cast<NonArithmeticOperatorExpression>(res).StealAsPtr();
  else if(res.type() == typeid(ComparisonExpression))
    return std::any_cast<ComparisonExpression>(res).StealAsPtr();
  else if(res.type() == typeid(AggregateExpression))
    return std::any_cast<AggregateExpression>(res).StealAsPtr();
  else
    return {};
}
} // NS

std::string trim(const char c, std::string && str)
{
  if(str.back() == c)
    str.pop_back();
  if(str.front() == c)
    return str.substr(1);
  return std::move(str);
}

std::any MyCypherVisitor::visitErrorNode(antlr4::tree::ErrorNode * node)
{
  const auto str = "[Error] " + node->toString();
  m_errors.push_back(str);
  print(str.c_str());
  return defaultResult();
}

void MyCypherVisitor::print(const char* str) const
{
  if(!m_print)
    return;
  std::cout << LogIndent{} << str << std::endl;
}

std::any MyCypherVisitor::defaultVisit(const char * funcName, int line, antlr4::ParserRuleContext* context)
{
  auto s = scope(funcName);
  return visitChildren(context);
}


std::any MyCypherVisitor::visitOC_Cypher(CypherParser::OC_CypherContext *context) {
  auto _ = scope("Cypher");
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(SingleQuery))
      return res;
  }
  m_errors.push_back("OC_Cypher not supported.");
  return {};
}

std::any MyCypherVisitor::visitOC_Statement(CypherParser::OC_StatementContext *context) {
  auto _ = scope("Statement");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_Statement Expected size of children 1");
    return {};
  }
  return context->children[0]->accept(this);
}

std::any MyCypherVisitor::visitOC_Query(CypherParser::OC_QueryContext *context) {
  auto _ = scope("Query");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_Query Expected size of children 1");
    return {};
  }
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SingleQuery))
    return res;
  else
    m_errors.push_back("OC_RegularQuery only supports SingleQuery for now.");
  return {};
}

std::any MyCypherVisitor::visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *context) {
  auto _ = scope("RegularQuery");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_RegularQuery : union is not supported yet");
    return {};
  }
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SingleQuery))
    return res;
  else
    m_errors.push_back("OC_RegularQuery only supports SingleQuery.");
  return {};
}

std::any MyCypherVisitor::visitOC_Union(CypherParser::OC_UnionContext *context) {
  m_errors.push_back("OC_Union not supported");
  return defaultVisit("Union", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *context) {
  auto _ = scope("SingleQuery");
  SingleQuery sq;
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_SingleQuery Expected size of children 1");
    return {};
  }
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SinglePartQuery))
    sq.singlePartQuery = std::move(std::any_cast<SinglePartQuery>(res));
  else
    m_errors.push_back("OC_SingleQuery only supports SinglePartQuery for now.");
  return sq;
}

std::any MyCypherVisitor::visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *context) {
  auto _ = scope("SinglePartQuery");
  SinglePartQuery spq;
  size_t countReturn{};
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(ReadingClause))
      spq.mayReadingClause = std::move(std::any_cast<ReadingClause>(res));
    else if(res.type() == typeid(Return))
    {
      ++countReturn;
      spq.returnClause = std::move(std::any_cast<Return>(res));
    }
  }
  if(countReturn != 1)
    m_errors.push_back("OC_SinglePartQuery expects single return.");
  return spq;
}

std::any MyCypherVisitor::visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *context) {
  m_errors.push_back("OC_MultiPartQuery not supported");
  return defaultVisit("MultiPartQuery", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *context) {
  m_errors.push_back("OC_UpdatingClause not supported");
  return defaultVisit("UpdatingClause", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *context) {
  auto _ = scope("ReadingClause");
  ReadingClause r;
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_ReadingClause Expected size of children 1");
    return {};
  }
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(Match))
    r.match = std::move(std::any_cast<Match>(res));
  else
    m_errors.push_back("OC_ReadingClause only supports MATCH for now.");
  return r;
}

std::any MyCypherVisitor::visitOC_Match(CypherParser::OC_MatchContext *context) {
  auto _ = scope("Match");
  Match m;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Pattern))
      m.pattern = std::move(std::any_cast<Pattern>(res));
    else if(res.type() == typeid(WhereClause))
      m.where = std::move(std::any_cast<WhereClause>(res));
  }
  return m;
}

std::any MyCypherVisitor::visitOC_Unwind(CypherParser::OC_UnwindContext *context) {
  m_errors.push_back("OC_Unwind not supported");
  return defaultVisit("Unwind", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_Merge(CypherParser::OC_MergeContext *context) { return defaultVisit("Merge", __LINE__, context); }

std::any MyCypherVisitor::visitOC_MergeAction(CypherParser::OC_MergeActionContext *context) { return defaultVisit("MergeAction", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Create(CypherParser::OC_CreateContext *context) { return defaultVisit("Create", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Set(CypherParser::OC_SetContext *context) { return defaultVisit("Set", __LINE__, context); }

std::any MyCypherVisitor::visitOC_SetItem(CypherParser::OC_SetItemContext *context) { return defaultVisit("SetItem", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Delete(CypherParser::OC_DeleteContext *context) { return defaultVisit("Delete", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Remove(CypherParser::OC_RemoveContext *context) { return defaultVisit("Remove", __LINE__, context); }

std::any MyCypherVisitor::visitOC_RemoveItem(CypherParser::OC_RemoveItemContext *context) { return defaultVisit("RemoveItem", __LINE__, context); }

std::any MyCypherVisitor::visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *context) {
  m_errors.push_back("OC_InQueryCall not supported");
  return defaultVisit("InQueryCall", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *context) {
  m_errors.push_back("OC_StandaloneCall not supported");
  return defaultVisit("StandaloneCall", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_YieldItems(CypherParser::OC_YieldItemsContext *context) { return defaultVisit("YieldItems", __LINE__, context); }

std::any MyCypherVisitor::visitOC_YieldItem(CypherParser::OC_YieldItemContext *context) { return defaultVisit("YieldItem", __LINE__, context); }

std::any MyCypherVisitor::visitOC_With(CypherParser::OC_WithContext *context) {
  m_errors.push_back("OC_With not supported");
  return defaultVisit("With", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_Return(CypherParser::OC_ReturnContext *context) {
  auto _ = scope("Return");
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(ProjectionBody))
      return res;
  }
  m_errors.push_back("unsupported alternative in OC_Return");
  return {};
}

std::any MyCypherVisitor::visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *context) {
  auto _ = scope("ProjectionBody");
  if(auto * projItems = context->DISTINCT())
  {
    m_errors.push_back("todo: support DISTINCT");
    return {};
  }
  if(auto * projItems = context->oC_Limit())
  {
    m_errors.push_back("todo: support LIMIT");
    return {};
  }
  if(auto * projItems = context->oC_Skip())
  {
    m_errors.push_back("todo: support SKIP");
    return {};
  }
  if(auto * projItems = context->oC_Order())
  {
    m_errors.push_back("todo: support ORDER");
    return {};
  }
  if(auto * projItems = context->oC_ProjectionItems())
    return projItems->accept(this);
  return {};
}

std::any MyCypherVisitor::visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *context) {
  auto _ = scope("ProjectionItems");
  ProjectionItems p;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    // TODO support '*'
    if(res.type() == typeid(NonArithmeticOperatorExpression))
      p.naoExps.push_back(std::move(std::any_cast<NonArithmeticOperatorExpression>(res)));
  }
  return p;
}

std::any MyCypherVisitor::visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *context) {
  auto _ = scope("ProjectionItem");
  if(context->children.size() != 1)
  {
    // TODO support entire grammar
    m_errors.push_back("OC_ProjectionItem expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_ProjectionItem");
  return {};
}

std::any MyCypherVisitor::visitOC_Order(CypherParser::OC_OrderContext *context) { return defaultVisit("Order", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Skip(CypherParser::OC_SkipContext *context) { return defaultVisit("Skip", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Limit(CypherParser::OC_LimitContext *context) { return defaultVisit("Limit", __LINE__, context); }

std::any MyCypherVisitor::visitOC_SortItem(CypherParser::OC_SortItemContext *context) { return defaultVisit("SortItem", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Where(CypherParser::OC_WhereContext *context) {
  auto _ = scope("WhereContext");

  auto res = context->oC_Expression()->accept(this);
  if(auto ptr = detail::tryStealAsExpressionPtr(std::move(res)))
    return WhereClause{std::shared_ptr<Expression>{ptr.release()}};
  m_errors.push_back("OC_Where encounterd unsupported expression.");
  return {};
}

std::any MyCypherVisitor::visitOC_Pattern(CypherParser::OC_PatternContext *context) {
  auto _ = scope("Pattern");
  Pattern p;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(PatternPart))
      p.patternParts.push_back(std::move(std::any_cast<PatternPart>(res)));
  }
  return p;
}

std::any MyCypherVisitor::visitOC_PatternPart(CypherParser::OC_PatternPartContext *context) {
  auto _ = scope("PatternPart");
  PatternPart pp;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Variable))
      pp.mayVariable = std::move(std::any_cast<Variable>(res));
    else if(res.type() == typeid(AnonymousPatternPart))
      pp.anonymousPatternPart = std::move(std::any_cast<AnonymousPatternPart>(res));
  }
  return pp;
}

std::any MyCypherVisitor::visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *context) {
  /*
   oC_AnonymousPatternPart
   :  oC_PatternElement ;
   */
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_AnonymousPatternPart Expected size of children 1");
    return {};
  }
  return context->children[0]->accept(this);
}

std::any MyCypherVisitor::visitOC_PatternElement(CypherParser::OC_PatternElementContext *context) {
  auto _ = scope("PatternElement");
  PatternElement pe;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(NodePattern))
      pe.firstNodePattern = std::move(std::any_cast<NodePattern>(res));
    else if(res.type() == typeid(PatternElementChain))
      pe.patternElementChains.push_back(std::move(std::any_cast<PatternElementChain>(res)));
  }
  return pe;
}

std::any MyCypherVisitor::visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *context) { return defaultVisit("Relationships", __LINE__, context); }

std::any MyCypherVisitor::visitOC_NodePattern(CypherParser::OC_NodePatternContext *context) {
  auto _ = scope("NodePattern");
  NodePattern pattern;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Variable))
      pattern.mayVariable = std::move(std::any_cast<Variable>(res));
    else if(res.type() == typeid(Labels))
      pattern.labels = std::move(std::any_cast<Labels>(res));
  }
  return pattern;
}

std::any MyCypherVisitor::visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *context) {
  auto _ = scope("PatternElementChain");
  PatternElementChain pec;
  {
    auto res = context->oC_NodePattern()->accept(this);
    if(res.type() == typeid(NodePattern))
      pec.nodePattern = std::move(std::any_cast<NodePattern>(res));
    else
    {
      m_errors.push_back("OC_PatternElementChain Expected a NodePattern");
      return {};
    }
  }
  {
    auto res = context->oC_RelationshipPattern()->accept(this);
    if(res.type() == typeid(RelationshipPattern))
      pec.relPattern = std::move(std::any_cast<RelationshipPattern>(res));
  }
  return pec;
}

std::any MyCypherVisitor::visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *context) {
  auto leftArrow = context->oC_LeftArrowHead();
  const bool left = leftArrow && !leftArrow->isEmpty();
  auto rightArrow = context->oC_RightArrowHead();
  const bool right = rightArrow && !rightArrow->isEmpty();
  TraversalDirection dir{TraversalDirection::Any};
  if(left && !right)
    dir = TraversalDirection::Backward;
  else if(!left && right)
    dir = TraversalDirection::Forward;

  RelationshipPattern res;
  res.traversalDirection = dir;
  
  if(auto detail = context->oC_RelationshipDetail())
  {
    if(auto v = detail->oC_Variable())
    {
      auto var = v->accept(this);
      if(var.type() == typeid(Variable))
        res.mayVariable = std::move(std::any_cast<Variable>(var));
      else
      {
        m_errors.push_back("OC_RelationshipDetail Expected Variable");
        return {};
      }
    }
    if(auto r = detail->oC_RelationshipTypes())
    {
      auto relTypes = r->accept(this);
      if(relTypes.type() == typeid(Labels))
        res.labels = std::move(std::any_cast<Labels>(relTypes));
      else
      {
        m_errors.push_back("OC_RelationshipDetail Expected Labels");
        return {};
      }
    }
  }
  return res;
}

std::any MyCypherVisitor::visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *context) { return defaultVisit("RelationshipDetail", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Properties(CypherParser::OC_PropertiesContext *context) { return defaultVisit("Properties", __LINE__, context); }

std::any MyCypherVisitor::visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *context) {
  auto _ = scope("RelationshipTypes");
  Labels labels;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Label))
      labels.labels.push_back(std::move(std::any_cast<Label>(res)));
  }
  return labels;
}

std::any MyCypherVisitor::visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *context) {
  auto _ = scope("NodeLabels");
  Labels labels;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Label))
      labels.labels.push_back(std::move(std::any_cast<Label>(res)));
  }
  return labels;
}

std::any MyCypherVisitor::visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *context) {
  auto _ = scope("NodeLabel");
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Label))
      return res;
  }
  return {};
}

std::any MyCypherVisitor::visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *context) { return defaultVisit("RangeLiteral", __LINE__, context); }

std::any MyCypherVisitor::visitOC_LabelName(CypherParser::OC_LabelNameContext *context) {
  auto _ = scope("LabelName");
  /*
   oC_LabelName
   :  oC_SchemaName ;
   */
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_LabelName Expected size of children 1");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SchemaName))
    return Label{std::move(std::any_cast<SchemaName>(res).symbolicName)};
  else
    m_errors.push_back("OC_LabelName Expected SchemaName");
  return {};
}

std::any MyCypherVisitor::visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *context) {
  auto _ = scope("RelTypeName");
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(Label))
      return res;
  }
  return {};
}

std::any MyCypherVisitor::visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *context) { return defaultVisit("PropertyExpression", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Expression(CypherParser::OC_ExpressionContext *context) {
  auto _ = scope("Expression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_Expression expects a single child");
    return {};
  }
  return context->children[0]->accept(this);
}

std::any MyCypherVisitor::visitOC_OrExpression(CypherParser::OC_OrExpressionContext *context) {
  return aggregate(Aggregator::OR, context->oC_XorExpression());
}

std::any MyCypherVisitor::visitOC_XorExpression(CypherParser::OC_XorExpressionContext *context) {
  return aggregate(Aggregator::XOR, context->oC_AndExpression());
}

std::any MyCypherVisitor::visitOC_AndExpression(CypherParser::OC_AndExpressionContext *context) {
  return aggregate(Aggregator::AND, context->oC_NotExpression());
}

std::any MyCypherVisitor::visitOC_NotExpression(CypherParser::OC_NotExpressionContext *context) {
  auto _ = scope("NotExpression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_NotExpression expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else if(res.type() == typeid(ComparisonExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_NotExpression");
  return {};
}

std::any MyCypherVisitor::visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *context) {
  auto _ = scope("ComparisonExpression");
  if(context->children.size() == 1)
  {
    auto res = context->children[0]->accept(this);
    if(res.type() == typeid(NonArithmeticOperatorExpression))
      return res;
    else
    {
      m_errors.push_back("unsupported alternative in OC_ComparisonExpression");
      return {};
    }
  }
  else if(context->children.size() == 3)
  {
    auto resLeft = context->children[0]->accept(this);
    if(resLeft.type() != typeid(NonArithmeticOperatorExpression))
    {
      m_errors.push_back("OC_ComparisonExpression left should be NonArithmeticOperatorExpression");
      return {};
    }
    ComparisonExpression res;
    res.leftExp = std::move(std::any_cast<NonArithmeticOperatorExpression>(resLeft));
    
    auto resRight = context->children[2]->accept(this);
    if(resRight.type() != typeid(PartialComparisonExpression))
    {
      // Note: the grammar allows this to not exist, but what is the semantic in this case?
      m_errors.push_back("OC_ComparisonExpression right should be PartialComparisonExpression");
      return {};
    }
    res.partial = std::move(std::any_cast<PartialComparisonExpression>(resRight));
    return res;
  }
  else
    m_errors.push_back("OC_ComparisonExpression expects 1 or 3 children");
  return {};
}

std::optional<Comparison> toComparison(std::string const & str)
{
  if(str == "=")
    return Comparison::EQ;
  if(str == "<>")
    return Comparison::NE;
  if(str == "<")
    return Comparison::LT;
  if(str == "<=")
    return Comparison::LE;
  if(str == ">")
    return Comparison::GT;
  if(str == ">=")
    return Comparison::GE;
  return std::nullopt;
}

std::any MyCypherVisitor::visitOC_PartialComparisonExpression(CypherParser::OC_PartialComparisonExpressionContext *context) {
  PartialComparisonExpression res;
  const std::string cmp = context->children[0]->getText();
  auto cmpOp = toComparison(cmp);
  if(!cmpOp.has_value())
  {
    m_errors.push_back("OC_PartialComparisonExpression operator not supported.");
    return {};
  }
  res.comp = *cmpOp;
  auto resExp = context->oC_StringListNullPredicateExpression()->accept(this);
  
  if(resExp.type() != typeid(NonArithmeticOperatorExpression))
  {
    m_errors.push_back("OC_PartialComparisonExpression right should be NonArithmeticOperatorExpression");
    return {};
  }
  res.rightExp = std::move(std::any_cast<NonArithmeticOperatorExpression>(resExp));

  return res;
}

std::any MyCypherVisitor::visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *context) {
  auto _ = scope("StringListNullPredicateExpression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_StringListNullPredicateExpression expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_StringListNullPredicateExpression");
  return {};  
}

std::any MyCypherVisitor::visitOC_StringPredicateExpression(CypherParser::OC_StringPredicateExpressionContext *context) {
  m_errors.push_back("OC_PartialComparisonExpression not supported");
  return defaultVisit("StringPredicateExpression", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_ListPredicateExpression(CypherParser::OC_ListPredicateExpressionContext *context) {
  m_errors.push_back("OC_ListPredicateExpression not supported");
  return defaultVisit("ListPredicateExpression", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_NullPredicateExpression(CypherParser::OC_NullPredicateExpressionContext *context) {
  m_errors.push_back("OC_ListPredicateExpression not supported");
  return defaultVisit("NullPredicateExpression", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *context) {
  auto _ = scope("AddOrSubtractExpression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_AddOrSubtractExpression expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_AddOrSubtractExpression");
  return {};
}

std::any MyCypherVisitor::visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *context) {
  auto _ = scope("MultiplyDivideModuloExpression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_MultiplyDivideModuloExpression expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_MultiplyDivideModuloExpression");
  return {};  
}

std::any MyCypherVisitor::visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *context) {
  auto _ = scope("PowerOfExpression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_PowerOfExpression expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_PowerOfExpression");
  return {};
}

std::any MyCypherVisitor::visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *context) {
  auto _ = scope("UnaryAddOrSubtractExpression");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_UnaryAddOrSubtractExpression expects a single child");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(NonArithmeticOperatorExpression))
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_UnaryAddOrSubtractExpression");
  return {};
}

std::any MyCypherVisitor::visitOC_NonArithmeticOperatorExpression(CypherParser::OC_NonArithmeticOperatorExpressionContext *context) {
  auto _ = scope("NonArithmeticOperatorExpression");
  NonArithmeticOperatorExpression r;
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(NonArithmeticOperatorExpression))
    {
      // Case to support id(...) function, rewritten as a property access.
      if(context->children.size() == 1)
        return res;
      m_errors.push_back("OC_NonArithmeticOperatorExpression has a sub NonArithmeticOperatorExpression but many children.");
    }
    else if(res.type() == typeid(PropertyKeyName))
    {
      if(r.mayPropertyName.has_value())
        m_errors.push_back("OC_NonArithmeticOperatorExpression with multiple PropertyKeyName is not supported.");
      r.mayPropertyName = std::move(std::any_cast<PropertyKeyName>(res));
    }
    else if(res.type() == typeid(Atom))
      r.atom = std::move(std::any_cast<Atom>(res));
    else if(res.type() == typeid(Labels))
      m_errors.push_back("OC_NonArithmeticOperatorExpression with Labels is not supported.");
    else if(res.type() == typeid(ListOperatorExpression))
      m_errors.push_back("OC_NonArithmeticOperatorExpression with ListOperatorExpression is not supported.");
  }
  return r;
}

std::any MyCypherVisitor::visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *context) {
  m_errors.push_back("OC_ListOperatorExpression not supported");
  defaultVisit("ListOperatorExpression", __LINE__, context);
  return ListOperatorExpression{};
}

std::any MyCypherVisitor::visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *context) {
  auto _ = scope("PropertyLookup");
  for(const auto & child : context->children)
  {
    auto res = child->accept(this);
    if(res.type() == typeid(PropertyKeyName))
      return res;
  }
  m_errors.push_back("OC_PropertyLookup failed");
  return {};
}

std::any MyCypherVisitor::visitOC_Atom(CypherParser::OC_AtomContext *context) {
  auto _ = scope("Atom");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_Atom Expected size of children 1");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(Variable))
    return Atom{std::move(std::any_cast<Variable>(res))};
  else if(res.type() == typeid(Literal))
    return Atom{std::move(std::any_cast<Literal>(res))};
  else if(res.type() == typeid(AggregateExpression))
    return Atom{std::move(std::any_cast<AggregateExpression>(res))};
  else if(res.type() == typeid(NonArithmeticOperatorExpression))
    // To support the id(...) function, we rewrite the function call into a property access.
    return res;
  else
    m_errors.push_back("unsupported alternative in OC_Atom");
  return {};
}

std::any MyCypherVisitor::visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *context) {
  m_errors.push_back("OC_CaseExpression not supported");
  return defaultVisit("CaseExpression", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *context) { return defaultVisit("CaseAlternative", __LINE__, context); }

std::any MyCypherVisitor::visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *context) {
  m_errors.push_back("OC_ListComprehension not supported");
  return defaultVisit("ListComprehension", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *context) {
  m_errors.push_back("OC_PatternComprehension not supported");
  return defaultVisit("PatternComprehension", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_Quantifier(CypherParser::OC_QuantifierContext *context) {
  m_errors.push_back("OC_Quantifier not supported");
  return defaultVisit("Quantifier", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *context) { return defaultVisit("FilterExpression", __LINE__, context); }

std::any MyCypherVisitor::visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *context) {
  m_errors.push_back("OC_PatternPredicate not supported");
  return defaultVisit("PatternPredicate", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *context) {
  auto _ = scope("ParenthesizedExpression");
  if(auto e = context->oC_Expression())
    return e->accept(this);
  else
  {
    m_errors.push_back("OC_ParenthesizedExpression has null expression");
    return {};
  }
}

std::any MyCypherVisitor::visitOC_IdInColl(CypherParser::OC_IdInCollContext *context) { return defaultVisit("IdInColl", __LINE__, context); }

std::any MyCypherVisitor::visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *context) {
  if(context->DISTINCT())
  {
    m_errors.push_back("OC_FunctionInvocation with DISTINCT not supported");
    return {};
  }
  // verify it is size 1 and matches a node (or relationships) variable.
  const auto expressions = context->oC_Expression();
  if(expressions.size() != 1)
  {
    m_errors.push_back("OC_FunctionInvocation expects a single expression for now.");
    return {};
  }
  auto expr = expressions[0]->accept(this);
  if(expr.type() != typeid(NonArithmeticOperatorExpression))
  {
    m_errors.push_back("OC_FunctionInvocation expression must be NonArithmeticOperatorExpression for now.");
    return {};
  }
  const auto& naoExp = std::any_cast<NonArithmeticOperatorExpression>(expr);
  if(naoExp.mayPropertyName.has_value())
  {
    m_errors.push_back("OC_FunctionInvocation expression must not have a property for now.");
    return {};
  }
  
  auto func = context->oC_FunctionName()->accept(this);
  if(func.type() != typeid(IdentityFunction))
  {
    m_errors.push_back("OC_FunctionInvocation function must be Identity for now.");
    return {};
  }
  NonArithmeticOperatorExpression res;
  res.atom = std::move(naoExp.atom);
  res.mayPropertyName = m_IDProperty;
  return res;
}

std::any MyCypherVisitor::visitOC_FunctionName(CypherParser::OC_FunctionNameContext *context) {
  if(!context->oC_Namespace()->getText().empty())
  {
    m_errors.push_back("OC_FunctionInvocation with namespace not supported");
    return {};
  }
  auto name = context->oC_SymbolicName()->accept(this);
  if(name.type() != typeid(SymbolicName))
  {
    m_errors.push_back("OC_FunctionInvocation with invalid function name");
    return {};
  }
  const auto & sname = std::any_cast<SymbolicName>(name);
  std::string lowerName;
  for(const auto c : sname.str)
    lowerName.push_back(std::tolower(c));
  if(lowerName != "id")
  {
    m_errors.push_back("OC_FunctionInvocation with non-id function name is not supported yet");
    return {};
  }
  return IdentityFunction{};
}

std::any MyCypherVisitor::visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *context) {
  m_errors.push_back("OC_ExistentialSubquery not supported");
  return defaultVisit("ExistentialSubquery", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_ExplicitProcedureInvocation(CypherParser::OC_ExplicitProcedureInvocationContext *context) { return defaultVisit("ExplicitProcedureInvocation", __LINE__, context); }

std::any MyCypherVisitor::visitOC_ImplicitProcedureInvocation(CypherParser::OC_ImplicitProcedureInvocationContext *context) { return defaultVisit("ImplicitProcedureInvocation", __LINE__, context); }

std::any MyCypherVisitor::visitOC_ProcedureResultField(CypherParser::OC_ProcedureResultFieldContext *context) { return defaultVisit("ProcedureResultField", __LINE__, context); }

std::any MyCypherVisitor::visitOC_ProcedureName(CypherParser::OC_ProcedureNameContext *context) { return defaultVisit("ProcedureName", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Namespace(CypherParser::OC_NamespaceContext *context) { return defaultVisit("Namespace", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Variable(CypherParser::OC_VariableContext *context) {
  auto _ = scope("Variable");
  /*
   oC_Variable
   :  oC_SymbolicName ;
   */
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_Variable Expected size of children 1");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SymbolicName))
    return Variable{std::move(std::any_cast<SymbolicName>(res))};
  return {};
}

std::any MyCypherVisitor::visitOC_Literal(CypherParser::OC_LiteralContext *context) {
  auto *numLit = context->oC_NumberLiteral();
  if(numLit)
  {
    auto res = numLit->accept(this);
    if(res.type() != typeid(std::string))
    {
      m_errors.push_back("OC_Literal should be a std::string");
      return {};
    }
    return Literal{std::move(std::any_cast<std::string>(res))};
  }
  defaultVisit("Literal", __LINE__, context);
  m_errors.push_back("OC_Literal todo support all literals");
  return {};
}

std::any MyCypherVisitor::visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *context) { return defaultVisit("BooleanLiteral", __LINE__, context); }

std::any MyCypherVisitor::visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *context) {
  return context->getText();
}

std::any MyCypherVisitor::visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *context) { return defaultVisit("IntegerLiteral", __LINE__, context); }

std::any MyCypherVisitor::visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *context) { return defaultVisit("DoubleLiteral", __LINE__, context); }

std::any MyCypherVisitor::visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *context) { return defaultVisit("ListLiteral", __LINE__, context); }

std::any MyCypherVisitor::visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *context) { return defaultVisit("MapLiteral", __LINE__, context); }

std::any MyCypherVisitor::visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *context) {
  auto _ = scope("PropertyKeyName");
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_PropertyKeyName Expected size of children 1");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SchemaName))
    return PropertyKeyName{std::move(std::any_cast<SchemaName>(res).symbolicName)};
  else
    m_errors.push_back("OC_PropertyKeyName Expected SchemaName");
  return {};
}

std::any MyCypherVisitor::visitOC_Parameter(CypherParser::OC_ParameterContext *context) {
  m_errors.push_back("OC_Parameter not supported");
  return defaultVisit("Parameter", __LINE__, context);
}

std::any MyCypherVisitor::visitOC_SchemaName(CypherParser::OC_SchemaNameContext *context) {
  auto _ = scope("SchemaName");
  /*
   oC_SchemaName
   :  oC_SymbolicName
   | oC_ReservedWord
   ;
   */
  if(context->children.size() != 1)
  {
    m_errors.push_back("OC_SchemaName Expected size of children 1");
    return {};
  }
  
  auto res = context->children[0]->accept(this);
  if(res.type() == typeid(SymbolicName))
    return SchemaName{std::move(std::any_cast<SymbolicName>(res))};
  else
    m_errors.push_back("Todo OC_SchemaName should support oC_ReservedWord");
  return {};
}

std::any MyCypherVisitor::visitOC_ReservedWord(CypherParser::OC_ReservedWordContext *context) { return defaultVisit("ReservedWord", __LINE__, context); }

std::any MyCypherVisitor::visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *context) {
  auto _ = scope("SymbolicName");
  if(auto unescapedSymbolicName = context->UnescapedSymbolicName())
  {
    return SymbolicName{unescapedSymbolicName->getText()};
  }
  else if(auto escapedSymbolicName = context->EscapedSymbolicName())
  {
    std::string str = escapedSymbolicName->getText();
    str = trim('`', std::move(str));
    return SymbolicName{std::move(str)};
  }
  else if(auto hexLetter = context->HexLetter())
  {
    return SymbolicName{hexLetter->getText()};
  }
  else
    m_errors.push_back("unhandled type of OC_SymbolicName");
  return {};
}

std::any MyCypherVisitor::visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *context) { return defaultVisit("LeftArrowHead", __LINE__, context); }

std::any MyCypherVisitor::visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *context) { return defaultVisit("RightArrowHead", __LINE__, context); }

std::any MyCypherVisitor::visitOC_Dash(CypherParser::OC_DashContext *context) { return defaultVisit("Dash", __LINE__, context); }

} // NS
