

#pragma once

#include <string>
#include <vector>
#include <optional>

struct SymbolicName
{
  std::string str;
};

struct Variable
{
  SymbolicName symbolicName;
};

struct SchemaName
{
  SymbolicName symbolicName;
};
struct Label
{
  SymbolicName symbolicName;
};
struct Labels
{
  std::vector<Label> labels;
};
struct NodePattern
{
  bool isTrivial() const { return !mayVariable.has_value() && labels.labels.empty(); }
  std::optional<Variable> mayVariable;
  Labels labels;
  // TODO properties
};
enum class TraversalDirection
{
  Any,
  Forward,
  Backward
};
inline TraversalDirection mirror(TraversalDirection d)
{
  switch(d)
  {
    case TraversalDirection::Forward:
      return TraversalDirection::Backward;
    case TraversalDirection::Backward:
      return TraversalDirection::Forward;
    default:
      return d;
  }
}
struct RelationshipPattern
{
  TraversalDirection traversalDirection;
  std::optional<Variable> mayVariable;
  Labels labels;
  // TODO properties
  // TODO range
};

struct PatternElementChain
{
  RelationshipPattern relPattern;
  NodePattern nodePattern;
};
struct PatternElement
{
  NodePattern firstNodePattern;
  std::vector<PatternElementChain> patternElementChains;
};
using AnonymousPatternPart = PatternElement;
struct PatternPart
{
  std::optional<Variable> mayVariable;
  AnonymousPatternPart anonymousPatternPart;
};
struct Pattern{
  std::vector<PatternPart> patternParts;
};
struct Literal{
  std::string str;
};
struct Atom{
  std::variant<Variable, Literal> var;
};
struct PropertyKeyName{
  SymbolicName symbolicName;
};

struct Expression
{
  virtual ~Expression() = default;
};

struct NonArithmeticOperatorExpression : public Expression {
  Atom atom;
  std::optional<PropertyKeyName> mayPropertyName;
};

enum class Comparison
{
  EQ, // =
  NE, // <>
  GT, // >
  LT, // <
  GE, // >=
  LE  // <=
};
struct PartialComparisonExpression{
  Comparison comp;
  NonArithmeticOperatorExpression rightExp;
};
struct ComparisonExpression : public Expression {
  NonArithmeticOperatorExpression leftExp;
  PartialComparisonExpression partial;
};
struct WhereClause{
  // For now we only support this.
  ComparisonExpression exp;
};

struct Match{
  Pattern pattern;
  std::optional<WhereClause> where;
};
struct ReadingClause{
  // todo Either ...
  Match match;
};

// Not used yet in valid cases.
struct ListOperatorExpression{};

struct ProjectionItems
{
  // simplified...
  std::vector<NonArithmeticOperatorExpression> naoExps;
};
// simplified...
using ProjectionBody = ProjectionItems;
using Return = ProjectionBody;
struct SinglePartQuery{
  std::optional<ReadingClause> mayReadingClause;
  Return returnClause;
};
struct SingleQuery{
  SinglePartQuery singlePartQuery;
};
/*
struct FunctionName{
  std::string namespaceStr;
  std::string funcName;
};*/
struct IdentityFunction{};
