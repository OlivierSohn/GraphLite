

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <unordered_set>
#include <any>

#include "SqlAST.h"

template < typename > constexpr bool c_false = false;

namespace openCypher
{
using Comparison = sql::Comparison;

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
struct Pattern
{
  std::vector<PatternPart> patternParts;
};
struct Literal
{
  std::string str;
};
struct PropertyKeyName
{
  SymbolicName symbolicName;
};

// Definitions for terms used in comments:
//
// # Equi-var
// All nodes of an "Equi-var" expression tree use properties of the _same_ variable.
//
// # Equi-property
// All nodes of an "Equi-property" expression tree use the _same_ property of the _same_ variable.

// Note on supported where clauses:
//
// The where clause for the (a)-[r]-(b) pattern match can be expressed in this form:
//   (A) <term applying only to properties and label of 'a'> AND
//   (B) <term applying only to properties and label of 'b'> AND
//   (C) <term applying only to properties and label of 'r'> AND
//   (D) <term applying to properties and label of multiple items, i.e for example 'a' and 'b'>
//
// Currently, we only handle where clauses containing (A), (B), (C), but not (D).
// This is why we needed to define the notion of "equi-var" expression tree:
//   supported where clauses are "equi-var" expressions.
//
// TODO: support (D) by evaluating (D) when we merge results of individual queries.
struct Expression
{
  virtual ~Expression() = default;
  
  virtual std::unique_ptr<Expression> StealAsPtr() = 0;
    
  // If "the expression tree is equi-var, or the expression tree is an AND-aggregation of equi-var subtrees",
  //   equi-var subtrees are returned in |res|.
  // Otherwise, an exception is thrown.
  virtual void asAndEquiVarSubTrees(std::unordered_map<std::string /*variable*/, std::vector<const Expression*>>& res) const = 0;
  
  // If the expression tree is equi-var,
  //   returns the corresponding variable
  // Else throws an exception
  virtual std::string asEquiVarTree() const = 0;

  // Precondition: the caller has verified that the Expression is equi-var,
  //   i.e calling asEquiVarTree() doesn't throw.
  //
  // If the expression tree is equi-property,
  //   returns the corresponding variable
  // Else throws an exception
  virtual std::string asEquiPropertyTree() const = 0;

  // throws if the translation is not supported yet.
  virtual std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::unordered_set<std::string>& sqlFields) const = 0;
};

// This helper class wraps the std::vector<std::unique_ptr<Expression>> in a shared_ptr
// to make it copyable (std::any can only wrap classes that are copyable).
struct SubExpressions
{
  SubExpressions()
  : m_exprs(std::make_shared<std::vector<std::unique_ptr<Expression>>>())
  {}
  
  void push_back(std::unique_ptr<Expression> && ptr)
  {
    m_exprs->push_back(std::move(ptr));
  }
  
  const std::vector<std::unique_ptr<Expression>>& get() const { return *m_exprs; }
  
private:
  std::shared_ptr<std::vector<std::unique_ptr<Expression>>> m_exprs;
};


enum class Aggregator
{
  AND,
  OR,
  XOR
};

inline std::string toStr(Aggregator a)
{
  switch(a)
  {
    case Aggregator::AND: return "AND";
    case Aggregator::OR: return "OR";
    case Aggregator::XOR: return "XOR";
  }
}

inline sql::Aggregator toSqlAggregator(Aggregator a)
{
  switch(a)
  {
    case Aggregator::AND: return sql::Aggregator::AND;
    case Aggregator::OR: return sql::Aggregator::OR;
    case Aggregator::XOR:
      throw std::logic_error("XOR not supported in SQLLite");
    default:
      throw std::logic_error("invalid enu; value");
  }
}

// Guaranteed to contain 2 or more sub-expressions
struct AggregateExpression : public Expression
{
  AggregateExpression(Aggregator a)
  : m_aggregator(a)
  {}

  void add(Expression && e)
  {
    add(e.StealAsPtr());
  }
  void add(std::unique_ptr<Expression> && ptr)
  {
    m_subExprs.push_back(std::move(ptr));
  }
  const std::vector<std::unique_ptr<Expression>>& subExpressions() const { return m_subExprs.get(); }

  void asAndEquiVarSubTrees(std::unordered_map<std::string /*variable*/, std::vector<const Expression*>>& res) const override
  {
    if(m_aggregator == Aggregator::XOR)
    {
      // SQL doesn't have XOR but we could transform the tree using this equivalency:
      //   a XOR b === (a OR b) AND NOT(a AND b)
      // Once the tree is transformed, if all subexpressions are equivar with the same var,
      // the tree will be equivar.
      throw std::logic_error("Xor is not AndEquiVarSubTrees");
    }

    for(const auto & exp: subExpressions())
      res[exp->asEquiVarTree()].push_back(exp.get());
    
    if(m_aggregator == Aggregator::OR)
    {
      if(res.size() == 1)
        // all sub expressions have the same variable, so we return ourselves instead.
        res.begin()->second = std::vector<const Expression*>{this};
      else
        throw std::logic_error("Or with different vars is not AndEquiVarSubTrees");
    }
  }

  std::string asEquiVarTree() const override
  {
    std::optional<std::string> var;
    for(const auto & expr : subExpressions())
    {
      const std::string var2 = expr->asEquiVarTree();
      if(var.has_value())
      {
        if(*var != var2)
          throw std::logic_error("not equi var");
      }
      else
        var = var2;
    }
    if(!var.has_value())
      throw std::logic_error("no subexpression found");
    return *var;
  }

  std::string asEquiPropertyTree() const override
  {
    std::optional<std::string> prop;
    for(const auto & expr : subExpressions())
    {
      const std::string prop2 = expr->asEquiPropertyTree();
      if(prop.has_value())
      {
        if(*prop != prop2)
          throw std::logic_error("not equi prop");
      }
      else
        prop = prop2;
    }
    if(!prop.has_value())
      throw std::logic_error("no subexpression found");
    return *prop;
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::unordered_set<std::string>& sqlFields) const override
  {
    std::vector<std::unique_ptr<sql::Expression>> sqlSubExprs;
    for(auto const & exp : m_subExprs.get())
      sqlSubExprs.push_back(exp->toSQLExpressionTree(sqlFields));
    return std::make_unique<sql::AggregateExpression>(toSqlAggregator(aggregator()), std::move(sqlSubExprs));
  }

  Aggregator aggregator() const { return m_aggregator; }

  std::unique_ptr<Expression> StealAsPtr() override
  {
    auto ptr = std::make_unique<AggregateExpression>(m_aggregator);
    *ptr = std::move(*this);
    return ptr;
  };
private:
  SubExpressions m_subExprs;
  Aggregator m_aggregator;
};

struct Atom
{
  std::variant<Variable, Literal, AggregateExpression> var;
};

struct NonArithmeticOperatorExpression : public Expression
{
  static constexpr const char * c_name {"NonArithmeticOperatorExpression"};
  
  Atom atom;
  std::optional<PropertyKeyName> mayPropertyName;
  
  std::unique_ptr<Expression> StealAsPtr() override
  {
    auto ptr = std::make_unique<NonArithmeticOperatorExpression>();
    *ptr = std::move(*this);
    return ptr;
  };
  
  void asAndEquiVarSubTrees(std::unordered_map<std::string /*variable*/, std::vector<const Expression*>>& res) const override
  {
    throw std::logic_error("NonArithmeticOperatorExpression cannot be used as a root of a WHERE expression (?)");
  }
  std::string asEquiVarTree() const override
  {
    return std::visit([&](auto && arg) -> std::string {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        if(!mayPropertyName.has_value())
          // in equi-var trees, all nodes must use _properties_ of the same variable.
          throw std::logic_error("no property");
        return arg.symbolicName.str;
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        throw std::logic_error("literal");
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.asEquiVarTree();
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, atom.var);
  }

  std::string asEquiPropertyTree() const override
  {
    return std::visit([&](auto && arg) -> std::string {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        if(!mayPropertyName.has_value())
          // in equi-var trees, all nodes must use _properties_ of the same variable.
          throw std::logic_error("no property");
        return mayPropertyName->symbolicName.str;
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        throw std::logic_error("literal");
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.asEquiPropertyTree();
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, atom.var);
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::unordered_set<std::string>& sqlFields) const override
  {
    return std::visit([&](auto && arg) -> std::unique_ptr<sql::Expression> {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        if(!mayPropertyName.has_value())
          throw std::logic_error("cannot use a raw variable in SQL, need to have a property");
        if(0 == sqlFields.count(mayPropertyName->symbolicName.str))
          // The property is not a SQL field so we return a null node.
          return std::make_unique<sql::Null>();
        else
          // The property is a SQL field so we return it as-is.
          return std::make_unique<sql::Field>(mayPropertyName->symbolicName.str);
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        if(mayPropertyName.has_value())
          throw std::logic_error("A literal should have no property");
        return std::make_unique<sql::Literal>(arg.str);
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.toSQLExpressionTree(sqlFields);
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    },atom.var);
  }

};

struct PartialComparisonExpression{
  Comparison comp;
  NonArithmeticOperatorExpression rightExp;
};
struct ComparisonExpression : public Expression {
  static constexpr const char * c_name {"ComparisonExpression"};
  
  NonArithmeticOperatorExpression leftExp;
  PartialComparisonExpression partial;
  
  std::unique_ptr<Expression> StealAsPtr() override
  {
    auto ptr = std::make_unique<ComparisonExpression>();
    *ptr = std::move(*this);
    return ptr;
  };

  void asAndEquiVarSubTrees(std::unordered_map<std::string /*variable*/, std::vector<const Expression*>>& res) const override
  {
    res[asEquiVarTree()].push_back(this);
  }
  std::string asEquiVarTree() const override
  {
    const std::string varLeft = leftExp.asEquiVarTree();
    bool ok = true;
    try
    {
      const std::string varRight = partial.rightExp.asEquiVarTree();
      if(varRight != varLeft)
        ok = false;
    }
    catch(std::exception &)
    {
      // means the right part is a Literal.
    }
    if(!ok)
      throw std::logic_error("ComparisonExpression: Right and Left variables are different");
    return varLeft;
  }
  std::string asEquiPropertyTree() const override
  {
    const std::string propLeft = leftExp.asEquiPropertyTree();
    bool ok = true;
    try
    {
      const std::string propRight = partial.rightExp.asEquiPropertyTree();
      if(propRight != propLeft)
        ok = false;
    }
    catch(std::exception &)
    {
      // means the right part is a Literal.
    }
    if(!ok)
      throw std::logic_error("ComparisonExpression: Right and Left properties are different");
    return propLeft;
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::unordered_set<std::string>& sqlFields) const override
  {
    // At this point we can have these cases:
    //
    // node.property = 1
    // node.property = node.otherProperty
    //
    // we cannot have cases like this:
    //
    // node.property = otherNode.property
    // node.property = otherNode.otherProperty
    // node = otherNode
    // node = 1

    std::unique_ptr<sql::Expression> left = leftExp.toSQLExpressionTree(sqlFields);
    std::unique_ptr<sql::Expression> right = partial.rightExp.toSQLExpressionTree(sqlFields);
    return std::make_unique<sql::ComparisonExpression>(std::move(left), partial.comp, std::move(right));
  }
};

struct WhereClause{
  // cannot be unique_ptr because std::any only wraps classes that are copyable.
  std::shared_ptr<Expression> exp;
};

struct Match{
  Pattern pattern;
  std::optional<WhereClause> where;
};
struct ReadingClause{
  // todo support UNWIND
  Match match;
};

// Not used yet in valid cases.
struct ListOperatorExpression{};

struct ProjectionItems
{
  // simplified wrt grammar... might need to be refactored later.
  std::vector<NonArithmeticOperatorExpression> naoExps;
};
// simplified wrt grammar... might need to be refactored later.
using ProjectionBody = ProjectionItems;
using Return = ProjectionBody;
struct SinglePartQuery{
  std::optional<ReadingClause> mayReadingClause;
  Return returnClause;
};
struct SingleQuery{
  SinglePartQuery singlePartQuery;
};
// Will be needed later
/*
 struct FunctionName{
 std::string namespaceStr;
 std::string funcName;
 };
 */
struct IdentityFunction{};

} // NS
