

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <unordered_set>
#include <set>
#include <map>
#include <any>

#include "SqlAST.h"

template < typename > constexpr bool c_false = false;

namespace openCypher
{
using Comparison = sql::Comparison;

struct SymbolicName
{
  std::string str;

  friend bool operator<(const SymbolicName&a, const SymbolicName&b)
  {
    return a.str < b.str;
  }
  friend bool operator==(const SymbolicName&a, const SymbolicName&b)
  {
    return a.str == b.str;
  }
};
inline std::ostream& operator<<(std::ostream& os, const SymbolicName& p)
{
  os << p.str;
  return os;
}

struct Variable
{
  SymbolicName symbolicName;
  
  friend bool operator<(const Variable&a, const Variable&b)
  {
    return a.symbolicName < b.symbolicName;
  }
  friend bool operator==(const Variable&a, const Variable&b)
  {
    return a.symbolicName == b.symbolicName;
  }
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

  friend bool operator<(const PropertyKeyName&a, const PropertyKeyName&b)
  {
    return a.symbolicName < b.symbolicName;
  }
  friend bool operator==(const PropertyKeyName&a, const PropertyKeyName&b)
  {
    return a.symbolicName == b.symbolicName;
  }
};
inline PropertyKeyName mkProperty(std::string const & name){
  return PropertyKeyName{SymbolicName{name}};
}

inline std::ostream& operator<<(std::ostream& os, const PropertyKeyName& p)
{
  os << p.symbolicName;
  return os;
}

using VarsAndProperties = std::map<Variable, std::set<PropertyKeyName>>;

inline void merge(VarsAndProperties&& v, VarsAndProperties& res)
{
  if(res.empty())
    res = std::move(v);
  else
  {
    for(auto& [var, properties] : v)
    {
      auto & props = res[var];
      if(props.empty())
        props = std::move(properties);
      else
        for(const auto& prop : properties)
          props.insert(prop);
    }
  }
}

struct Expression;

using ExpressionsByVarAndProperties = std::map<VarsAndProperties, std::vector<const Expression*>>;

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
    
  // Returns |exprs| containing expressions grouped by used variables and properties.
  //
  // The Expression is equivalent to an And-aggregation of all expressions in |exprs|.
  //
  // Expressions in |exprs| are the deepest possible i.e we traverse successive
  // consecutive AND-aggregations from the top of the tree
  // to return the expressions of the deepest possible AND-aggregation.
  //
  // For example, in the expression
  //
  // ((1 OR 2 OR 3)  AND  (7 AND 8))  AND  (11 OR 12)
  //
  //   which corresponds to the expression tree
  //
  //                9(AND)
  //        ----------------------
  //      5(AND)               10(OR)
  //    ---------              -----
  //  4(OR)    6(AND)          11  12
  // -------   -------
  // 1  2  3   7     8
  //
  // we will return expressions 4, 7, 8, 10
  //
  // And if the exact expression is:
  //
  // ((a.style=3 OR a.style=5 OR a.type=50)  AND  (r.length<10 AND b.weight > 30))  AND  (a.type=100 OR b.type=100)
  //
  // 4, 7, 8 are equi-var expressions, 10 is not an equi-var expression:
  // - 10 uses variables 'a' (with property 'type'), and variable 'b' (with property 'type')
  // - 4 uses variable 'a' (with properties 'style' and 'type')
  // - 7 uses variable 'r' (with property 'length')
  // - 8 uses variable 'b' (with property 'weight')
  virtual void asMaximalANDAggregation(ExpressionsByVarAndProperties& exprs) const = 0;

  virtual VarsAndProperties varsAndProperties() const = 0;
  // throws if the translation is not supported yet.
  virtual std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<openCypher::PropertyKeyName>& sqlFields,
                      const std::map<Variable, std::map<PropertyKeyName, std::string>>& propertyMappingCypherToSQL) const = 0;
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

  void asMaximalANDAggregation(ExpressionsByVarAndProperties& exprs) const override
  {
    switch(m_aggregator)
    {
      case Aggregator::XOR:
        // SQL doesn't have XOR but we could transform the tree using this equivalency:
        //   a XOR b === (a OR b) AND NOT(a AND b)
        // Once the tree is transformed, if all subexpressions are equivar with the same var,
        // the tree will be equivar.
        throw std::logic_error("Xor is not supported");
        break;
      case Aggregator::OR:
        exprs[varsAndProperties()].push_back(this);
        break;
      case Aggregator::AND:
        for(const auto & exp: subExpressions())
        {
          // if exp is an AND aggregation, we recursively call this function on sub-expressions
          if(auto * aggr = dynamic_cast<const AggregateExpression*>(exp.get()))
          {
            if(aggr->aggregator() == Aggregator::AND)
            {
              for(const auto & subExpr : aggr->subExpressions())
                subExpr->asMaximalANDAggregation(exprs);
              continue;
            }
          }
          exprs[exp->varsAndProperties()].push_back(exp.get());
        }
        break;
    }
  }

  VarsAndProperties varsAndProperties() const override
  {
    VarsAndProperties res;
    for(const auto & exp: subExpressions())
    {
      merge(exp->varsAndProperties(), res);
    }
    return res;
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<openCypher::PropertyKeyName>& sqlFields,
                      const std::map<Variable, std::map<PropertyKeyName, std::string>>& propertyMappingCypherToSQL) const override
  {
    std::vector<std::unique_ptr<sql::Expression>> sqlSubExprs;
    for(auto const & exp : m_subExprs.get())
      sqlSubExprs.push_back(exp->toSQLExpressionTree(sqlFields, propertyMappingCypherToSQL));
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
  
  void asMaximalANDAggregation(ExpressionsByVarAndProperties& exprs) const override
  {
    // Probably a logic error, the owner of the NonArithmeticOperatorExpression should implement
    // asMaximalANDAggregation differently.
    throw std::logic_error("asMaximalANDAggregation not implemented for NonArithmeticOperatorExpression");
  }

  VarsAndProperties varsAndProperties() const override
  {
    return std::visit([&](auto && arg) -> VarsAndProperties {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        VarsAndProperties res;
        auto & props = res[arg];
        if(mayPropertyName.has_value())
          props.insert(*mayPropertyName);
        return res;
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        return {};
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.varsAndProperties();
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, atom.var);
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<openCypher::PropertyKeyName>& sqlFields,
                      const std::map<Variable, std::map<PropertyKeyName, std::string>>& propertyMappingCypherToSQL) const override
  {
    return std::visit([&](auto && arg) -> std::unique_ptr<sql::Expression> {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        if(!mayPropertyName.has_value())
          throw std::logic_error("cannot use a raw variable in SQL, need to have a property");
        if(0 == sqlFields.count(*mayPropertyName))
          // The property is not a SQL field so we return a null node.
          return std::make_unique<sql::Null>();
        else
        {
          // The property is a SQL field so we return it.
          
          if(const auto it = propertyMappingCypherToSQL.find(arg); it != propertyMappingCypherToSQL.end())
            if(const auto it2 = it->second.find(*mayPropertyName); it2 != it->second.end())
              return std::make_unique<sql::Field>(it2->second);
          return std::make_unique<sql::Field>(mayPropertyName->symbolicName.str);
        }
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        if(mayPropertyName.has_value())
          throw std::logic_error("A literal should have no property");
        return std::make_unique<sql::Literal>(arg.str);
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.toSQLExpressionTree(sqlFields, propertyMappingCypherToSQL);
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

  void asMaximalANDAggregation(ExpressionsByVarAndProperties& exprs) const override
  {
    exprs[varsAndProperties()].push_back(this);
  }
  VarsAndProperties varsAndProperties() const override
  {
    VarsAndProperties left = leftExp.varsAndProperties();
    VarsAndProperties right = partial.rightExp.varsAndProperties();
    merge(std::move(right), left);
    return left;
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<openCypher::PropertyKeyName>& sqlFields,
                      const std::map<Variable, std::map<PropertyKeyName, std::string>>& propertyMappingCypherToSQL) const override
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

    std::unique_ptr<sql::Expression> left = leftExp.toSQLExpressionTree(sqlFields, propertyMappingCypherToSQL);
    std::unique_ptr<sql::Expression> right = partial.rightExp.toSQLExpressionTree(sqlFields, propertyMappingCypherToSQL);
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
