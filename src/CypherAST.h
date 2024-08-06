

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <unordered_set>
#include <set>
#include <map>
#include <any>
#include <functional>

#include "SqlAST.h"
#include "Value.h"
#include "Logs.h"

// All types defined here my need to be refactored later as more of the openCypher grammar is supported.

namespace openCypher
{
using Comparison = sql::Comparison;


struct SymbolicName
{
  std::string str;
  
  // We intentionnaly don't have < and == operators so that this cannot be the key of a map.
};

inline std::ostream& operator<<(std::ostream& os, const SymbolicName& p)
{
  os << p.str;
  return os;
}

struct ParameterName
{
  SymbolicName symbolicName;
  
  friend bool operator<(const ParameterName&a, const ParameterName&b)
  {
    return a.symbolicName.str < b.symbolicName.str;
  }
  friend bool operator==(const ParameterName&a, const ParameterName&b)
  {
    return a.symbolicName.str == b.symbolicName.str;
  }
};




struct Label
{
  SymbolicName symbolicName;
  
  friend bool operator<(const Label& a, const Label& b)
  { return a.symbolicName.str < b.symbolicName.str; }
  
  friend bool operator==(const Label& a, const Label& b)
  { return a.symbolicName.str == b.symbolicName.str; }
};
inline std::ostream& operator<<(std::ostream& os, const Label& p)
{
  os << p.symbolicName;
  return os;
}

}  // NS

namespace std
{
template<>
struct hash<openCypher::Label>
{
  size_t operator()(const openCypher::Label& l) const
  {
    return std::hash<std::string>()(l.symbolicName.str);
  }
};
}  // NS


namespace openCypher
{

struct IndexedLabels
{
  using Index = sql::ElementTypeIndex;
  
  std::optional<Index> getIfExists(Label const & type) const
  {
    auto it = typeToIndex.find(type);
    if(it == typeToIndex.end())
      return {};
    return it->second;
  }
  const Label* getIfExists(Index const type) const
  {
    auto it = indexToType.find(type);
    if(it == indexToType.end())
      return {};
    return &it->second;
  }
  
  void add(Index idx, Label const & name)
  {
    auto it = typeToIndex.find(name);
    if(it != typeToIndex.end())
      throw std::logic_error("duplicate type");
    typeToIndex[name] = idx;
    indexToType[idx] = name;
    if(m_maxIndex.has_value())
      m_maxIndex = std::max(idx, *m_maxIndex);
    else
      m_maxIndex = idx;
  }
  
  const std::unordered_map<Label, Index>& getTypeToIndex() const { return typeToIndex; }
  
  const std::optional<Index> getMaxIndex() const { return m_maxIndex; }
private:
  std::unordered_map<Label, Index> typeToIndex;
  std::unordered_map<Index, Label> indexToType;
  std::optional<Index> m_maxIndex;
};



struct Variable
{
  SymbolicName symbolicName;
  
  friend bool operator<(const Variable&a, const Variable&b)
  {
    return a.symbolicName.str < b.symbolicName.str;
  }
  friend bool operator==(const Variable&a, const Variable&b)
  {
    return a.symbolicName.str == b.symbolicName.str;
  }
};
inline std::ostream& operator<<(std::ostream& os, const Variable& p)
{
  os << p.symbolicName;
  return os;
}


struct SchemaName
{
  SymbolicName symbolicName;
};


struct Labels
{
  // The labels constraints are AND-ed.
  std::set<Label> labels;
  
  bool empty() const { return labels.empty(); }
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


// Should this inherit from Expression? in the sql AST, sql::Literal inherits from sql::Expression.
struct Literal
{
  std::variant<std::shared_ptr<Value>, HomogeneousNonNullableValues> variant;
  
  std::unique_ptr<sql::Expression> toSQLExpressionTree() const
  {
    return std::make_unique<sql::Literal>(variant);
  }
};


struct PropertyKeyName
{
  SymbolicName symbolicName;
  
  friend bool operator<(const PropertyKeyName&a, const PropertyKeyName&b)
  {
    return a.symbolicName.str < b.symbolicName.str;
  }
  friend bool operator==(const PropertyKeyName&a, const PropertyKeyName&b)
  {
    return a.symbolicName.str == b.symbolicName.str;
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

} // NS


enum class IsNullable{ Yes, No };

struct PropertySchema
{
  PropertySchema(const openCypher::PropertyKeyName & name,
                 ValueType type=ValueType::Integer,
                 IsNullable nullable=IsNullable::Yes,
                 std::shared_ptr<Value> defaultValue={})
  : name(name)
  , type(type)
  , isNullable(nullable)
  , defaultValue(defaultValue)
  {}
  
  openCypher::PropertyKeyName name;
  ValueType type;
  IsNullable isNullable{IsNullable::No};
  
  // std::shared_ptr used here has the same semantic meaning as an std::optional.
  // The reason we use std::shared_ptr instead of std::optional is because
  // a 'Value' is not copyable and we want to avoid having to write a copy constructor
  // in this class.
  std::shared_ptr<Value> defaultValue;
  
  // The name is the key, i.e we cannot have two properties (for the same entity or relationship type) with the same name.
  friend bool operator< (const PropertySchema& a, const PropertySchema& b)
  {
    return a.name < b.name;
  }
};


// Represents information related to an openCypher variable when building a SQL query.
//
// When building the system relationships query, only |cypherPropertyToSQLQueryColumnName| and |typeIndexSQLQueryColumn| are used :
//   |cypherPropertyToSQLQueryColumnName| is used to map the ID property field.
//
// When building a typed property table query, only |variableLabel| is used.
struct VarQueryInfo {
  
  VarQueryInfo(const openCypher::IndexedLabels & indexedTypes)
  : allElementTypes(indexedTypes)
  {}
  
  // How the property names should be serialized in the query.
  std::map<openCypher::PropertyKeyName, sql::QueryColumnName> cypherPropertyToSQLQueryColumnName;
  
  // The column name representing the type index in the query.
  std::optional<sql::QueryColumnName> typeIndexSQLQueryColumn;
  
  // when this has a value, we can assume the variable has these labels.
  std::optional<std::set<openCypher::Label>> variableLabels;
  
  // The indexed types available for the variable "kind" (i.e node or relationship) in the DB
  std::reference_wrapper<const openCypher::IndexedLabels> allElementTypes;
};


namespace openCypher
{

struct VarUsage
{
  // These properties of the variable are used
  std::set<PropertyKeyName> properties;

  // A label constraint is used with this variable.
  bool usedInLabelConstraints{};
  
  bool operator <(VarUsage const & other) const
  {
    return std::tie(usedInLabelConstraints, properties) <
    std::tie(other.usedInLabelConstraints, other.properties);
  }
};

using VarsUsages = std::map<Variable, VarUsage>;

inline void merge(VarsUsages&& v, VarsUsages& res)
{
  if(res.empty())
    res = std::move(v);
  else
  {
    for(auto& [var, varUsage1] : v)
    {
      VarUsage & varUsage2 = res[var];

      if(varUsage1.usedInLabelConstraints)
        varUsage2.usedInLabelConstraints = true;

      if(varUsage2.properties.empty())
        varUsage2.properties = std::move(varUsage1.properties);
      else
        for(const auto& prop : varUsage1.properties)
          varUsage2.properties.insert(prop);
    }
  }
}

struct Expression;

using ExpressionsByVarsUsages = std::map<VarsUsages, std::vector<const Expression*>>;

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
  // Example: in the expression
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
  // If the detailed expression is:
  //
  // ((a.style=3 OR a.style=5 OR a.type=50)  AND  (r.length<10 AND b.weight > 30))  AND  (a.type=100 OR b.type=100)
  //
  // 4, 7, 8 are equi-var expressions:
  // - 4 uses variable 'a' (with properties 'style' and 'type')
  // - 7 uses variable 'r' (with property 'length')
  // - 8 uses variable 'b' (with property 'weight')
  //
  // 10 is not an equi-var expression:
  // - 10 uses variables 'a' (with property 'type'), and variable 'b' (with property 'type')
  virtual void asMaximalANDAggregation(ExpressionsByVarsUsages& exprs) const = 0;

  virtual VarsUsages varsUsages() const = 0;
  // throws if the translation is not supported yet.
  virtual std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<PropertySchema>& sqlFields,
                      const std::map<Variable, VarQueryInfo>& varsQueryInfo) const = 0;
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

  void asMaximalANDAggregation(ExpressionsByVarsUsages& exprs) const override
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
        exprs[varsUsages()].push_back(this);
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
          exprs[exp->varsUsages()].push_back(exp.get());
        }
        break;
    }
  }

  VarsUsages varsUsages() const override
  {
    VarsUsages res;
    for(const auto & exp: subExpressions())
    {
      merge(exp->varsUsages(), res);
    }
    return res;
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<PropertySchema>& sqlFields,
                      const std::map<Variable, VarQueryInfo>& varsQueryInfo) const override
  {
    std::vector<std::unique_ptr<sql::Expression>> sqlSubExprs;
    for(auto const & exp : m_subExprs.get())
      sqlSubExprs.push_back(exp->toSQLExpressionTree(sqlFields, varsQueryInfo));
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
  Labels labels;

  std::unique_ptr<Expression> StealAsPtr() override
  {
    auto ptr = std::make_unique<NonArithmeticOperatorExpression>();
    *ptr = std::move(*this);
    return ptr;
  };
  
  void asMaximalANDAggregation(ExpressionsByVarsUsages& exprs) const override
  {
    if(mayPropertyName.has_value())
      throw std::logic_error("asMaximalANDAggregation not implemented for NonArithmeticOperatorExpression that has a property name");

    std::visit([&](auto && arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        if(labels.empty())
          throw std::logic_error("asMaximalANDAggregation expects a label for a variable.");
        exprs[varsUsages()].push_back(this);
      }
      else if constexpr (std::is_same_v<T, Literal>)
        throw std::logic_error("asMaximalANDAggregation didn't expect a literal.");
      else if constexpr (std::is_same_v<T, AggregateExpression>)
        arg.asMaximalANDAggregation(exprs);
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, atom.var);
  }

  VarsUsages varsUsages() const override
  {
    return std::visit([&](auto && arg) -> VarsUsages {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        VarsUsages res;
        auto & varUsage = res[arg];
        if(mayPropertyName.has_value())
          varUsage.properties.insert(*mayPropertyName);
        if(!labels.empty())
          varUsage.usedInLabelConstraints = true;
        return res;
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        return {};
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.varsUsages();
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, atom.var);
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<PropertySchema>& sqlFields,
                      const std::map<Variable, VarQueryInfo>& varsQueryInfo) const override
  {
    return std::visit([&](auto && arg) -> std::unique_ptr<sql::Expression> {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, Variable>)
      {
        const auto itVarInfo = varsQueryInfo.find(arg);
        if(itVarInfo == varsQueryInfo.end())
          throw std::logic_error("toSQLExpressionTree doesn't have required information for the var.");

        const VarQueryInfo& info = itVarInfo->second;

        if(!mayPropertyName.has_value())
        {
          if(labels.empty())
            throw std::logic_error("cannot use a raw variable in SQL, need to have a property or a label constraint");
          else
          {
            if(info.variableLabels.has_value())
            {
              const bool labelConstraintOK = [&]()
              {
                for(const auto & requiredLabel : labels.labels)
                  if(!info.variableLabels->count(requiredLabel))
                    return false;
                return true;
              }();
              if(labelConstraintOK)
                return std::make_unique<sql::True>();
              else
                return std::make_unique<sql::False>();
            }
            else
            {
              // we don't know which label(s) the elements corresponding ot the variable will have
              if(!info.typeIndexSQLQueryColumn.has_value())
                throw std::logic_error("toSQLExpressionTree: var info must either have labels or type index sql query column.");
              std::set<sql::ElementTypeIndex> typeIndices;
              for(const auto & label: labels.labels)
              {
                if(auto index = info.allElementTypes.get().getIfExists(label))
                  typeIndices.insert(*index);
                else
                  // a required label does not exist as type in the DB.
                  return std::make_unique<sql::False>();
              }
              return std::make_unique<sql::ElementLabelsConstraints>(*info.typeIndexSQLQueryColumn, typeIndices);
            }
          }
        }
        if(0 == sqlFields.count(PropertySchema{
          *mayPropertyName,
          // The ValueType is ignored when comparing keys so we can use any ValueType here.
          ValueType::String}))
        {
          // The property is not a SQL field so we return a null node.
          return std::make_unique<sql::Null>();
        }
        else
        {
          // The property is a SQL Table Column so we return it.
          
          if(const auto it = info.cypherPropertyToSQLQueryColumnName.find(*mayPropertyName); it != info.cypherPropertyToSQLQueryColumnName.end())
            return std::make_unique<sql::QueryColumn>(it->second);
          // The contract is that the caller will use the property name as query column name.
          return std::make_unique<sql::QueryColumn>(sql::QueryColumnName{mayPropertyName->symbolicName.str});
        }
      }
      else if constexpr (std::is_same_v<T, Literal>)
      {
        if(mayPropertyName.has_value())
          throw std::logic_error("A literal should have no property");
        return arg.toSQLExpressionTree();
      }
      else if constexpr (std::is_same_v<T, AggregateExpression>)
      {
        return arg.toSQLExpressionTree(sqlFields, varsQueryInfo);
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

  void asMaximalANDAggregation(ExpressionsByVarsUsages& exprs) const override
  {
    exprs[varsUsages()].push_back(this);
  }
  VarsUsages varsUsages() const override
  {
    VarsUsages left = leftExp.varsUsages();
    VarsUsages right = partial.rightExp.varsUsages();
    merge(std::move(right), left);
    return left;
  }

  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<PropertySchema>& sqlFields,
                      const std::map<Variable, VarQueryInfo>& varsQueryInfo) const override
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

    std::unique_ptr<sql::Expression> left = leftExp.toSQLExpressionTree(sqlFields, varsQueryInfo);
    std::unique_ptr<sql::Expression> right = partial.rightExp.toSQLExpressionTree(sqlFields, varsQueryInfo);
    return std::make_unique<sql::ComparisonExpression>(std::move(left), partial.comp, std::move(right));
  }
};


// For now does not support String / Null parts, only List part.
struct StringListNullPredicateExpression : public Expression {
  static constexpr const char * c_name {"ComparisonExpression"};
  
  NonArithmeticOperatorExpression leftExp;
  
  // will be a variant later.
  // For the List case, inList.variant is a std::vector<std::string>
  Literal inList;
  
  std::unique_ptr<Expression> StealAsPtr() override
  {
    auto ptr = std::make_unique<StringListNullPredicateExpression>();
    *ptr = std::move(*this);
    return ptr;
  };
  
  void asMaximalANDAggregation(ExpressionsByVarsUsages& exprs) const override
  {
    exprs[varsUsages()].push_back(this);
  }
  VarsUsages varsUsages() const override
  {
    return leftExp.varsUsages();
  }
  
  std::unique_ptr<sql::Expression>
  toSQLExpressionTree(const std::set<PropertySchema>& sqlFields,
                      const std::map<Variable, VarQueryInfo>& varsQueryInfo) const override
  {
    std::unique_ptr<sql::Expression> left = leftExp.toSQLExpressionTree(sqlFields, varsQueryInfo);
    std::unique_ptr<sql::Expression> right = inList.toSQLExpressionTree();
    return std::make_unique<sql::StringListNullPredicateExpression>(std::move(left), std::move(right));
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
  std::vector<NonArithmeticOperatorExpression> naoExps;
};


struct Limit {
  size_t maxCountRows;
};


struct ProjectionBody{
  std::optional<Limit> limit;
  ProjectionItems items;
};

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
