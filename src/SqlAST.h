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

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <iostream>
#include <sstream>
#include <map>
#include <set>

#include "Value.h"

template < typename > constexpr bool c_false = false;


// Whether a node or relationship may have multiple labels or just one.
enum class CountLabelsPerElement
{
  One,
  Multi
};

namespace sql
{

// Index of the type of an element (node or relationship)
struct ElementTypeIndex
{
  ElementTypeIndex() : ElementTypeIndex(std::numeric_limits<size_t>::max()) {}

  ElementTypeIndex(size_t i)
  : m_index(i)
  {}

  size_t unsafeGet() const { return m_index; }

  bool operator < (ElementTypeIndex const & other) const
  { return m_index < other.m_index; }
  friend bool operator == (ElementTypeIndex const & a, ElementTypeIndex const & b)
  { return a.m_index == b.m_index; }

private:
  size_t m_index;
};


struct QueryVars
{
  // There is a convention in sqlite to identify bound variables by their position in the query.
  // So the order of calls to this method must match the query string order.
  std::string addVar(HomogeneousNonNullableValues const & value)
  {
    m_variables[m_nextKey] = value;
    return nextName();
  }
  
  const std::map<int, HomogeneousNonNullableValues> & vars() const { return m_variables; }
  
private:
  // key starts at 1 because of https://www.sqlite.org/c3ref/bind_blob.html:
  // The NNN value must be between 1 and the sqlite3_limit() parameter SQLITE_LIMIT_VARIABLE_NUMBER (default value: 32766).
  std::map<int, HomogeneousNonNullableValues> m_variables;
  int m_nextKey {1};
  
  std::string nextName()
  {
    std::ostringstream s;
    s << "carray(?" << m_nextKey << ")";
    ++m_nextKey;
    return s.str();
  }
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

inline Comparison negateComparison(Comparison c)
{
  switch(c)
  {
    case Comparison::EQ:
      return Comparison::NE;
    case Comparison::NE:
      return Comparison::EQ;

    case Comparison::GT:
      return Comparison::LE;
    case Comparison::LE:
      return Comparison::GT;

    case Comparison::GE:
      return Comparison::LT;
    case Comparison::LT:
      return Comparison::GE;
  }
}

// Cypher and SQL comparison strings are the same.
inline std::string toStr(Comparison cmp)
{
  switch(cmp)
  {
    case Comparison::EQ: return "=";
    case Comparison::NE: return "<>";
    case Comparison::LT: return "<";
    case Comparison::LE: return "<=";
    case Comparison::GT: return ">";
    case Comparison::GE: return ">=";
  }
  throw std::logic_error("not supported");
}

enum class Evaluation
{
  False,
  Unknown, // for null
  True
};

inline Evaluation negated(Evaluation e)
{
  switch(e)
  {
    case Evaluation::False:
      return Evaluation::True;
    case Evaluation::True:
      return Evaluation::False;
  }
  return Evaluation::Unknown;
}

struct Expression
{
  virtual ~Expression() = default;

  virtual std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const = 0;
  
  virtual void toString(std::ostream& os, QueryVars& vars) const = 0;
};

struct Literal : public Expression
{
  Literal(std::variant<std::shared_ptr<Value>, HomogeneousNonNullableValues> const& variant)
  : m_variant(variant)
  {}

  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override { return std::nullopt; }

  void toString(std::ostream& os, QueryVars& vars) const override {
    std::visit([&](auto && arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, std::shared_ptr<Value>>)
        os << *arg;
      else if constexpr (std::is_same_v<T, HomogeneousNonNullableValues>)
      {
        if(!m_varName.has_value())
          m_varName = vars.addVar(arg);
        os << *m_varName;
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, m_variant);
  }

  auto& getVariant() const { return m_variant; }

private:
  std::variant<std::shared_ptr<Value>, HomogeneousNonNullableValues> m_variant;
  mutable std::optional<std::string> m_varName;
};


// Represent the name of a table column as it appears in a SQL query,
// i.e either the same as the table column name, or prefixed by the table name, or aliased, etc...
struct QueryColumnName {
  std::string name;
};


inline std::ostream& operator<<(std::ostream& os, const QueryColumnName& p)
{
  os << p.name;
  return os;
}


struct QueryColumn : public Expression
{
  QueryColumn(QueryColumnName const& str)
  : m_name(str)
  {}
  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override { return std::nullopt; }
  void toString(std::ostream& os, QueryVars& vars) const override
  {
    os << m_name;
  }

private:
  QueryColumnName m_name;
};

// Represents a null value.
struct Null : public Expression
{
  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override { return Evaluation::Unknown; }
  void toString(std::ostream& os, QueryVars& vars) const override { os << "NULL"; }
};

// Represents a TRUE value.
struct True : public Expression
{
  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override { return Evaluation::True; }
  void toString(std::ostream& os, QueryVars& vars) const override { os << "TRUE"; }
};

// Represents a FALSE value.
struct False : public Expression
{
  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override { return Evaluation::False; }
  void toString(std::ostream& os, QueryVars& vars) const override { os << "FALSE"; }
};

// Represents a negation.
struct Not : public Expression
{
  Not(std::unique_ptr<Expression> && expr)
  : m_expr(std::move(expr))
  {}

  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override
  {
    if(std::optional<Evaluation> eval = m_expr->tryEvaluate(countLabelsPerElement))
      return negated(*eval);
    return std::nullopt;
  }

  void toString(std::ostream& os, QueryVars& vars) const override
  {
    os << " NOT ( ";
    m_expr->toString(os, vars);
    os << " ) ";
  }

private:
  std::unique_ptr<Expression> m_expr;
};

struct ElementLabelsConstraints : public Expression
{
  // |typeIndexQueryColumn| is the name of the sql query column that will hold the type index information.
  // |labelsConstraintsANDed| are the labels that the element must have.
  ElementLabelsConstraints(const QueryColumnName& typeIndexQueryColumn,
                           const std::set<ElementTypeIndex>& labelsConstraintsANDed)
  : m_typeConstraintsANDed(labelsConstraintsANDed)
  , m_typeIndexQueryColumn(typeIndexQueryColumn)
  {}

  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override
  {
    if(countLabelsPerElement == CountLabelsPerElement::One)
      if(m_typeConstraintsANDed.size() >= 2)
        return Evaluation::False;
    return std::nullopt;
  }
  
  void toString(std::ostream& os, QueryVars& vars) const override
  {
    os << m_typeIndexQueryColumn << " IN ( ";

    bool first = true;
    for(const auto & typeIndex : m_typeConstraintsANDed)
    {
      if(first)
        first = false;
      else
        os << ", ";
      os << typeIndex.unsafeGet();
    }

    os << " ) ";
  }

private:
  std::set<ElementTypeIndex> m_typeConstraintsANDed;
  QueryColumnName m_typeIndexQueryColumn;
};

struct ComparisonExpression : public Expression {
  ComparisonExpression(std::unique_ptr<Expression> && left, const Comparison comp, std::unique_ptr<Expression> && right)
  : m_comp(comp)
  , m_left(std::move(left))
  , m_right(std::move(right))
  {}

  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override
  {
    auto leftEval = m_left->tryEvaluate(countLabelsPerElement);
    auto rightEval = m_right->tryEvaluate(countLabelsPerElement);
    if(leftEval.has_value() && *leftEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    if(rightEval.has_value() && *rightEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    if(leftEval.has_value() && rightEval.has_value())
    {
      // left and right evaluations are either true or false at this point.
      
      const bool left = *leftEval == Evaluation::True;
      const bool right = *rightEval == Evaluation::True;

      switch(m_comp)
      {
        case Comparison::EQ:
          return (left == right) ? Evaluation::True : Evaluation::False;
        case Comparison::NE:
          return (left != right) ? Evaluation::True : Evaluation::False;
        // we could support more cases in the future...
      }
    }
    return std::nullopt;
  }

  void toString(std::ostream& os, QueryVars& vars) const override
  {
    os << " ( ";
    m_left->toString(os, vars);
    os << " ) ";
    os << toStr(m_comp);
    os << " ( ";
    m_right->toString(os, vars);
    os << " ) ";
  }

private:
  std::unique_ptr<Expression> m_left;
  Comparison m_comp;
  std::unique_ptr<Expression> m_right;
};

// For now does not support String / Null parts, only List part.
struct StringListNullPredicateExpression : public Expression {
  StringListNullPredicateExpression(std::unique_ptr<Expression> && left, bool negate, std::unique_ptr<Expression> && right)
  : m_left(std::move(left))
  , m_negate(negate)
  , m_right(std::move(right))
  {
    std::optional<Type> type;
    if(auto lit = dynamic_cast<Literal*>(m_right.get()))
    {
      if(std::holds_alternative<HomogeneousNonNullableValues>(lit->getVariant()))
        type = Type::InList;
    }
    if(!type.has_value())
      throw std::logic_error("not String / Null predicates not supported yet.");
    m_type = *type;
  }
  
  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override
  {
    // This works for the List case i.e "a.prop IN [1, 2]",
    // but might need to be revisited for otehr cases when we support them.
    auto leftEval = m_left->tryEvaluate(countLabelsPerElement);
    auto rightEval = m_right->tryEvaluate(countLabelsPerElement);
    if(leftEval.has_value() && *leftEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    if(rightEval.has_value() && *rightEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    return std::nullopt;
  }
  
  void toString(std::ostream& os, QueryVars& vars) const override
  {
    m_left->toString(os, vars);
    if(m_negate)
      os << " NOT ";
    os << " IN ";
    m_right->toString(os, vars);
  }
  
private:
  std::unique_ptr<Expression> m_left;
  bool m_negate;
  std::unique_ptr<Expression> m_right;

  enum class Type{
    InList
  };
  Type m_type;
};


enum class Aggregator
{
  AND,
  OR,
};

inline std::string toStr(Aggregator a)
{
  switch(a)
  {
    case Aggregator::AND: return "AND";
    case Aggregator::OR: return "OR";
  }
  throw std::logic_error("invalid enum value");
}

struct AggregateExpression : public Expression
{
  AggregateExpression(Aggregator a, std::vector<std::unique_ptr<Expression>> && sub)
  : m_aggregator(a)
  , m_subExprs(std::move(sub))
  {}

  std::optional<Evaluation> tryEvaluate(const CountLabelsPerElement countLabelsPerElement) const override
  {
    switch(m_aggregator)
    {
      case Aggregator::AND:
      {
        bool hasUnknown{};
        bool hasNonEvaluated{};
        for(const auto & subExpr : m_subExprs)
        {
          if(auto subEval = subExpr->tryEvaluate(countLabelsPerElement))
          {
            switch(*subEval)
            {
              case Evaluation::Unknown:
                hasUnknown = true;
                break;
              case Evaluation::False:
                return Evaluation::False;
              case Evaluation::True:
                break;
            }
          }
          else
            hasNonEvaluated = true;
        }
        if(hasUnknown)
          return Evaluation::Unknown;
        if(hasNonEvaluated)
          return std::nullopt;
        return Evaluation::True;
        break;
      }

      case Aggregator::OR:
      {
        bool hasUnknown{};
        bool hasNonEvaluated{};
        for(const auto & subExpr : m_subExprs)
        {
          if(auto subEval = subExpr->tryEvaluate(countLabelsPerElement))
          {
            switch(*subEval)
            {
              case Evaluation::Unknown:
                hasUnknown = true;
                break;
              case Evaluation::False:
                break;
              case Evaluation::True:
                return Evaluation::True;
            }
          }
          else
            hasNonEvaluated = true;
        }
        if(hasNonEvaluated)
          return std::nullopt;
        if(hasUnknown)
          return Evaluation::Unknown;
        return Evaluation::False;
        break;
      }
    }
    throw std::logic_error("invalid enum value");
  }

  void toString(std::ostream& os, QueryVars& vars) const override {
    bool first = true;
    for(const auto & subExpr : m_subExprs)
    {
      if(first)
        first = false;
      else
        os << toStr(m_aggregator);
      os << " (";
      subExpr->toString(os, vars);
      os << ") ";
    }
  }

private:
  Aggregator m_aggregator;
  std::vector<std::unique_ptr<Expression>> m_subExprs;
};

}

namespace std
{
template<>
struct hash<sql::ElementTypeIndex>
{
  size_t operator()(const sql::ElementTypeIndex& i) const
  {
    return std::hash<size_t>()(i.unsafeGet());
  }
};

}
