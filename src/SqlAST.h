

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <iostream>
#include <sstream>
#include <map>

#include "Value.h"

template < typename > constexpr bool c_false = false;

namespace sql
{

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

struct Expression
{
  virtual ~Expression() = default;

  virtual std::optional<Evaluation> tryEvaluate(std::string const& elementType) = 0;
  
  virtual void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const = 0;
};

struct Literal : public Expression
{
  Literal(std::variant<std::shared_ptr<Value>, HomogeneousNonNullableValues> const& variant)
  : m_variant(variant)
  {}

  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override { return std::nullopt; }

  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override {
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

struct Field : public Expression
{
  Field(std::string const& str)
  : str(str)
  {}
  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override { return std::nullopt; }
  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override
  {
    os << str;
  }

  std::string str;
};

// Represents a null value.
struct Null : public Expression
{
  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override { return Evaluation::Unknown; }
  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override { os << "NULL"; }
};

// Represents a null value.
struct AllowedTypes : public Expression
{
  std::set<std::string> allowedTypes;

  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override {
    if(allowedTypes.count(elementType))
      return Evaluation::True;
    return Evaluation::False;
  }
  
  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override {
    if(allowedTypes.count(elementType))
      os << " TRUE ";
    else
      os << " FALSE ";
  }
};

struct ComparisonExpression : public Expression {
  ComparisonExpression(std::unique_ptr<Expression> && left, const Comparison comp, std::unique_ptr<Expression> && right)
  : m_comp(comp)
  , m_left(std::move(left))
  , m_right(std::move(right))
  {}

  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override
  {
    auto leftEval = m_left->tryEvaluate(elementType);
    auto rightEval = m_right->tryEvaluate(elementType);
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

  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override
  {
    m_left->toString(os, elementType, vars);
    os << " ";
    os << toStr(m_comp);
    os << " ";
    m_right->toString(os, elementType, vars);
  }

private:
  std::unique_ptr<Expression> m_left;
  Comparison m_comp;
  std::unique_ptr<Expression> m_right;
};

// For now does not support String / Null parts, only List part.
struct StringListNullPredicateExpression : public Expression {
  StringListNullPredicateExpression(std::unique_ptr<Expression> && left, std::unique_ptr<Expression> && right)
  : m_left(std::move(left))
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
  
  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override
  {
    // This works for the List case i.e "a.prop IN [1, 2]",
    // but might need to be revisited for otehr cases when we support them.
    auto leftEval = m_left->tryEvaluate(elementType);
    auto rightEval = m_right->tryEvaluate(elementType);
    if(leftEval.has_value() && *leftEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    if(rightEval.has_value() && *rightEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    return std::nullopt;
  }
  
  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override
  {
    m_left->toString(os, elementType, vars);
    os << " IN ";
    m_right->toString(os, elementType, vars);
  }
  
private:
  std::unique_ptr<Expression> m_left;
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

  std::optional<Evaluation> tryEvaluate(std::string const& elementType) override
  {
    switch(m_aggregator)
    {
      case Aggregator::AND:
      {
        bool hasUnknown{};
        bool hasNonEvaluated{};
        for(const auto & subExpr : m_subExprs)
        {
          if(auto subEval = subExpr->tryEvaluate(elementType))
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
          if(auto subEval = subExpr->tryEvaluate(elementType))
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

  void toString(std::ostream& os, std::string const& elementType, QueryVars& vars) const override {
    bool first = true;
    for(const auto & subExpr : m_subExprs)
    {
      if(first)
        first = false;
      else
        os << toStr(m_aggregator);
      os << " (";
      subExpr->toString(os, elementType, vars);
      os << ") ";
    }
  }

private:
  Aggregator m_aggregator;
  std::vector<std::unique_ptr<Expression>> m_subExprs;
};

}
