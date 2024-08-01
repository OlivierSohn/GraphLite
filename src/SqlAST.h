

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <iostream>

template < typename > constexpr bool c_false = false;

namespace sql
{

struct QueryVars
{
  // There is a convention in sqlite to identify bound variables by their position in the query.
  // So the order of calls to this method must match the query string order.
  std::string addVar(std::vector<int64_t> const & value)
  {
    std::string varName{"?" + std::to_string(m_nextKey)};
    m_variables[m_nextKey] = value;
    ++m_nextKey;
    return varName;
  }
  const std::map<int, std::vector<int64_t>> & vars() const { return m_variables; }
private:
  // key starts at 1
  std::map<int, std::vector<int64_t>> m_variables;
  int m_nextKey {1};
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

  virtual std::optional<Evaluation> tryEvaluate() = 0;
  
  virtual void toString(std::ostream& os, QueryVars& vars) const = 0;
};

struct Literal : public Expression
{
  Literal(std::variant<std::string, std::vector<int64_t>> const& variant)
  : variant(variant)
  {}
  std::optional<Evaluation> tryEvaluate() override { return std::nullopt; }
  void toString(std::ostream& os, QueryVars& vars) const override {
    std::visit([&](auto && arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, std::string>)
        os << arg;
      else if constexpr (std::is_same_v<T, std::vector<int64_t>>)
      {
        // Note that using a bound variable with carray has not increased performance noticeably.
        if(!m_varName.has_value())
          m_varName = vars.addVar(arg);
        os << "carray(" << *m_varName << ")";
      }
      else
        static_assert(c_false<T>, "non-exhaustive visitor!");
    }, variant);
  }

  std::variant<std::string, std::vector<int64_t>> variant;

private:
  mutable std::optional<std::string> m_varName;
};

struct Field : public Expression
{
  Field(std::string const& str)
  : str(str)
  {}
  std::optional<Evaluation> tryEvaluate() override { return std::nullopt; }
  void toString(std::ostream& os, QueryVars& vars) const override
  {
    os << str;
  }

  std::string str;
};

// Represents a null value.
struct Null : public Expression
{
  std::optional<Evaluation> tryEvaluate() override { return Evaluation::Unknown; }
  void toString(std::ostream& os, QueryVars& vars) const override { os << "NULL"; }
};

struct ComparisonExpression : public Expression {
  ComparisonExpression(std::unique_ptr<Expression> && left, const Comparison comp, std::unique_ptr<Expression> && right)
  : m_comp(comp)
  , m_left(std::move(left))
  , m_right(std::move(right))
  {}

  std::optional<Evaluation> tryEvaluate() override
  {
    auto leftEval = m_left->tryEvaluate();
    auto rightEval = m_right->tryEvaluate();
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
    m_left->toString(os, vars);
    os << " ";
    os << toStr(m_comp);
    os << " ";
    m_right->toString(os, vars);
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
      if(std::holds_alternative<std::vector<int64_t>>(lit->variant))
        type = Type::InList;
    }
    if(!type.has_value())
      throw std::logic_error("not String / Null predicates not supported yet.");
    m_type = *type;
  }
  
  std::optional<Evaluation> tryEvaluate() override
  {
    // This works for the List case i.e "a.prop IN [1, 2]",
    // but might need to be revisited for otehr cases when we support them.
    auto leftEval = m_left->tryEvaluate();
    auto rightEval = m_right->tryEvaluate();
    if(leftEval.has_value() && *leftEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    if(rightEval.has_value() && *rightEval == Evaluation::Unknown)
      return Evaluation::Unknown;
    return std::nullopt;
  }
  
  void toString(std::ostream& os, QueryVars& vars) const override
  {
    m_left->toString(os, vars);
    os << " IN ";
    m_right->toString(os, vars);
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

  std::optional<Evaluation> tryEvaluate() override
  {
    switch(m_aggregator)
    {
      case Aggregator::AND:
      {
        bool hasUnknown{};
        bool hasNonEvaluated{};
        for(const auto & subExpr : m_subExprs)
        {
          if(auto subEval = subExpr->tryEvaluate())
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
          if(auto subEval = subExpr->tryEvaluate())
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
