#include <chrono>
#include <filesystem>

#include "GraphDBSqlite.h"
#include "CypherQuery.h"
#include "Logs.h"
#include "TestUtils.h"

namespace openCypher::test
{


std::set<Value> mkSet(std::initializer_list<std::reference_wrapper<const Value>>&& values)
{
  std::set<Value> res;
  for(const auto & i : values)
    res.insert(copy(i));
  return res;
}

std::set<std::vector<Value>> toSet(const std::vector<std::vector<Value>>& vecValues)
{
  std::set<std::vector<Value>> res;
  for(const auto & values : vecValues)
  {
    std::vector<Value> v;
    v.reserve(values.size());
    for(const auto & val : values)
      v.push_back(copy(val));
    res.insert(std::move(v));
  }
  return res;
}

}
