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

#include "CypherQuery.h"
#include "antlr4-runtime.h"
#include "cypherparser/CypherParser.h"
#include "cypherparser/CypherLexer.h"
#include "cypherparser/CypherListener.h"
#include "MyCypherVisitor.h"

namespace openCypher::detail
{
RegularQuery cypherQueryToAST(const PropertySchema& idProperty,
                              const std::string& query,
                              const std::map<ParameterName, HomogeneousNonNullableValues>& queryParams,
                              const bool printAST)
{
  auto chars = antlr4::ANTLRInputStream(query);
  auto lexer = CypherLexer(&chars);
  auto tokens = antlr4::CommonTokenStream(&lexer);
  auto parser = CypherParser(&tokens);

  //parser.setBuildParseTree(false);
  parser.setTrimParseTree(true);
  
  // Could be slightly faster with:
  // parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
  
  CypherParser::OC_CypherContext* cypherTree = parser.oC_Cypher();
  
  auto visitor = MyCypherVisitor(idProperty, queryParams, printAST);
  auto resVisit = visitor.visit(cypherTree);
  
  if(!visitor.getErrors().empty())
  {
    std::ostringstream s;
    for(const auto& err : visitor.getErrors())
      s << "  " << err << std::endl;
    throw std::logic_error("Visitor errors:\n" + s.str());
  }
  
  if(resVisit.type() != typeid(RegularQuery))
    throw std::logic_error("No RegularQuery was returned.");
  return std::any_cast<RegularQuery>(resVisit);;
}

std::vector<std::string>
extractColumnNames(const std::vector<ProjectionItem>& items)
{
  std::vector<std::string> res;
  res.reserve(items.size());
  for(const auto & [nao, mayVar] : items)
  {
    if(mayVar.has_value())
      res.push_back(mayVar->symbolicName.str);
    else
    {
      std::string name = std::get<openCypher::Variable>(nao.atom.var).symbolicName.str;
      if(nao.mayPropertyName.has_value())
      {
        name += ".";
        name += nao.mayPropertyName->symbolicName.str;
      }
      res.push_back(std::move(name));
    }
  }
  return res;
}

} // NS
