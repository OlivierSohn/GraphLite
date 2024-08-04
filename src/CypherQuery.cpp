#include "CypherQuery.h"
#include "antlr4-runtime.h"
#include "cypherparser/CypherParser.h"
#include "cypherparser/CypherLexer.h"
#include "cypherparser/CypherListener.h"
#include "MyCypherVisitor.h"

namespace openCypher::detail
{
SingleQuery cypherQueryToAST(const PropertySchema& idProperty,
                             const std::string& query,
                             const std::map<SymbolicName, HomogeneousNonNullableValues>& queryParams,
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
  
  if(resVisit.type() != typeid(SingleQuery))
    throw std::logic_error("No SingleQuery was returned.");
  return std::any_cast<SingleQuery>(resVisit);;
}

} // NS
