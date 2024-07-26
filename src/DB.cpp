#include "DB.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

NodeTypeIdx DB::addNodeType(const std::string& type)
{
  const auto typeIdx = nodeTypes.getIndexOfType(type);
  if(nodesTables.size() <= typeIdx.unsafeGet())
    nodesTables.resize(typeIdx.unsafeGet() + 1ull);
  return typeIdx;
}
RelationshipTypeIdx DB::addRelationshipType(const std::string& type)
{
  const auto typeIdx = relationshipTypes.getIndexOfType(type);
  if(relationshipsTables.size() <= typeIdx.unsafeGet())
    relationshipsTables.resize(typeIdx.unsafeGet() + 1ull);
  return typeIdx;
}

ID DB::addNode(const std::string& type)
{
  const auto nodeTypeIdx = addNodeType(type);
  const auto id = ID{nodeIdToKey.size()};
  const auto rowIdx = nodesTables[nodeTypeIdx.unsafeGet()].addElement(id);
  nodeIdToKey.push_back(NodeKey{nodeTypeIdx, rowIdx});
  return id;
}

RowIdx Table::addElement(const ID id)
{
  RowIdx const rowIdx{ids.size()};
  ids.push_back(id);
  return rowIdx;
}

bool DB::setNodeProperty(const ID nodeId, std::string const& name, const double value)
{
  if(nodeIdToKey.size() <= nodeId.unsafeGet())
    return false;
  const auto & nodeKey = nodeIdToKey[nodeId.unsafeGet()];
  nodesTables[nodeKey.nodeTypeIdx.unsafeGet()].setElementProperty(nodeKey.rowIdx, name, value);
  return true;
}

void Table::setElementProperty(const RowIdx rowIdx, const std::string &name, const double value)
{
  const auto propertyIdx = propertyNames.getIndexOfType(name);
  if(columns.size() <= propertyIdx.unsafeGet())
    columns.resize(propertyIdx.unsafeGet() + 1ull);
  auto & values = columns[propertyIdx.unsafeGet()].values;
  if(values.size() <= rowIdx.unsafeGet())
    values.resize(rowIdx.unsafeGet() + 1ull);
  values[rowIdx.unsafeGet()] = value;
}

arrow::Status DB::writeToDisk()
{
  {
    constexpr auto c_nodeIdToKey_prefix = "nodeIdToKey";

    arrow::UInt32Builder ui32NodeTypeIdxBuilder;
    arrow::UInt64Builder ui64RowIdxBuilder;

    ARROW_RETURN_NOT_OK(ui32NodeTypeIdxBuilder.Reserve(nodeIdToKey.size()));
    ARROW_RETURN_NOT_OK(ui64RowIdxBuilder.Reserve(nodeIdToKey.size()));

    for(const auto & v : nodeIdToKey)
    {
      ARROW_RETURN_NOT_OK(ui32NodeTypeIdxBuilder.Append(v.nodeTypeIdx.unsafeGet()));
      ARROW_RETURN_NOT_OK(ui64RowIdxBuilder.Append(v.rowIdx.unsafeGet()));
    }
    std::shared_ptr<arrow::Array> nodeTypeIndices;
    std::shared_ptr<arrow::Array> rowsIndices;
    ARROW_ASSIGN_OR_RAISE(nodeTypeIndices, ui32NodeTypeIdxBuilder.Finish());
    ARROW_ASSIGN_OR_RAISE(rowsIndices, ui64RowIdxBuilder.Finish());
    
    std::vector<std::shared_ptr<arrow::Array>> columns = {nodeTypeIndices, rowsIndices};

    std::shared_ptr<arrow::Field> field_nodeTypeIndex, field_rowIndex;
    std::shared_ptr<arrow::Schema> schema;
    
    field_nodeTypeIndex = arrow::field("NodeTypeIndex", arrow::uint32());
    field_rowIndex = arrow::field("RowIndex", arrow::uint64());

    schema = arrow::schema({field_nodeTypeIndex, field_rowIndex});
    
    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, columns);
    ARROW_RETURN_NOT_OK(writeTable(c_nodeIdToKey_prefix, *table));
  }

  return arrow::Status::OK();
}

arrow::Status DB::writeTable(std::string const& prefixName, const arrow::Table& table)
{
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(prefixName + "_in.arrow"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, table.schema()));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(table));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());
  
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(prefixName + "_in.csv"));
  ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                        arrow::csv::MakeCSVWriter(outfile, table.schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(table));
  ARROW_RETURN_NOT_OK(csv_writer->Close());
  
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(prefixName + "_in.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 5));
  
  return arrow::Status::OK();
}
