//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() { 
  child_executor_->Init();

  cursor_ = 0;
  rows_ = 0;
  TableInfo * table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> table_indexes= exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  Tuple tuple;
  RID rid;
  
  while (child_executor_->Next(&tuple, &rid)) {
    bool deleted = table_info->table_->MarkDelete(rid, exec_ctx_->GetTransaction());
    if(deleted){
      for(IndexInfo *index_info : table_indexes){
        BPlusTreeIndexForOneIntegerColumn *index = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
        index->DeleteEntry(tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs()), rid, exec_ctx_->GetTransaction());
      }
      ++rows_;
    }
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(cursor_ < 1){
    std::vector<Value> values{};
    values.reserve(1);
    values.push_back(ValueFactory::GetIntegerValue(rows_));
    *tuple = Tuple{values, &plan_->OutputSchema()};
    ++cursor_;
    return true; 
  }
  return false;
}

}  // namespace bustub
