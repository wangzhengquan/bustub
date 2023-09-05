//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
  child_executor_->Init();
  cursor_ = 0;
  rows_ = 0;
  
  TableInfo * table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  Tuple tuple;
  RID rid;
  
  while (child_executor_->Next(&tuple, &rid)) {
    bool inserted = table_info_->table_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction());
    if(inserted){
      ++rows_;
    }
  }
  
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
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
