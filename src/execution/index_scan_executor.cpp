//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"


namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  cursor_ = index_->GetBeginIterator();

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(cursor_ == index_->GetEndIterator()){
    return false;
  }
  *rid = cursor_->second;
  table_info_->table_->GetTuple(cursor_->second, tuple, exec_ctx_->GetTransaction(), true); 
  ++cursor_;
  return true;
}

}  // namespace bustub
