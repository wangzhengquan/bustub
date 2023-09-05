//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
 // throw NotImplementedException("SeqScanExecutor is not implemented"); 

 cursor_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

	if(cursor_ == exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End())
		return false; 
	*tuple = *cursor_;
	*rid = tuple->GetRid();
	++cursor_;	
	return true;

}

}  // namespace bustub
