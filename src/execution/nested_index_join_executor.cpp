//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
  child_executor_->Init();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  right_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());

}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

  Tuple left_tuple;
  RID left_rid;

  
  while(true){
    if(!child_executor_->Next(&left_tuple, &left_rid)){
      return false;
    }
    std::vector<RID> result;
    std::vector<Value> key_values;
    // Tuple tuple = left_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    Value value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    key_values.emplace_back(value);
    Tuple key_tuple(key_values, index_->GetKeySchema());
    index_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
    if(result.size() > 0){
      Tuple right_tuple;
      right_table_info_->table_->GetTuple(result[0], &right_tuple, exec_ctx_->GetTransaction(), true);
      *tuple = Tuple::Join(&left_tuple, child_executor_->GetOutputSchema(), &right_tuple, plan_->InnerTableSchema(), GetOutputSchema());
      return true;
    } else if(plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = Tuple::Join(&left_tuple, child_executor_->GetOutputSchema(), nullptr, plan_->InnerTableSchema(), GetOutputSchema());
      return true;
    }
  }
  return true; 
}

 


}  // namespace bustub
