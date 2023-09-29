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
#include "execution/expressions/column_value_expression.h"

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
  index_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetIndexTableOid());
  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());

}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

  Tuple inner_tuple;
  RID inner_rid;

  
  while(true){
    if(!child_executor_->Next(&inner_tuple, &inner_rid)){
      return false;
    }
    std::vector<RID> result;
    std::vector<Value> key_values;
    // Tuple tuple = inner_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    const auto *key_predicate_expr = dynamic_cast<const ColumnValueExpression *>(plan_->KeyPredicate().get());
    Value value = key_predicate_expr->Evaluate(&inner_tuple, child_executor_->GetOutputSchema());
    key_values.emplace_back(value);
    Tuple key_tuple(key_values, index_->GetKeySchema());
    index_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
    if(result.size() > 0){
      Tuple index_tuple;
      index_table_info_->table_->GetTuple(result[0], &index_tuple, exec_ctx_->GetTransaction(), true);

      if(key_predicate_expr->GetTupleIdx() == 0)
        *tuple = Tuple::Join(&inner_tuple, child_executor_->GetOutputSchema(), &index_tuple, plan_->IndexTableSchema(), GetOutputSchema());
      else 
        *tuple = Tuple::Join(&index_tuple, plan_->IndexTableSchema(), &inner_tuple, child_executor_->GetOutputSchema(), GetOutputSchema());
      return true;
    } else if(plan_->GetJoinType() == JoinType::LEFT) {
      if(key_predicate_expr->GetTupleIdx() == 0) {
        *tuple = Tuple::Join(&inner_tuple, child_executor_->GetOutputSchema(), nullptr, plan_->IndexTableSchema(), GetOutputSchema());
        return true;
      }
      
    }
  }
  return true; 
}

 


}  // namespace bustub
