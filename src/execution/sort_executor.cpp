#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() { 
  child_executor_->Init();
  tuples_.clear();

  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)){
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [&](Tuple &tuple1, Tuple &tuple2){
    
    for(auto & order : plan_->GetOrderBy()) {
      OrderByType order_type = order.first;
      AbstractExpressionRef order_exp = order.second;
      Value v1 = order_exp->Evaluate(&tuple1, GetOutputSchema());
      Value v2 = order_exp->Evaluate(&tuple2, GetOutputSchema());

      if(v1.CompareLessThan(v2) == CmpBool::CmpTrue){
        return order_type == OrderByType::ASC ? true : false;
      } 
      else if(v1.CompareGreaterThan(v2) == CmpBool::CmpTrue){
        return order_type == OrderByType::ASC ? false : true;
      }
      else if(v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      } else {
        return false;
      }
    }
    return true;
    
  });


  cursor_ = tuples_.begin();

}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(cursor_ == tuples_.end())
    return false; 

  *tuple = *cursor_;
  ++cursor_;
  return true;
}

}  // namespace bustub
