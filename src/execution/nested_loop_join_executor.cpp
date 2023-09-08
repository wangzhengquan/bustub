//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), 
      left_executor_(std::move(left_executor)), 
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();

  exist_match_ = false;
  left_found_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(!left_found_)
    return false; 

  while(true) {
    while(!right_executor_->Next(&right_tuple_, &right_rid_)){
      
      if( !exist_match_ ) {
        if(plan_->GetJoinType() == JoinType::LEFT) {
          *tuple = Tuple::Join(&left_tuple_, left_executor_->GetOutputSchema(), nullptr, right_executor_->GetOutputSchema(), GetOutputSchema());

          left_found_ = left_executor_->Next(&left_tuple_, &left_rid_);
          right_executor_->Init(); 
          exist_match_ = false;
          return true;
        }
      }
      
      left_found_ = left_executor_->Next(&left_tuple_, &left_rid_);
      if(!left_found_)
        return false;
      
      right_executor_->Init(); 
      exist_match_ = false;
    }

    Value value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_, right_executor_->GetOutputSchema());
    // value.IsNull() || !value.GetAs<bool>()
    if(!value.IsNull() && value.GetAs<bool>()){
      *tuple = Tuple::Join(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_, right_executor_->GetOutputSchema(), GetOutputSchema());
      exist_match_ = true;
      return true;
    } 

  } 
 
  return true;

}

 



}  // namespace bustub
