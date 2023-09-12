#include "execution/executors/topn_executor.h"

namespace bustub {




TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
    priority_queue(OrderByCompare(plan_)) {}

void TopNExecutor::Init() { 
  child_executor_->Init();
  tuples_.clear();

  OrderByCompare compare(plan_);
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)){
    if(priority_queue.size() < plan_->GetN())
      priority_queue.push(tuple);
    else if(compare(tuple, priority_queue.top())){
      priority_queue.pop();
      priority_queue.push(tuple);
    }
  }

  while(!priority_queue.empty()){
    tuples_.push_front(priority_queue.top());
    priority_queue.pop();
  }

  // std::reverse(tuples_.begin(), tuples_.end());

  cursor_ = tuples_.begin();
  
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(cursor_ == tuples_.end())
    return false; 

  *tuple = *cursor_;
  ++cursor_;
  return true;
}

}  // namespace bustub
