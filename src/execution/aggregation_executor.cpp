//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
    aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  aht_.Clear();
  
  while (child_->Next(&tuple, &rid)) {
     AggregateKey key = MakeAggregateKey(&tuple);
     AggregateValue value = MakeAggregateValue(&tuple);
     aht_.InsertCombine(key, value);
  }
  if(aht_.GetHashTable().size() == 0 &&  plan_->GetGroupBys().size() == 0){
    aht_.GetHashTable().insert({AggregateKey(), aht_.GenerateInitialAggregateValue()});
  }

  cursor_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(cursor_ == aht_.End())
    return false; 
  // std::cout << "cursor_.Key().group_bys_.size()  = " << cursor_.Key().group_bys_.size() << std::endl;
  // std::cout << "cursor_.Val().aggregates_.size()" << cursor_.Val().aggregates_.size() << std::endl;
  std::vector<Value> values;
  values.reserve(cursor_.Key().group_bys_.size() + cursor_.Val().aggregates_.size());
  values.insert(values.end(), cursor_.Key().group_bys_.cbegin(), cursor_.Key().group_bys_.cend());
  values.insert(values.end(), cursor_.Val().aggregates_.cbegin(), cursor_.Val().aggregates_.cend());
  *tuple = {values, &plan_->OutputSchema()};
  ++cursor_;
  return true; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
