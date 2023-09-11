//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"
#include <queue>

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  class OrderByCompare {
  public:
    const TopNPlanNode *plan_;
    
    explicit OrderByCompare(const TopNPlanNode *plan) : 
             plan_(plan) {};

    bool operator() (const Tuple &tuple1, const Tuple &tuple2) const { 
      for(auto & order : plan_->GetOrderBy()) {
        OrderByType order_type = order.first;
        AbstractExpressionRef order_exp = order.second;
        Value v1 = order_exp->Evaluate(&tuple1, plan_->OutputSchema());
        Value v2 = order_exp->Evaluate(&tuple2, plan_->OutputSchema());

        if(v1.CompareLessThan(v2) == CmpBool::CmpTrue){
          return order_type == OrderByType::DESC ? false : true; 
        } 
        else if(v1.CompareGreaterThan(v2) == CmpBool::CmpTrue){
          return order_type == OrderByType::DESC ? true : false;
        }
        else if(v1.CompareEquals(v2) == CmpBool::CmpTrue) {
          continue;
        } else {
          return false;
        }
      }
      return false;
    }
  } ;

  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::priority_queue<Tuple, std::vector<Tuple>, OrderByCompare> priority_queue;
  std::vector<Tuple> tuples_;
  std::vector<Tuple>::iterator cursor_;

};

}  // namespace bustub
