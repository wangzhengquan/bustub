#include "optimizer/optimizer.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // std::cout << "optimized_plan->GetType():" << optimized_plan->GetTypeString() << std::endl;

  // Has exactly one child
//  BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Limit with multiple children?? Impossible!");
  if(optimized_plan->children_.size() == 1){
    const AbstractPlanNodeRef &child_plan = optimized_plan->children_[0];

    if (optimized_plan->GetType() == PlanType::Limit && child_plan->GetType() == PlanType::Sort) {
      const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);

      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(),
                 sort_plan.GetOrderBy(), limit_plan.GetLimit());
        
    }
  }
  

  return optimized_plan;
}

}  // namespace bustub
