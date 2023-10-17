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
#include "type/value_factory.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
        plan_ = plan;
    }

void IndexScanExecutor::Init() {
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    
    auto b_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
    iter_ = b_tree_index->GetBeginIterator();
    end_ = b_tree_index->GetEndIterator();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (iter_ == end_) {
        return false;
    }

    while (iter_ != end_) {
        auto tuplepair = table_info_->table_->GetTuple((*iter_).second);
        *tuple = tuplepair.second;
        *rid = tuple->GetRid();
        ++iter_;

        return true;
    } 

    return false;

}
    
}  // namespace bustub
