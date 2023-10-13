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
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    plan_ = plan;
}

void SeqScanExecutor::Init() {
    table_oid_ = plan_->GetTableOid();
    auto catalog = exec_ctx_->GetCatalog();

    auto table_info = catalog->GetTable(table_oid_);
    auto &table = table_info->table_;
    // iter_ = new TableIterator(table->MakeIterator());
    iter_ = new TableIterator(table->MakeEagerIterator());

}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (true) {
        if (iter_->IsEnd()) {
            delete iter_;
            iter_ = nullptr;
            return false;
        }
        *tuple = iter_->GetTuple().second;

        if (iter_->GetTuple().first.is_deleted_ || 
            (plan_->filter_predicate_ != nullptr && 
            plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(table_oid_)->schema_)
                .CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue)) {
                    ++(*iter_);
                    continue;
        } else {
            //transaction
        }

        *tuple = iter_->GetTuple().second;
        *rid = tuple->GetRid();
        break;
    }
    ++(*iter_);

    return true;
}

}  // namespace bustub
