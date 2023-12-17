//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  return LockTableDirectlyOrNot(txn, lock_mode, oid, true);
}

// directly为true代表直接对表加锁，false代表对行加锁时需要先对表加锁
auto LockManager::LockTableDirectlyOrNot(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool directly) -> bool {
  auto txn_state = txn->GetState();
  auto iso_level = txn->GetIsolationLevel();
  if (txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED) {
    printf("ck1\n");
    return false;
  }

  if (txn_state == TransactionState::GROWING) {
    if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
        printf("ck2\n");
        return false;
      }
    } 
  } else if (txn_state == TransactionState::SHRINKING) {
    if (iso_level == IsolationLevel::REPEATABLE_READ) {
      printf("ck3\n");
      return false;
    } else if (iso_level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        printf("ck4\n");
        return false;
      }
    } else if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      printf("ck5\n");
      return false;
    }
  } 

  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  // 检查此锁请求是否为一次锁升级
  bool upgrade = false;
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      if (lr->lock_mode_ == lock_mode) {
        //duplicated lock
        return true;
      }
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        if (!directly) {
          printf("ck6\n");
          return false;
        }

        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        if (!directly) {
          printf("ck7\n");
          return false;
        }

        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        // 抛 INCOMPATIBLE_UPGRADE 异常
      }

      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(iter);
      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);
      delete lr;
      lrq->request_queue_.push_back(new LockRequest{txn->GetTransactionId(), lock_mode, oid});
      upgrade = true;
      break;
    }
  }

  // 将锁请求加入请求队列
  if (!upgrade) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }
  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    // printf("txn abort id: %d\n", txn->GetTransactionId());
    lrq->cv_.notify_all();
    printf("ck8\n");
    return false;
  }

  AddIntoTxnTableLockSet(txn, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!CheckAllRowsUnlock(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      auto iso_level = txn->GetIsolationLevel();
      if (iso_level == IsolationLevel::REPEATABLE_READ) {
        if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (iso_level == IsolationLevel::READ_COMMITTED) {
        if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
        if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);

      lrq->request_queue_.erase(iter);
      delete lr;
      lrq->cv_.notify_all();

      return true;
    } 
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false;

}

void LockManager::RemoveFromTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}

void LockManager::AddIntoTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    // printf("txn %d is\n", txn->GetTransactionId());
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED ||
           requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } else if (curr_lock_mode == LockMode::SHARED) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } else if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } else if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE;
  }
  return false;
}



auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  auto txn_state = txn->GetState();
  auto iso_level = txn->GetIsolationLevel();
  if (txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED) {
    // printf("err1\n");
    return false;
  }

  if (txn_state == TransactionState::SHRINKING) {
    if (iso_level == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    } else if (iso_level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    } else if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (lock_mode == LockMode::SHARED && !CheckTableOwnLock(txn, LockMode::INTENTION_SHARED, oid) &&
      !CheckTableOwnLock(txn, LockMode::SHARED, oid) &&
      !CheckTableOwnLock(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid)) {
      if (!LockTableDirectlyOrNot(txn, LockMode::INTENTION_SHARED, oid, false) && !LockTableDirectlyOrNot(txn, LockMode::SHARED, oid, false) &&
        !LockTableDirectlyOrNot(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid, false)) {
          return false;
      }
  } else if (lock_mode == LockMode::EXCLUSIVE && !CheckTableOwnLock(txn, LockMode::INTENTION_EXCLUSIVE, oid) &&
        !CheckTableOwnLock(txn, LockMode::EXCLUSIVE, oid) && !CheckTableOwnLock(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid)) {
      if (!LockTableDirectlyOrNot(txn, LockMode::INTENTION_EXCLUSIVE, oid, false) &&
        !LockTableDirectlyOrNot(txn, LockMode::EXCLUSIVE, oid, false) &&
        !LockTableDirectlyOrNot(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid, false)) {
      // printf("err3\n");
      return false;
    }
  } 

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  bool upgrade = false;
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      if (lr->lock_mode_ == lock_mode) {
        // 重复的锁
        return true;
      }
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        // 抛出 UPGRADE_CONFLICT 异常
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        // 抛 INCOMPATIBLE_UPGRADE 异常
      }
      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(iter);
      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);
      delete lr;
      lrq->request_queue_.push_back(new LockRequest{txn->GetTransactionId(), lock_mode, oid});
      upgrade = true;
      break;
    }
  }

  if (!upgrade) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
  }

  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    // printf("txn abort id: %d\n", txn->GetTransactionId());
    lrq->cv_.notify_all();
    // printf("err4\n");
    return false;
  }

  AddIntoTxnRowLockSet(txn, lock_mode, oid, rid);

  return true;


}

auto LockManager::CheckTableOwnLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  auto lrq = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  lrq->latch_.lock();
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId() && lr->lock_mode_ == lock_mode) {
      lrq->latch_.unlock();
      return true;
    }
  }
  lrq->latch_.unlock();
  return false;
}


auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (!force) {
        auto iso_level = txn->GetIsolationLevel();
        if (iso_level == IsolationLevel::REPEATABLE_READ) {
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        } else if (iso_level == IsolationLevel::READ_COMMITTED) {
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        } else if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      ReomoveTxnRowLockSet(txn, lr->lock_mode_, oid, rid);
      lrq->request_queue_.erase(iter);
      delete lr;
      lrq->cv_.notify_all();
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false; 

}

void LockManager::ReomoveTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
}

void LockManager::AddIntoTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode, std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
   if (txn->GetState() == TransactionState::ABORTED) {
    // printf("%d abort\n", txn->GetTransactionId());
    for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
         iter++) {
      auto lr = *iter;
      if (lr->txn_id_ == txn->GetTransactionId()) {
        lock_request_queue->request_queue_.erase(iter);
        delete lr;
        break;
      }
    }

    return true;
  }

  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       iter++) {
    auto lr = *iter;
    if (lr->granted_ && !LockCompatible(lock_mode, lr->lock_mode_)) {
      return false;
    }
  }

  // if transaction is to upgrade lock, it immediately get the lock
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
           iter++) {
        auto lr = *iter;
        if (!lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
          (*iter)->granted_ = true;
          break;
        }
      }
      return true;
    }
    return false;
  }

  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (!lr->granted_) {
      if (lr->txn_id_ == txn->GetTransactionId()) {
        (*iter)->granted_ = true;
        return true;
      }
      lr->granted_ = false;

      if (!LockCompatible(lock_mode, lr->lock_mode_)) {
        return false;
      }
    }
  }

  return true;

}


void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  for (auto &[k, v] : row_lock_map_) {
    for (auto iter = v->request_queue_.begin(); iter != v->request_queue_.end(); iter++) {
      auto lr = *iter;
      /*if (lr->granted_) {
        ReomoveTxnRowLockSet(txn_manager_->GetTransaction(lr->txn_id_), lr->lock_mode_, lr->oid_, lr->rid_);
      }*/
      delete lr;
    }
  }

  for (auto &[k, v] : table_lock_map_) {
    for (auto iter = v->request_queue_.begin(); iter != v->request_queue_.end(); iter++) {
      auto lr = *iter;
      /*if (lr->granted_) {
        RemoveFromTxnTableLockSet(txn_manager_->GetTransaction(lr->txn_id_), lr->lock_mode_, lr->oid_);
      }*/
      delete lr;
    }
  }
  // You probably want to unlock all table and txn locks here.
}

auto LockManager::LockCompatible(LockMode mode1, LockMode mode2) -> bool {
  if (mode1 == LockMode::INTENTION_SHARED) {
    return mode2 == LockMode::INTENTION_SHARED || mode2 == LockMode::INTENTION_EXCLUSIVE || mode2 == LockMode::SHARED ||
           mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } else if (mode1 == LockMode::INTENTION_EXCLUSIVE) {
    return mode2 == LockMode::INTENTION_SHARED || mode2 == LockMode::INTENTION_EXCLUSIVE;
  } else if (mode1 == LockMode::SHARED) {
    return mode2 == LockMode::INTENTION_SHARED || mode2 == LockMode::SHARED;
  } else if (mode1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return mode2 == LockMode::INTENTION_SHARED;
  }

  return false;
}

auto LockManager::CheckAllRowsUnlock(Transaction *txn, const table_oid_t &oid) -> bool {
  row_lock_map_latch_.lock();
  for (auto [k, v] : row_lock_map_) {
    row_lock_map_latch_.unlock();
    v->latch_.lock();
    for (auto iter = v->request_queue_.begin(); iter != v->request_queue_.end(); iter++) {
      if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->oid_ == oid && (*iter)->granted_) {
        v->latch_.unlock();
        row_lock_map_latch_.unlock();
        return false;
      }
    }
    v->latch_.unlock();
    row_lock_map_latch_.lock();
  }

  row_lock_map_latch_.unlock();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
