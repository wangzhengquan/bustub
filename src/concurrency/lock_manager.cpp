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
  bool upgrade = false;
  LockRequest *request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);

  while(!txn->GetState() == TransactionState::ABORTED) {
    std::unique_lock<std::mutex> table_lock_map_lk(table_lock_map_mutex_);
    if (auto search =  table_lock_map_.find(oid); search != table_lock_map_.end()){
      std::shared_ptr<LockRequestQueue> lock_request_queue = search->second;
      std::unique_lock<std::mutex> lock_request_queue_lk(lock_request_queue->mutex_);

      // upgrade
      for(LockRequest *item : lock_request_queue->request_queue_) {
        if(*item == *request){
          upgrade = true;
          int cmp = Compare(item->lock_mode_, lock_mode);
          if(cmp == 0 || cmp == 1) return true;
          //upgrade
          else if(IsCompatible(*request, lock_request_queue->request_queue_)){
            request->lock_mode_ = lock_mode;
            RemoveTableOidFromTransactionLockSet(txn, item->lock_mode_, oid);
            InsertTableOidIntoTransactionLockSet(txn, lock_mode, oid);
            return true;
          } else {
            table_lock_map_lk.unlock();
            lock_request_queue->cv_.wait(lock_request_queue_lk);
            break;
          }
        }
      }
      if(upgrade) {
        continue;
      }

      if(IsCompatible(*request, lock_request_queue->request_queue_)){
        request->granted_ = true;
        lock_request_queue->request_queue_.push_back(request);
        InsertTableOidIntoTransactionLockSet(txn, lock_mode, oid);
        return true;

      } else {
        table_lock_map_lk.unlock();
        lock_request_queue->cv_.wait(lock_request_queue_lk);
      }
    } else {
      std::shared_ptr<LockRequestQueue> lock_request_queue  = std::make_shared<LockRequestQueue>();
      table_lock_map_.insert({oid, lock_request_queue});
      request->granted_ = true;
      lock_request_queue->request_queue_.push_back(request);
      InsertTableOidIntoTransactionLockSet(txn, lock_mode, oid);
      return true;
    }
  }
  
  return false; 
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  std::unique_lock<std::mutex> table_lock_map_lk(table_lock_map_mutex_);
  if (auto search =  table_lock_map_.find(oid); search != table_lock_map_.end()) {
    std::shared_ptr<LockRequestQueue> lock_request_queue = search->second;
    std::unique_lock<std::mutex> lock_request_queue_lk(lock_request_queue->mutex_);

    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();)
    {
      LockRequest *request = *it;
      if(request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid) {
        RemoveTableOidFromTransactionLockSet(txn, request->lock_mode_, oid);
        delete request;
        it = lock_request_queue->request_queue_.erase(it);
        lock_request_queue_lk.unlock();
        lock_request_queue->cv_.notify_all();
        return true;
      }
    }
  } 

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(),  AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  switch(lock_mode) {
    case LockMode::SHARED : 
      if(!LockTable(txn, LockMode::INTENTION_SHARED, oid)){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException (txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
        return false;
      }
      break;
    case LockMode::EXCLUSIVE : 
      if(!LockTable(txn, LockMode::INTENTION_EXCLUSIVE, oid)){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException (txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
        return false;
      }
      break;
    default:
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException (txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  

  bool upgrade = false;
  LockRequest *request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);

  while(!txn->GetState() == TransactionState::ABORTED) {
    std::unique_lock<std::mutex> row_lock_map_lk(row_lock_map_mutex_);
    if (auto search =  row_lock_map_.find(rid); search != row_lock_map_.end()){
      std::shared_ptr<LockRequestQueue> lock_request_queue = search->second;
      std::unique_lock<std::mutex> lock_request_queue_lk(lock_request_queue->mutex_);

      // upgrade
      for(LockRequest *item : lock_request_queue->request_queue_) {
        if(*item == *request){
          upgrade = true;
          int cmp = Compare(item->lock_mode_, lock_mode);
          if(cmp == 0 || cmp == 1) return true;
          else if(IsCompatible(*request, lock_request_queue->request_queue_)){
            request->lock_mode_ = lock_mode;
            RemoveRIDFromTransactionLockSet(txn, item->lock_mode_, oid, rid);
            InsertRIDIntoTransactionLockSet(txn, lock_mode, oid, rid);
            return true;
          } else {
            row_lock_map_lk.unlock();
            lock_request_queue->cv_.wait(lock_request_queue_lk);
            break;
          }
        }
      }
      if(upgrade) {
        continue;
      }

      if(IsCompatible(*request, lock_request_queue->request_queue_)){
        request->granted_ = true;
        lock_request_queue->request_queue_.push_back(request);
        InsertRIDIntoTransactionLockSet(txn, lock_mode, oid, rid);
        return true;

      } else {
        row_lock_map_lk.unlock();
        lock_request_queue->cv_.wait(lock_request_queue_lk);
      }
    } else {
      std::shared_ptr<LockRequestQueue> lock_request_queue  = std::make_shared<LockRequestQueue>();
      row_lock_map_.insert({rid, lock_request_queue});
      request->granted_ = true;
      lock_request_queue->request_queue_.push_back(request);
      InsertRIDIntoTransactionLockSet(txn, lock_mode, oid, rid);
      return true;
    }
  }
  
  return false; 
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  std::unique_lock<std::mutex> row_lock_map_lk(row_lock_map_mutex_);
  if (auto search =  row_lock_map_.find(rid); search != row_lock_map_.end()) {
    std::shared_ptr<LockRequestQueue> lock_request_queue = search->second;
    std::unique_lock<std::mutex> lock_request_queue_lk(lock_request_queue->mutex_);

    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();)
    {
      LockRequest *request = *it;
      if(request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == rid) {
        RemoveRIDFromTransactionLockSet(txn, request->lock_mode_, oid, rid);
        delete request;
        it = lock_request_queue->request_queue_.erase(it);
        lock_request_queue_lk.unlock();
        lock_request_queue->cv_.notify_all();
        return true;
      }
    }
  } 

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(),  AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
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


// ============== privte ================
void LockManager::InsertRIDIntoTransactionLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,  const RID &rid){
  switch(lock_mode) {
    case LockMode::SHARED : 
      (*txn->GetSharedRowLockSet())[oid].insert(rid);
      txn->GetSharedLockSet()->insert(rid);
      break;
    case LockMode::EXCLUSIVE : 
      (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      txn->GetExclusiveLockSet()->insert(rid);
      break;
    default:
      break;
  }
}

void LockManager::RemoveRIDFromTransactionLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,  const RID &rid){
  switch(lock_mode) {
    case LockMode::SHARED : 
      (*txn->GetSharedRowLockSet())[oid].erase(rid);
      txn->GetSharedLockSet()->erase(rid);
      break;
    case LockMode::EXCLUSIVE : 
      (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      break;
    default:
      break;
  }
}

void LockManager::InsertTableOidIntoTransactionLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid){
  switch(lock_mode) {
    case LockMode::INTENTION_SHARED : 
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE : 
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED : 
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE : 
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE : 
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    default:
      break;
  }
}

void LockManager::RemoveTableOidFromTransactionLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid){
  switch(lock_mode) {
    case LockMode::INTENTION_SHARED : 
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE : 
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED : 
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE : 
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE : 
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    default:
      break;
  }
}

auto LockManager::IsCompatible(const LockRequest &request, const std::list<LockRequest *>  &request_queue) ->  bool
{
  for(LockRequest *item : request_queue){
    if(request.txn_id_ != item->txn_id_ && !IsCompatible(request.lock_mode_, item->lock_mode_)){
      return false;
    } 
  }
  return true;
  
}

auto LockManager::Compare(const LockMode &a, const LockMode &b) ->  int {
  switch(a)  {
    case LockMode::INTENTION_SHARED : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return 0;
        case LockMode::INTENTION_EXCLUSIVE : 
          return -1;
        case LockMode::SHARED : 
          return -1;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return -1;
        case LockMode::EXCLUSIVE : 
          return -1;
        default: break;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return 1;
        case LockMode::INTENTION_EXCLUSIVE : 
          return 0;
        case LockMode::SHARED : 
          return 2;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return -1;
        case LockMode::EXCLUSIVE : 
          return -1;
        default: break;
      }
      break;
    case LockMode::SHARED : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return 1;
        case LockMode::INTENTION_EXCLUSIVE : 
          return 2;
        case LockMode::SHARED : 
          return 0;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return -1;
        case LockMode::EXCLUSIVE : 
          return -1;
        default: break;
          
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return 1;
        case LockMode::INTENTION_EXCLUSIVE : 
          return 1;
        case LockMode::SHARED : 
          return 1;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return 0;
        case LockMode::EXCLUSIVE : 
          return -1;
        default: break;
      }
      break;
    case LockMode::EXCLUSIVE : 
      switch(b) {
        case LockMode::EXCLUSIVE : 
          return 0;
        default:
          return 1;
      }
      break;
  }
  return 2;
}

auto LockManager::IsCompatible(const LockMode &a, const LockMode &b) ->  bool {
  switch(a)  {
    case LockMode::INTENTION_SHARED : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return true;
        case LockMode::INTENTION_EXCLUSIVE : 
          return true;
        case LockMode::SHARED : 
          return true;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return true;
        case LockMode::EXCLUSIVE : 
          return false;
        default: break;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return true;
        case LockMode::INTENTION_EXCLUSIVE : 
          return true;
        case LockMode::SHARED : 
          return false;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return false;
        case LockMode::EXCLUSIVE : 
          return false;
        default: break;
      }
      break;
    case LockMode::SHARED : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return true;
        case LockMode::INTENTION_EXCLUSIVE : 
          return false;
        case LockMode::SHARED : 
          return true;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return false;
        case LockMode::EXCLUSIVE : 
          return false;
        default: break;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE : 
      switch(b) {
        case LockMode::INTENTION_SHARED : 
          return true;
        case LockMode::INTENTION_EXCLUSIVE : 
          return false;
        case LockMode::SHARED : 
          return false;
        case LockMode::SHARED_INTENTION_EXCLUSIVE : 
          return false;
        case LockMode::EXCLUSIVE : 
          return false;
        default: break;
      }
      break;
    case LockMode::EXCLUSIVE : 
      return false;
      break;
  }
  return false;
}

}  // namespace bustub
