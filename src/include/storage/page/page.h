//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page.h
//
// Identification: src/include/storage/page/page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <iostream>
#include <list>

#include "common/config.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
 */
class Page {
  // There is book-keeping information inside the page that should only be relevant to the buffer pool manager.
  friend class BufferPoolManagerInstance;
  friend class LRUKReplacer;
 

 public:
  enum class State{NORMAL=0, WAITTING_TO_DELETE, DELETED};
  /** Constructor. Zeros out the page data. */
  Page() { ResetMemory(); }

  /** Default destructor. */
  ~Page() = default;

  /** @return the actual data contained within this page */
  inline auto GetData() -> char * { return data_; }

  /** @return the page id of this page */
  inline auto GetPageId() -> page_id_t { return page_id_; }

  /** @return the pin count of this page */
  inline auto GetPinCount() -> int { return pin_count_; }

  /** @return true if the page in memory has been modified from the page on disk, false otherwise */
  inline auto IsDirty() -> bool { return is_dirty_; }

  /** Acquire the page write latch. */
  inline void WLatch() { rwlatch_.WLock(); }

  /** Release the page write latch. */
  inline void WUnlatch() { rwlatch_.WUnlock(); }

  /** Acquire the page read latch. */
  inline void RLatch() { rwlatch_.RLock(); }

  /** Release the page read latch. */
  inline void RUnlatch() { rwlatch_.RUnlock(); }

  /** @return the page LSN. */
  inline auto GetLSN() -> lsn_t { return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN); }

  /** Sets the page LSN. */
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }

  void Print()  {
    std::cout << "page_id: " <<  page_id_ << ", pin_count: " << pin_count_ << std::endl;
  }

 protected:
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  static constexpr size_t SIZE_PAGE_HEADER = 8;
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN = 4;

 private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, BUSTUB_PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  char data_[BUSTUB_PAGE_SIZE]{};
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page. */
  int pin_count_ = 0;
  /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
  bool is_dirty_ = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;

// ================= lru_k_replacer ======================
private:
  std::list<size_t> access_histories_{};
  size_t k_ = 1;
  State state_ = State::NORMAL;
  std::shared_mutex frame_mutex_;

public:
  void RecordAccess(size_t timestamp) {
    pin_count_++;
    access_histories_.push_back(timestamp);
    if (access_histories_.size() > k_) {
      access_histories_.pop_front();
    }
  }

  //void SetEvictable(bool evictable) { evictable_ = evictable; }

  auto Evictable() -> bool { return pin_count_ <= 0; }

  // auto AccessHistories() -> std::list<size_t> { return access_histories_; }

  auto KDistance(size_t current_timestamp) -> size_t {
    if (access_histories_.size() < k_) return INT64_MAX;
    return current_timestamp - access_histories_.front();
  }

  auto Distance(size_t current_timestamp) -> size_t {
    if (access_histories_.empty()) return INT64_MAX;
    return current_timestamp - access_histories_.front();
  }
  
  void ClearAccessHistory(){
    access_histories_.clear();
  }

  void SetDirty(bool dirty){
    is_dirty_ = dirty;
  }

  auto IsRemoved() -> bool{
    return page_id_ == INVALID_PAGE_ID;;
  }

  auto Remove() -> bool{
    BUSTUB_ASSERT(Evictable(), "Remove a Un Evictable frame.");
    if(IsRemoved()) 
      return true;
    page_id_ = INVALID_PAGE_ID;
    state_ = State::NORMAL;
    return true;
  }
  
  
};

}  // namespace bustub
