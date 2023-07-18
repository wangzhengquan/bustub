//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  std::cout << "free_list_ front = " << free_list_.front() << std::endl;

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  // delete replacer_;
}


auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;

  free_list_latch_.WLock();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
 // std::cout << "NewPgImp pop frame_id=" << frame_id << std::endl;
    free_list_.pop_front();
    free_list_latch_.WUnlock();
  } else {
    free_list_latch_.WUnlock();

    // evict
    frame_id = Evict();
    if(frame_id == -1){
      LOG_WARN("no avalide frame");
      Print();
      return nullptr;
    }
  }
  page_id_t new_page_id = AllocatePage();
// std::cout << "new_page_id=" << new_page_id  << ", frame_id=" << frame_id << std::endl;
  Page &page = pages_[frame_id];
  page.WLatch();
  page.is_dirty_ = true;
  page.ResetMemory();
  page.page_id_ = new_page_id;
  page.state_ = Page::State::NORMAL;
  page.RecordAccess(++current_timestamp_);
  page_table_->Insert(new_page_id, frame_id);

  if (page_id != nullptr) {
    *page_id = new_page_id;
  }
  page.WUnlatch();
  return &page;
}

/**
 * 1. 同时evict
 * 2. evict and cache hit ocurce simultaneously.
*/
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;

  pages_latch_.RLock();
  if (page_table_->Find(page_id, frame_id)) {
    Page &page = pages_[frame_id];
// std::cout << "FetchPgImp hit frame_id=" << frame_id << ", page_id=" << page.GetPageId()  << std::endl;    
    page.WLatch();
    page.RecordAccess(++current_timestamp_);
    pages_latch_.RUnlock();
    page.WUnlatch();
    return &page;
  }
  pages_latch_.RUnlock();

  free_list_latch_.WLock();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
 // std::cout << "FetchPgImp pop frame_id=" << frame_id << std::endl;
    free_list_.pop_front();
    free_list_latch_.WUnlock();
  } else {
    free_list_latch_.WUnlock();

    // evict
    frame_id = Evict();
    if(frame_id == -1){
      LOG_WARN("no avalide frame") ;  
      Print();
      return nullptr;
    }
  }

  Page &page = pages_[frame_id];
  page.WLatch();
  page.ResetMemory();
  page.page_id_ = page_id;
  page.state_ = Page::State::NORMAL;
  page.RecordAccess(++current_timestamp_);
  disk_manager_->ReadPage(page_id, page.GetData());
  page.is_dirty_ = false;
  page_table_->Insert(page_id, frame_id);
  page.WUnlatch();

  return &page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if(page_id == INVALID_PAGE_ID) {
    return false;
  }
    
  pages_latch_.RLock();
  if (!page_table_->Find(page_id, frame_id)) {
    pages_latch_.RUnlock();
    return false;
  }

  Page &page = pages_[frame_id];
  page.WLatch();
  page.pin_count_--;
  if (page.pin_count_ < 0) {
    LOG_WARN("\n page.pin_count_ < 0 , pageid = %d, pin_count = %d", page.GetPageId(),  page.pin_count_ );
    // BUSTUB_ASSERT(false, "page.pin_count_ < 0");
    pages_latch_.RUnlock();
    page.WUnlatch();
    return false;
  } else if (page.pin_count_ == 0 && page.state_ ==  Page::State::WAITTING_TO_DELETE) {
    // remove page
    page_table_->Remove(page_id);
    page.Remove();
    pages_latch_.RUnlock();

    page.ResetMemory();
    free_list_latch_.WLock();
    free_list_.push_back(frame_id);
    free_list_latch_.WUnlock();
    DeallocatePage(page_id);
    page.WUnlatch();
  } else {
    pages_latch_.RUnlock();

    if(is_dirty){
      page.is_dirty_ = is_dirty;
    }
    page.WUnlatch();
  }
  
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id cannot be INVALID_PAGE_ID)");
  frame_id_t frame_id;
  pages_latch_.RLock();
  if (!page_table_->Find(page_id, frame_id)) {
    pages_latch_.RUnlock();
    return false;
  }

  Page &page = pages_[frame_id];
  page.WLatch();
  pages_latch_.RUnlock();
  if(!page.Removed()){
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.is_dirty_ = false;
  }
  page.WUnlatch();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    Page &page = pages_[i];
    page.WLatch();
    if (page.page_id_ == INVALID_PAGE_ID) {
      page.WUnlatch();
      continue;
    }
    if(!page.Removed()){
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
      page.is_dirty_ = false;
    }
    
    page.WUnlatch();
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  pages_latch_.WLock();
  if (!page_table_->Find(page_id, frame_id)) {
    pages_latch_.WUnlock();
    return true;
  }
  Page &page = pages_[frame_id];
  page.WLatch();
  if (!page.Evictable() ) {
    page.state_ = Page::State::WAITTING_TO_DELETE;
    // LOG_WARN("Delete a currently active page %d", page.page_id_);
    page.WUnlatch();
    pages_latch_.WUnlock();
    return false;
  }
  page_table_->Remove(page_id);
  page.Remove();
  pages_latch_.WUnlock();
  
  page.ResetMemory();
  free_list_latch_.WLock();
  free_list_.push_back(frame_id);
  free_list_latch_.WUnlock();
  DeallocatePage(page_id);
  page.WUnlatch();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::Evict() -> frame_id_t{
  frame_id_t frame_id;
  pages_latch_.WLock();
  // BUSTUB_ASSERT(Victim(&frame_id), "FetchPgImp, no available frame \n ");
  if(!Victim(&frame_id)){
    pages_latch_.WUnlock();
    return -1;
  }
  Page &old_page = pages_[frame_id];
  old_page.WLatch();
  if (old_page.IsDirty()) {
    disk_manager_->WritePage(old_page.GetPageId(), old_page.GetData());
    old_page.is_dirty_ = false;
  }
  page_table_->Remove(old_page.page_id_);
  old_page.Remove();
  old_page.WUnlatch();
  pages_latch_.WUnlock();
  return frame_id;
}

auto BufferPoolManagerInstance::Victim(frame_id_t *frame_id) -> bool {
  frame_id_t max_k_dist_frame_id = -1;
  size_t max_k_dist = 0;
  
  // if (evictable_size_ == 0) return false;
  for (size_t i = 0; i < pool_size_; i++) {
    Page& frame = pages_[i];
    frame.RLatch();
    if(frame.Removed() || !frame.Evictable()){
      frame.RUnlatch();
      continue;
    }
    size_t k_dist = frame.KDistance(current_timestamp_);
    if(max_k_dist_frame_id == -1){
      max_k_dist = k_dist;
      max_k_dist_frame_id = i;
    }
    else if (k_dist > max_k_dist) {
      // frames_[max_k_dist_frame_id].RUnlatch();
      max_k_dist = k_dist;
      max_k_dist_frame_id = i;
    } else if (k_dist == max_k_dist) {
      if (frame.Distance(current_timestamp_) > pages_[max_k_dist_frame_id].Distance(current_timestamp_)) {
        // frames_[max_k_dist_frame_id].RUnlatch();
        max_k_dist_frame_id = i;
      }
    } else {
      // frame.RUnlatch();
    }
    frame.RUnlatch();
  }

  if (max_k_dist_frame_id == -1) {
    return false;
  }
  *frame_id = max_k_dist_frame_id;
  return true;
}

void BufferPoolManagerInstance::Print()  {
  pages_latch_.RLock();
  std::cout << "----------BufferPoolManager-----------" << std::endl;
  for (size_t i = 0; i < pool_size_; ++i) {
    Page& frame = pages_[i];
    frame.RLatch();
    std::cout << "frame_id: " << i << ", page_id: " <<  frame.page_id_ << ", pin_count: " << frame.pin_count_ << std::endl;
    frame.RUnlatch();
  }
  std::cout << "--------------------------------------" << std::endl;
  pages_latch_.RUnlock();
}


}  // namespace bustub

