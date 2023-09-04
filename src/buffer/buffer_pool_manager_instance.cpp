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

#include <set>
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
    pages_[i].k_ = replacer_k;
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }

}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
}


auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;

  Page *page;
  std::unique_lock lock(mutex_);
  // free_list_latch_.WLock();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
 // std::cout << "NewPgImp pop frame_id=" << frame_id << std::endl;
    free_list_.pop_front();
    page = &pages_[frame_id];
    page->frame_mutex_.lock();
    
    // page->WLatch();
    // free_list_latch_.WUnlock();
  } else {
    // free_list_latch_.WUnlock();
    // evict
    frame_id = Victim();
    if(frame_id == -1){
      LOG_WARN("no avalide frame") ;  
      Print();
      return nullptr;
    }
    page = &pages_[frame_id];
    page->frame_mutex_.lock();
   
    if (page->IsDirty()) {
      page->RLatch();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->RUnlatch();
      page->is_dirty_ = false;
    }
    page_table_->Remove(page->page_id_);
    page->Remove();
  }
  page_id_t new_page_id = AllocatePage();
  page_table_->Insert(new_page_id, frame_id);
  lock.unlock();

  page->pin_count_ = 0;
  page->ClearAccessHistory();

  
// std::cout << "new_page_id=" << new_page_id  << ", frame_id=" << frame_id << std::endl;
  page->is_dirty_ = true;
  page->ResetMemory();
  page->RecordAccess(++current_timestamp_);
  page->page_id_ = new_page_id;
  page->state_ = Page::State::NORMAL;

  if (page_id != nullptr) {
    *page_id = new_page_id;
  }
  page->frame_mutex_.unlock();
  return page;
}

/**
 * 需要考虑的情况
 * 1. 同时evict
 * 2. evict and cache hit ocurce simultaneously.
*/
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  Page *page;
  std::unique_lock lock(mutex_);
  if (page_table_->Find(page_id, frame_id)) {
    page = &pages_[frame_id];
// std::cout << "FetchPgImp hit frame_id=" << frame_id << ", page_id=" << page.GetPageId()  << std::endl;    
    page->frame_mutex_.lock();
    page->RecordAccess(++current_timestamp_);
    page->frame_mutex_.unlock();
    return page;
  }
  

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
    page->frame_mutex_.lock();
  } else {

    // evict
    frame_id = Victim();
    if(frame_id == -1){
      LOG_WARN("no avalide frame") ;  
      Print();
      return nullptr;
    }
    page = &pages_[frame_id];
    page->frame_mutex_.lock();
    
    if (page->IsDirty()) {
      page->RLatch();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->RUnlatch();
      page->is_dirty_ = false;
    }
    page_table_->Remove(page->page_id_);
    page->Remove();
    
  }
  
  page_table_->Insert(page_id, frame_id);
  lock.unlock();
  
  page->pin_count_ = 0;
  page->ClearAccessHistory();

  page->RecordAccess(++current_timestamp_);
  page->ResetMemory();
  page->page_id_ = page_id;
  page->state_ = Page::State::NORMAL;
  
  disk_manager_->ReadPage(page_id, page->GetData());
  page->is_dirty_ = false;

  page->frame_mutex_.unlock();

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if(page_id == INVALID_PAGE_ID) {
    return false;
  }
    
  std::unique_lock lock(mutex_);
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  // page.WLatch();
  std::unique_lock frame_lock(page.frame_mutex_);
  page.pin_count_--;
  if (page.pin_count_ < 0) {
    LOG_WARN("\n page.pin_count_ < 0 , pageid = %d, pin_count = %d", page.GetPageId(),  page.pin_count_ );
    // BUSTUB_ASSERT(false, "page.pin_count_ < 0");
    return false;
  } else if (page.pin_count_ == 0 && page.state_ ==  Page::State::WAITTING_TO_DELETE) {
    // delete page
    // std::cout << "delete page " << page_id <<  std::endl;
    page_table_->Remove(page_id);
    page.Remove();
    free_list_.push_back(frame_id);
    lock.unlock();

    page.state_ = Page::State::NORMAL;
    page.ClearAccessHistory();
    page.WLatch();
    page.ResetMemory();
    page.WUnlatch();
    DeallocatePage(page_id);
    
  } else {

    if(is_dirty){
      page.is_dirty_ = is_dirty;
    }
  }
  
  return true;
}


auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  std::unique_lock lock(mutex_);
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page &page = pages_[frame_id];
  std::unique_lock frame_lock(page.frame_mutex_);
  if (!page.Evictable() ) {
    page.state_ = Page::State::WAITTING_TO_DELETE;
    // LOG_WARN("Delete a currently active page %d", page.page_id_);
    return false;
  }
  page_table_->Remove(page_id);
  page.Remove();
  free_list_.push_back(frame_id);
  lock.unlock();
  
  // page.pin_count_ = 0;
  page.is_dirty_ = false;
  page.ClearAccessHistory();
  page.WLatch();
  page.ResetMemory();
  page.WUnlatch();
  DeallocatePage(page_id);
 
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id cannot be INVALID_PAGE_ID)");
  frame_id_t frame_id;
  std::unique_lock lock(mutex_);
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  std::unique_lock frame_lock(page.frame_mutex_);
  lock.unlock();
  if(!page.IsRemoved()){
    page.RLatch();
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.RUnlatch();
    page.is_dirty_ = false;
  }
  
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    Page &page = pages_[i];
    std::unique_lock frame_lock(page.frame_mutex_);
    if (page.IsRemoved()) {
      
      continue;
    }
    page.RLatch();
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.RUnlatch();
    page.is_dirty_ = false;
    
  }
}


auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { 
  return next_page_id_++; 
}

 

auto BufferPoolManagerInstance::Victim() -> frame_id_t {
  frame_id_t max_k_dist_frame_id = -1;
  size_t max_k_dist = 0;
  
  // if (evictable_size_ == 0) return false;
  for (size_t i = 0; i < pool_size_; i++) {
    Page& frame = pages_[i];
    std::shared_lock frame_lock(frame.frame_mutex_);
    if(frame.IsRemoved() || !frame.Evictable()){
      // frame.RUnlatch();
      frame_lock.unlock();
      continue;
    }
    size_t k_dist = frame.KDistance(current_timestamp_);
    if(max_k_dist_frame_id == -1){
      max_k_dist = k_dist;
      max_k_dist_frame_id = i;
    }
    else if (k_dist > max_k_dist) {
      max_k_dist = k_dist;
      max_k_dist_frame_id = i;
    } else if (k_dist == max_k_dist) {
      if (frame.Distance(current_timestamp_) > pages_[max_k_dist_frame_id].Distance(current_timestamp_)) {
        // frames_[max_k_dist_frame_id].RUnlatch();
        max_k_dist_frame_id = i;
      }
    } else {
    }
    frame_lock.unlock();
    // frame.RUnlatch();
  }

   
  return max_k_dist_frame_id;
}

void BufferPoolManagerInstance::Print()  {
  std::cout << "----------BufferPoolManager-----------" << std::endl;
  for (size_t i = 0; i < pool_size_; ++i) {
    Page& frame = pages_[i];
    std::cout << "frame_id: " << i << ", page_id: " <<  frame.page_id_ << ", pin_count: " << frame.pin_count_ << std::endl;
  }
  std::cout << "--------------------------------------" << std::endl;
}


auto BufferPoolManagerInstance::Check() ->bool  {
  bool suc = true;
  std::set<page_id_t> set;
  for (size_t i = 0; i < pool_size_; ++i) {
    Page& frame = pages_[i];
    if(set.find(frame.page_id_) != set.end()){
      std::cout << "same pageid at diffrent frame " << std::endl;
      suc = false;
    }
    set.insert(frame.page_id_);
  }
  return suc;
   
}


}  // namespace bustub

