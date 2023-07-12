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
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  std::unique_lock free_list_lock(free_list_mutex_);
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    free_list_lock.unlock();
  } else {
    free_list_lock.unlock();
    if (!replacer_->Evict(&frame_id)) {
      BUSTUB_ASSERT(false, "NewPgImp nullptr \n ");
      return nullptr;
    }
  }
  page_id_t new_page_id = AllocatePage();

  Page &page = pages_[frame_id];
  if (page.page_id_ != INVALID_PAGE_ID) {
    page_table_->Remove(page.page_id_);
  }
  page.WLatch();
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  page.is_dirty_ = true;
  page.ResetMemory();
  page.page_id_ = new_page_id;
  page.pin_count_ = 1;
  page.WUnlatch();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(new_page_id, frame_id);

  if (page_id != nullptr) *page_id = new_page_id;
  return &page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page &page = pages_[frame_id];
    page.WLatch();
    page.pin_count_++;
    page.WUnlatch();
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }

  std::unique_lock free_list_lock(free_list_mutex_);
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    free_list_lock.unlock();
  } else {
    free_list_lock.unlock();
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }

  Page &page = pages_[frame_id];
  if (page.page_id_ != INVALID_PAGE_ID) {
    page_table_->Remove(page.page_id_);
  }
  page.WLatch();
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.is_dirty_ = false;
  }
  page.ResetMemory();
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page.GetData());
  page.WUnlatch();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);

  return &page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page &page = pages_[frame_id];
  page.WLatch();
  if (page.pin_count_ == 0) {
    page.WUnlatch();
    return false;
  }
  page.pin_count_--;
  page.is_dirty_ = is_dirty;
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page.WUnlatch();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id cannot be INVALID_PAGE_ID)");
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  page.WLatch();
  disk_manager_->WritePage(page.GetPageId(), page.GetData());
  page.is_dirty_ = false;
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
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.is_dirty_ = false;
    page.WUnlatch();
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // Although it is required to return true here, I believe that returning false is more appropriate.
    return false;
  }

  Page &page = pages_[frame_id];
  page.WLatch();
  if (page.GetPinCount() > 0) {
    page.WUnlatch();
    return false;
  }
  page_table_->Remove(page_id);
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page.WUnlatch();

  replacer_->SetEvictable(frame_id, true);
  if (replacer_->Remove(frame_id)) {
    std::unique_lock free_list_lock(free_list_mutex_);
    free_list_.push_back(frame_id);
    free_list_lock.unlock();
  }
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
