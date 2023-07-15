//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), frames_(num_frames) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  frame_id_t max_k_dist_frame_id;
  size_t max_k_dist = 0;
  bool suc = false;
  std::unique_lock lock(mutex_);
  if (evictable_size_ == 0) return false;
  for (size_t i = 0; i < frames_.size(); i++) {
    std::shared_ptr<Frame> frame = frames_[i];
    if (!frame || !frame->Evictable()) {
      continue;
    }

    suc = true;
    size_t k_dist = frame->KDistance(current_timestamp_);
    if (k_dist > max_k_dist) {
      max_k_dist = k_dist;
      max_k_dist_frame_id = i;
    } else if (k_dist == max_k_dist) {
      if (frame->Distance(current_timestamp_) > frames_[max_k_dist_frame_id]->Distance(current_timestamp_)) {
        max_k_dist_frame_id = i;
      }
    }
  }

  if (suc) {
    frames_[max_k_dist_frame_id] = nullptr;
    evictable_size_--;
    *frame_id = max_k_dist_frame_id;
  }

  return suc;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid (ie. larger than replacer_size_)");
  std::unique_lock lock(mutex_);
  std::shared_ptr<Frame> &frame = frames_[frame_id];
  if (!frame) {
    frame = std::make_shared<Frame>(k_);
    frames_[frame_id] = frame;
  }
  frame->RecordAccess(++current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid (ie. larger than replacer_size_)");
  std::unique_lock lock(mutex_);
  std::shared_ptr<Frame> &frame = frames_[frame_id];
  if (!frame) {
    return;
  }
  if (evictable == frame->Evictable()) return;

  evictable ? evictable_size_++ : evictable_size_--;
  frame->SetEvictable(evictable);
}

auto LRUKReplacer::Remove(frame_id_t frame_id) -> bool {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid (ie. larger than replacer_size_)");
  std::unique_lock lock(mutex_);
  std::shared_ptr<Frame> &frame = frames_[frame_id];
  if (!frame) return false;
  if (!frame->Evictable()) {
    throw std::invalid_argument("Remove is called on a non-evictable frame.");
    return false;
  }

  frames_[frame_id] = nullptr;
  evictable_size_--;
  return true;
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock lock(mutex_);
  return evictable_size_;
}

}  // namespace bustub