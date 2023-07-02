//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
// https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <bitset>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize(num_buckets_);
  for (int i = 0; i < num_buckets_; i++) {
    dir_[i] = std::make_shared<Bucket>(bucket_size_, global_depth_);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t bucket_index = IndexOf(key);
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // Search for the key in the bucket using the "Find" function of the bucket
  if (dir_[bucket_index]->Find(key, value)) {
    // If the key is found, update the value parameter with the value associated with the key and return true
    return true;
  } else {
    // If the key is not found, release the lock on the bucket and return false
    return false;
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  std::shared_ptr<Bucket> bucket = dir_[IndexOf(key)];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  int old_num_buckets_ = num_buckets_;
  std::unique_lock<std::shared_mutex> lock(mutex_);
  // std::cout << "insert(" << key <<"," << value << ")"<< std::endl;
  std::shared_ptr<Bucket> bucket = dir_[IndexOf(key)];
  // bucket->Insert(key, value);
  if (bucket->IsFull()) {
    if (bucket->GetDepth() == global_depth_) {
      int high_bit = 1 << global_depth_;
      num_buckets_ *= 2;
      global_depth_++;
      dir_.resize(num_buckets_);
      for (int i = 0; i < old_num_buckets_; i++) {
        // LOG_DEBUG("%d, %d, %d, %d\n", i, high_bit, i + high_bit, num_buckets_);
        dir_[i + high_bit] = dir_[i];
      }
    }
    std::shared_ptr<Bucket> bucket0 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    std::shared_ptr<Bucket> bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    int local_hight_bit = 1 << (bucket->GetDepth());
    for (const auto &[key2, value2] : bucket->GetItems()) {
      std::shared_ptr<Bucket> new_buket = (std::hash<K>()(key2) & local_hight_bit) == 0 ? bucket0 : bucket1;
      new_buket->Insert(key2, value2);
    }

    for (int i = std::hash<K>()(key) & (local_hight_bit - 1); i < num_buckets_; i += local_hight_bit) {
      dir_[i] = (i & local_hight_bit) == 0 ? bucket0 : bucket1;
    }
  }

  bucket = dir_[IndexOf(key)];
  bucket->Insert(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::show() {
  std::cout << "Gloable depth = " << global_depth_ << std::endl;
  for (int i = 0; i < num_buckets_; i++) {
    std::shared_ptr<Bucket> bucket = dir_[i];
    std::bitset<4> bits(i);
    std::cout << bits << "(" << bucket->GetDepth() << ")"
              << ":";

    for (const auto &[key, value] : bucket->GetItems()) {
      std::cout << key << ", ";
    }
    std::cout << std::endl;
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t size, int depth) : size_(size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&key](std::pair<K, V> &item) { return item.first == key; });
  if (it != list_.end()) {
    value = it->second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&key](std::pair<K, V> &item) { return item.first == key; });
  if (it != list_.end()) {
    list_.erase(it);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  auto it = std::find_if(list_.begin(), list_.end(), [&key](std::pair<K, V> &item) { return item.first == key; });
  if (it != list_.end()) {
    it->second = value;
    return true;
  } else if (!IsFull()) {
    list_.push_back({key, value});
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
