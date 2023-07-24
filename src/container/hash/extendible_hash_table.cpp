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
#include "container/hash/hash_function.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1 << global_depth_) {
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
  std::shared_lock<std::shared_mutex> lock(mutex_);
  Node *node = dir_[IndexOf(key)]->Find(key);
  if(node == nullptr){
    return false;
  }
  value = node->value.second;
  return true;
   
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  std::shared_ptr<Bucket> bucket = dir_[IndexOf(key)];
  Node *node = bucket->Find(key);
  if(node == nullptr){
    return false;
  }
  if(bucket->Remove(node)) {
    --size_;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) -> bool{
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  std::shared_ptr<Bucket> bucket = dir_[IndexOf(key)];
  while (bucket->IsFull()) {
    // extend dir
    if (bucket->GetDepth() == global_depth_) {
      // int high_bit = 1 << global_depth_;
      dir_.resize(num_buckets_ << 1);
      for (int i = 0; i < num_buckets_; i++) {
        // LOG_DEBUG("i = %d, high_bit = %d, i + high_bit = %d, num_buckets_ = %d\n", i, high_bit, i + high_bit, num_buckets_);
        dir_[i + num_buckets_] = dir_[i];
      }
      num_buckets_ <<= 1;
      global_depth_++;
    }

   
    // split bucket
    std::shared_ptr<Bucket> bucket0 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    std::shared_ptr<Bucket> bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    int local_hight_bit_mask = 1 << (bucket->GetDepth());
    // split the bucket into two bucket according to the (bucket->GetDepth()+1)'th bit of the hash value of the node's key 
    for(Node *node = bucket->list_; node != nullptr; node = node->next){
      // size_t
// std::cout << node->value.first << " , hash  = " << std::bitset<32>(std::hash<K>()(node->value.first)) 
//  << ", local_hight_bit="<< (std::hash<K>()(node->value.first) & local_hight_bit_mask) << std::endl;

      std::shared_ptr<Bucket> new_bucket = (std::hash<K>()(node->value.first) & local_hight_bit_mask) == 0 ? bucket0 : bucket1;
      new_bucket->InsertOrAssign(node->value.first, node->value.second);
    } 

    /* 
     * All of the dir items whose index  has the same low 'bucket->GetDepth()' bits with 'bucket_index' points to the same bucket,
     * All of them should be split into two buckets base on the (bucket->GetDepth()+1)'th bit of their corresponding index.
     */
    ;
    for (int i = std::hash<K>()(key) & (local_hight_bit_mask - 1); i < num_buckets_; i += local_hight_bit_mask) {
      // if bucket->GetDepth()'th bit of the i is 0 set dir_[i] points to bucket0, else to bucket1;
      dir_[i] = (i & local_hight_bit_mask) == 0 ? bucket0 : bucket1;
    }

    bucket = dir_[IndexOf(key)];
  }
  
  auto [node, is_insert] = bucket->InsertOrAssign(key, value);
  if(is_insert){
    size_++;
  }
  return true;
      
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Show() {
  std::cout << "---------- Table Structure -----------" << std::endl;
  std::cout << "Gloable depth = " << global_depth_ << std::endl;
  for (int i = 0; i < num_buckets_; i++) {
    std::shared_ptr<Bucket> bucket = dir_[i];
    std::bitset<32> bits(i);
    if(!bucket){
       std::cout << i << ") : null" << std::endl;
       continue;
    }
    std::cout << i << ") " << bits << "(depth=" << bucket->GetDepth() << ")" << " : ";
         
    for(Node *node = bucket->list_; node != nullptr; node = node->next){
      // std::cout << "(" << node->value.first << ", " << node->value.second << ") ";
       std::cout << "(" << node->value.first << ", " << std::bitset<32>(std::hash<K>()(node->value.first)) << ") ";
    }
    std::cout << std::endl;
  }
  std::cout << "--------------------------------------" << std::endl;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Check() -> bool{
  int suc = true;
  for (int i = 0; i < num_buckets_; i++) {
    std::shared_ptr<Bucket> bucket = dir_[i];
    if(!bucket){
      LOG_ERROR("bucket  %d is null. ", i);
      suc = false;
      continue;
    }
    int mask = (1 << (bucket->GetDepth())) - 1;   
    // int pre = -1;  
    for(Node *node = bucket->list_; node != nullptr; node = node->next){
      // std::cout << "(" << node->value.first << ", " << node->value.second << ") ";
      int keybit = std::hash<K>()(node->value.first ) & mask;
      if(keybit != (i & mask)){
        suc = false;
        std::cout << "keybit , i=" << i << ", key=" << node->value.first << std::endl;
        // LOG_ERROR("keybit of %d:  ", i, );
      }
    }
  }
  return suc;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t capcity, int depth) : capcity_(capcity), depth_(depth) {
  // std::cout << "Bucket construct" << std::endl;
}

template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::~Bucket(){
  // std::cout << "~Bucket" << std::endl;
  Clear();
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Bucket::Clear(){
  Node *node = list_;
  while (node != nullptr) {
    auto temp = node;
    node = node->next;
    delete temp;
    // temp = nullptr;
  }
  list_ = nullptr;
  size_ = 0;
  depth_ = 0;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key) -> Node * {
  for( Node *node = list_; node != nullptr; node = node->next){
    if(node->value.first == key ){
      return node;
    }
    
  }
  return nullptr;
}


template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const Node *node) -> bool {
  if(node == nullptr){
    return false;
  }
  if(node->pre != nullptr){
    node->pre->next = node->next;
  } 

  if(node->next != nullptr){
    node->next->pre = node->pre;
  } 

  if(node == list_) {
    list_ = node->next;
  }
  

  delete node;
  --size_;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::InsertOrAssign(const K &key, const V &value) -> std::pair<Node*, bool> {
  for( Node *node = list_; node != nullptr; node = node->next){
    if(node->value.first == key ){
      node->value.second = value;
      return {node, false};
    }
  }
  BUSTUB_ASSERT(!IsFull(), "Bucket is full");
  Node * node = new Node({key, value}, list_);
  if(list_ != nullptr){
    list_->pre = node;
  }
  
  list_ = node;
  size_++;
  return {node, true};
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
