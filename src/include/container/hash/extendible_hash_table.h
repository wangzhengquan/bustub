//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
  
 public:
  using ElementType = std::pair<K, V>;
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  auto Insert(const K &key, const V &value) -> bool override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override;

  auto GetSize() -> size_t {
    return size_;
  }
  void Show();
  auto Check() -> bool;

  auto IndexOf(const K &key) -> size_t;


 private:
  struct Node {
    ElementType value;
    Node* next;
    Node* pre;
    Node(const ElementType& v = ElementType(), Node* n = nullptr, Node* p = nullptr) :
        value(v), next(n), pre(p) {}
  };
  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    friend class ExtendibleHashTable;
    explicit Bucket(size_t capcity, int depth = 0);
    ~Bucket();


    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return size_ >= capcity_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetSize() const -> size_t { return size_; }
    inline auto GetList() const -> Node * { return list_; }
    /**
     *
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key) -> Node *;

    /**
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    // auto remove(const K &key) -> bool;
    auto Remove(const Node *node) -> bool;

    /**
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return The bool component is true if the insertion took place and false if the assignment took place. 
     *         The Node * component is pointing at the element that was inserted or updated
     */
    auto InsertOrAssign(const K &key, const V &value) -> std::pair<Node *, bool>;

    void Clear();

    const size_t capcity_;
    size_t size_ = 0;
    int depth_;
    Node *list_ = nullptr;
  };
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::shared_mutex mutex_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table
  size_t size_ = 0;
  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  // auto IndexOf(const K &key) -> size_t;

  // auto GetGlobalDepthInternal() const -> int;
  // auto GetLocalDepthInternal(int dir_index) const -> int;
  // auto GetNumBucketsInternal() const -> int;
};
 

}  // namespace bustub
