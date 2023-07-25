//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <list>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 private:
  enum class Operation { FIND = 0, INSERT, REMOVE };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE - 1);
  ~BPlusTree();
  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  auto Remove(const KeyType &key, Transaction *transaction = nullptr) -> bool;

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  auto Check()  ->  bool ;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);
  void UnlockPageList(std::list<BPlusTreePage *> &locked_list, bool dirty, const Operation op);
  // void PrintLockedPageList(std::list<BPlusTreePage *> &locked_list);
  auto Check_(BPlusTreePage *page)  -> bool ;
  /**
   * @insert find for insert
   */
  auto Find(const KeyType &key, Operation op, std::list<BPlusTreePage *> &locked_list)
      -> BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *;
  /**
   * Insert  {key, value} into LeafPage
   */
  void InsertInLeafPage(LeafPage *page, const KeyType &key, const ValueType &value);
  /**
   * Insert  {key, value} into InternalPage
   */
  void InsertInInternalPage(InternalPage *page, const KeyType &key, const page_id_t &value);
  void InsertInNewRoot(const KeyType &key, BPlusTreePage *page, const KeyType &key_r, BPlusTreePage *page_r);
  /**
   * Remove the node at index in LeafPage
   */
  void RemoveInLeafPage(LeafPage *page, int index, const KeyType &key);
  void RemoveInInternalPage(InternalPage *page, int index, const KeyType &key);
  /**
   * Change the parent id of all child nodes under this node to this node's id.
   * */
  // void SetParentOfChildren(InternalPage *page);
  void SetParentOfChildrenInPageTo(InternalPage *page, page_id_t parent_page_id);
  void SetParentOfPageTo(page_id_t page_id, page_id_t parent_page_id) ;

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t root_page_id_ = INVALID_PAGE_ID;
  // It is employed as a lock instead of using it as a real page.
  BPlusTreePage * new_root_page_;
  // BPlusTreePage* root_page_ = nullptr;
};

}  // namespace bustub
