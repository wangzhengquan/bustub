//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <list>
#include <queue>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))

/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
  friend class BPlusTree<KeyType, RID, KeyComparator>;
  friend class BPlusTree<KeyType, ValueType, KeyComparator>;

 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE - 1);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);
  inline auto At(int index) -> MappingType &;
  inline auto At(int index) const -> const MappingType &;
  auto IndexOfKey(const KeyType &key, const KeyComparator &comparator) const -> int;
  /**
   * @return the position where it's inserted
   */
  auto Insert(const MappingType &pair, const KeyComparator &comparator) -> int;
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int;
  void InsertAt(const MappingType &pair, int i);
  void Append(const KeyType &key, const ValueType &value);
  void Coalesce(BPlusTreeInternalPage *other, const KeyComparator &comparator, const bool to_right);
  void RemoveAt(int i);

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::At(int index) -> MappingType & {
  BUSTUB_ASSERT(index < GetSize(), "invalid index ");
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::At(int index) const -> const MappingType & {
  BUSTUB_ASSERT(index < GetSize(), "invalid index ");
  return array_[index];
}

}  // namespace bustub
