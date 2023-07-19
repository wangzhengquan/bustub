//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>
#include <stdexcept>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // BUSTUB_ASSERT(max_size > 2, "invalid max_size");
  SetMaxSize(max_size + 1);
  SetSize(0);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "invalid index ");
  return At(index).first;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "invalid index ");
  return At(index).second; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "invalid index ");
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "invalid index ");
  array_[index].second = value;
}

/**
 * Position where to find the key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexOfKey(const KeyType &key, const KeyComparator &comparator) const -> int {
  int i;
  // Note that the compare operation will stop when 'i=1', that is because the key at position 0 in an internal node is
  // a virtual key that conceptually represents the minimum key of the node.
  for (i = GetSize() - 1; i > 0 && comparator(key, array_[i].first) == -1; i--)
    ;
  return i;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const MappingType &pair, const KeyComparator &comparator) -> int {
  return Insert(pair.first, pair.second, comparator);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int i;
  /**
   * Here, the stop condition is 'i > 1' instead of 'i > 0' because the key at position 0 in an internal node is a
   * virtual key that conceptually represents the minimum key of the node. However, it may not necessarily have the
   * minimum or even an actual value associated with it. To treat the virtual key as the minimum one, when iterating
   * over the keys in the node, we start from maximum position and continue until 'i > 1'. Therefore, unless the
   * 'array_' is empty, the insert index 'i' will always be greater than 0.
   */
  for (i = GetSize(); i > 1 && comparator(key, array_[i - 1].first) == -1; i--)
    ;
  InsertAt(key, value, i);
  return i;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(const KeyType &key, const ValueType &value, int i) {
  BUSTUB_ASSERT(size_ < GetMaxSize(), "Insert out of range");
  for (int j = size_; j > i; j--) {
    array_[j] = array_[j - 1];
  }
  array_[i].first = key;
  array_[i].second = value;
  ++size_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Append(const KeyType &key, const ValueType &value) {
  BUSTUB_ASSERT(size_ < GetMaxSize(), "Insert out of range");
  array_[size_].first = key;
  array_[size_].second = value;
  ++size_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Coalesce(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other, const KeyComparator &comparator) {
  int other_size = other->GetSize();
  if (other_size == 0) return;
  BUSTUB_ASSERT(size_ + other_size < GetMaxSize(), "Insert out of range");
  if (size_ == 0) {
    std::copy(&other->array_[0], &other->array_[other_size], &array_[0]);
  } else if (comparator(other->KeyAt(0), KeyAt(size_ - 1)) > 0) {
    std::copy(&other->array_[0], &other->array_[other_size], &array_[size_]);
  } else if (comparator(other->KeyAt(other_size - 1), KeyAt(0)) < 0) {
    std::copy(&array_[0], &array_[size_], &other->array_[other_size]);
    std::copy(&other->array_[0], &other->array_[size_ + other_size], &array_[0]);
  }
  size_ += other_size;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int i) {
  BUSTUB_ASSERT(i < GetSize(), "invalid index ");
  for (int j = i, size = GetSize() - 1; j < size; j++) {
    array_[j] = array_[j + 1];
  }
  --size_;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
