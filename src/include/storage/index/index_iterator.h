//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  // using difference_type   = std::ptrdiff_t;
  // using value_type        = MappingType;
  // using pointer           = MappingType*;
  // using reference         = MappingType&;
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = nullptr, BufferPoolManager *bpm=nullptr, int index = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() ->  MappingType &;
  auto operator->() ->  MappingType *;
  // Prefix increment
  auto operator++() -> IndexIterator &;
  // Postfix increment
  auto operator++(int) -> IndexIterator;

  auto operator==(const IndexIterator &other) const -> bool ;

  auto operator!=(const IndexIterator &itr) const -> bool { return !(itr == *this); }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_ = nullptr;
  BufferPoolManager *bpm_;
  int index_;
};

}  // namespace bustub
