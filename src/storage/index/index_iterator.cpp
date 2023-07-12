/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, BufferPoolManager *bpm, int index)
    : leaf_page_(leaf_page), bpm_(bpm), index_(index){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_->GetNextPageId() == INVALID_PAGE_ID && index_ >= leaf_page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->At(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator->() -> const MappingType * { return &(leaf_page_->At(index_)); }

// Prefix increment
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ < leaf_page_->GetSize()) {
    ++index_;
  } else {
    leaf_page_ =
        reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bpm_->FetchPage(leaf_page_->GetNextPageId())->GetData());
    index_ = 0;
  }
  return *this;
}
// Postfix increment
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++(int) -> INDEXITERATOR_TYPE {
  INDEXITERATOR_TYPE tmp = *this;
  ++(*this);
  return tmp;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
