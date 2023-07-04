//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <stdexcept>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { 
  // return array_[GetSize()].second.GetPageId(); 
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  // array_[GetSize()].second = RID(next_page_id, 0);
  next_page_id_ = next_page_id;
}


/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // the lask key is for NextPageId
   
  BUSTUB_ASSERT(index < GetSize(), "invalid index ");  
  return array_[index].first ;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
   
  BUSTUB_ASSERT(index < GetSize(), "invalid index ");  
  return array_[index].second; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetAt(int index, const KeyType &key, const ValueType &value){
  BUSTUB_ASSERT(index < GetMaxSize(), "invalid index");
  array_[index].first=key;
  array_[index].second=value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexOfKey(const KeyType &key,  const KeyComparator &comparator) const -> int{
   for(int i = 0, size = GetSize(); i < size; i++) {
    if(comparator(key, array_[i].first) == 0){
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const MappingType &pair, const KeyComparator &comparator) -> bool{
  return Insert(pair.first, pair.second, comparator);
}
 

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool{
  BUSTUB_ASSERT(size_ < GetMaxSize(), "out of range");  
  int i ;
  // for( i = 0; i < size_ && comparator(key, array_[i].first) > 0; i++) ;
  for(i = GetSize(); i > 0 && comparator(key, array_[i-1].first) == -1; i--)
      ;
  InsertAt(key, value, i);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(const KeyType &key, const ValueType &value, int i) {
  for(int j= size_; j > i; j-- ){
    array_[j] = array_[j-1];
  }

  array_[i].first = key;
  array_[i].second = value;
  ++size_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Append(const KeyType &key, const ValueType &value) {
  BUSTUB_ASSERT(size_ < GetMaxSize(), "Insert out of range");    
  array_[size_].first = key;
  array_[size_].second = value;
  ++size_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Coalesce(B_PLUS_TREE_LEAF_PAGE_TYPE *other, const KeyComparator &comparator) {
  int other_size = other->GetSize();
  BUSTUB_ASSERT(size_ + other_size < GetMaxSize(), "Insert out of range"); 
  if(size_ == 0){
    std::copy(&other->array_[0], &other->array_[other_size], &array_[0]);
  }   
  else if(comparator(other->KeyAt(0), KeyAt(size_-1)) > 0 ){
    std::copy(&other->array_[0], &other->array_[other_size], &array_[size_]);
  } 
  else if (comparator(other->KeyAt(other_size-1), KeyAt(0)) < 0 ) {
    std::copy(&array_[0], &array_[size_], &other->array_[other_size]);
    std::copy(&other->array_[0], &other->array_[size_+other_size], &array_[0]);
  }
  size_ += other_size;
}




INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int i) {
  BUSTUB_ASSERT(i < GetSize(), "invalid index ");  
  for(int j = i, size = GetSize()-1 ; j < size; j++) {
    array_[j] = array_[j+1];
  }
  --size_;
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
