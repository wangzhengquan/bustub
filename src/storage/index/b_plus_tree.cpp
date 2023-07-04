#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  header_page->GetRootId(index_name_, &root_page_id_);
  // if(root_page_id_!= INVALID_PAGE_ID)
  //   root_page_ = static_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPlusTree(){
   
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  return root_page_id_ == INVALID_PAGE_ID;
     

  // BPlusTreePage * page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  // return page->GetSize() == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto *leaf_page = Find(key);
  if(leaf_page == nullptr)
    return false;

  int i = leaf_page->IndexOfKey(key, comparator_) ;
  if(i == -1){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  } else {
    result->push_back(leaf_page->ValueAt(i));
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }
  
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Find(const KeyType &key) -> LeafPage* {
  if(root_page_id_ == INVALID_PAGE_ID) 
    return nullptr;

  BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while(!cur_page->IsLeafPage()){
    InternalPage * cur_inter_page = static_cast<InternalPage* >(cur_page);
    int i = cur_inter_page->IndexOfKey(key, comparator_);
   // BUSTUB_ASSERT(i >=0 , "Find error invalide index = %d\n ", i); 
    if(i == -1) return nullptr;

    page_id_t page_id = cur_inter_page->ValueAt(i);
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
   
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  }

  if(cur_page->IsLeafPage()) {
    LeafPage * cur_leaf_page = static_cast<LeafPage * >(cur_page);
    return cur_leaf_page;
  }

  return nullptr;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if(root_page_id_ == INVALID_PAGE_ID) {
    LeafPage * root_page  = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    root_page->SetNextPageId(INVALID_PAGE_ID);
   // buffer_pool_manager_->FlushPage(root_page_id_);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }

  LeafPage *page = Find(key);
// std::cout << ">>>>>>>>>> page_le = "<< page_l << std::endl;
  if(page->IndexOfKey(key, comparator_) != -1){
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  LeafPageInsert(page, key, value);
  return true;
 
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafPageInsert(LeafPage * page, const KeyType &key, const ValueType &value) {
  page->Insert(key, value, comparator_);
  if(page->GetSize() < page->GetMaxSize()){
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
  // split 
  page_id_t page_r_id;
  LeafPage * page_r = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&page_r_id)->GetData());
  page_r->Init(page_r_id, page->GetParentPageId(), leaf_max_size_);
  int mid = ceil(static_cast<double>(page->GetMaxSize() - 1) / 2);
  for(int i = mid, j=0; i < page->GetSize(); i++, j++){
    page_r->array_[j] = page->array_[i];
  }
  page->SetSize(mid);
  page_r->SetSize(page->GetMaxSize() - mid);
  page_r->SetNextPageId(page->GetNextPageId());
  page->SetNextPageId(page_r->GetPageId());
  if(page->IsRootPage()){
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page_r->GetPageId(), true);
    InsertInNewRoot(page->KeyAt(0), page, page_r->KeyAt(0), page_r);
  } else {
    InternalPage * parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page_r->GetParentPageId() )->GetData());
    InternalPageInsert(parent_page, page_r->KeyAt(0), page_r_id);
  }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalPageInsert(InternalPage * page, const KeyType &key, const page_id_t &value)  {
  page->Insert(key, value, comparator_);
  if(page->GetSize() < page->GetMaxSize()){
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return ;
  }
  // split 
  page_id_t page_r_id;
  InternalPage * page_r = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&page_r_id)->GetData());
  page_r->Init(page_r_id, page->GetParentPageId(), leaf_max_size_);
  int mid = ceil(static_cast<double>(page->GetMaxSize()-1)/2);
  for(int i = mid, j=0; i < page->GetSize(); i++, j++){
    page_r->array_[j] = page->array_[i];
  }
  page->SetSize(mid);
  page_r->SetSize(page->GetMaxSize() - mid);

  for (int i = 0; i < page_r->GetSize(); i++) {
    // Set parent_id of nodes in page_r to page_r_id
    BPlusTreePage * page_tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_r->ValueAt(i))->GetData());
    page_tmp->SetParentPageId(page_r_id);
  }

  if(page->IsRootPage()){
    InsertInNewRoot(page->KeyAt(0), page, page_r->KeyAt(0), page_r);
  } else {
    InternalPage * parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page_r->GetParentPageId() )->GetData());
    InternalPageInsert(parent_page, page_r->KeyAt(0), page_r->GetPageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInNewRoot(const KeyType &key, BPlusTreePage *page, const KeyType &key_r, BPlusTreePage *page_r) {
  InternalPage * root_page  = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
  root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
  page->SetParentPageId(root_page_id_);
  page_r->SetParentPageId(root_page_id_);
  root_page->Insert(key, page->GetPageId(), comparator_);  
  root_page->Insert(key_r, page_r->GetPageId(), comparator_);
  UpdateRootPageId(0);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);

} 
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LeafPage *leaf_page = Find(key);
  if(leaf_page == nullptr)
    return ;

  int i = leaf_page->IndexOfKey(key, comparator_) ;
  if(i == -1){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }  
  LeafPageRemoveAt(leaf_page, i, key);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return ;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafPageRemoveAt(LeafPage * m_page, int index, const KeyType &key){
  LeafPage * l_page=nullptr, * r_page=nullptr;
  m_page->RemoveAt(index); 
  if(m_page->IsRootPage()){
    return;
  }
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(m_page->GetParentPageId() )->GetData());
  int indexOfPage = parent_page->IndexOfKey(key, comparator_);
  int min = m_page->GetMinSize(); 

  // if the original minest key was the one which is removed
  parent_page->SetKeyAt(indexOfPage, m_page->KeyAt(0));
  if(m_page->GetSize() >= min){
    return;
  }

  if(indexOfPage+1 < parent_page->GetSize()){
    r_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(indexOfPage+1))->GetData());
  }
  if(indexOfPage-1 >=0 ){
    l_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(indexOfPage-1))->GetData());
  }
    
  if(l_page != nullptr && l_page->GetSize() > min){
    // borrow from left
    m_page->Insert(l_page->At(l_page->GetSize()-1), comparator_);
    l_page->RemoveAt(l_page->GetSize()-1);
    parent_page->SetKeyAt(indexOfPage, m_page->KeyAt(0));

  }
  else if(r_page != nullptr && r_page->GetSize() > min){
    // borrow from right
    m_page->Insert(r_page->At(0), comparator_);
    r_page->RemoveAt(0);
    parent_page->SetKeyAt(indexOfPage+1, r_page->KeyAt(0));

  } else {
    // Coalesce
    if(l_page != nullptr){
      l_page->Coalesce(m_page, comparator_);
      l_page->SetNextPageId(m_page->GetNextPageId());
      buffer_pool_manager_->DeletePage(m_page->GetPageId());
      InternalPageRemoveAt(parent_page, indexOfPage, key);
    } else if(r_page != nullptr){
      m_page->Coalesce(r_page, comparator_);
      m_page->SetNextPageId(r_page->GetNextPageId());
      buffer_pool_manager_->DeletePage(r_page->GetPageId());
      InternalPageRemoveAt(parent_page, indexOfPage+1, key);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalPageRemoveAt(InternalPage * m_page, int index, const KeyType &key){
  InternalPage * l_page=nullptr, * r_page=nullptr;
  m_page->RemoveAt(index);
  if(m_page->IsRootPage()){
    if(m_page->GetSize()==1){
      root_page_id_ = m_page->ValueAt(0);
      UpdateRootPageId(0);
      buffer_pool_manager_->DeletePage(m_page->GetPageId());
    }
    return;
  }

  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(m_page->GetParentPageId() )->GetData());
  int indexOfPage = parent_page->IndexOfKey(key, comparator_);
  int min = m_page->GetMinSize(); ;
  
  // if the original minest key was the one which is removed
  parent_page->SetKeyAt(indexOfPage, m_page->KeyAt(0));
  if(m_page->GetSize() >= min){
    return;
  }
  if(indexOfPage+1 < parent_page->GetSize()){
    r_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(indexOfPage+1))->GetData());
  }
  if(indexOfPage-1 >=0 ){
    l_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(indexOfPage-1))->GetData());
  }

  if(l_page != nullptr && l_page->GetSize() > min){
    // borrow from left
    m_page->Insert(l_page->At(l_page->GetSize()-1), comparator_);
    l_page->RemoveAt(l_page->GetSize()-1);
    parent_page->SetKeyAt(indexOfPage, m_page->KeyAt(0));
  }
  else if(r_page != nullptr && r_page->GetSize() > min){
    // borrow from right
    m_page->Insert(r_page->At(0), comparator_);
    r_page->RemoveAt(0);
    parent_page->SetKeyAt(indexOfPage+1, r_page->KeyAt(0));
  } else {
    // Coalesce
    if(l_page != nullptr){
      l_page->Coalesce(m_page, comparator_);
      buffer_pool_manager_->DeletePage(m_page->GetPageId());
      InternalPageRemoveAt(parent_page, indexOfPage, key);
    } else if(r_page != nullptr){
      m_page->Coalesce(r_page, comparator_);
      buffer_pool_manager_->DeletePage(r_page->GetPageId());
      InternalPageRemoveAt(parent_page, indexOfPage+1, key);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while(!cur_page->IsLeafPage()){
    InternalPage * cur_inter_page = static_cast<InternalPage* >(cur_page);
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_inter_page->ValueAt(0))->GetData());
  }
  return INDEXITERATOR_TYPE(static_cast<LeafPage* >(cur_page), buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  LeafPage* leaf_page = Find(key);
  int i = leaf_page->IndexOfKey(key, comparator_) ;
  
  return INDEXITERATOR_TYPE(leaf_page, buffer_pool_manager_, i == -1 ? 0 : i); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while(!cur_page->IsLeafPage()){
    InternalPage * cur_inter_page = static_cast<InternalPage* >(cur_page);
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_inter_page->ValueAt(cur_inter_page->GetSize()-1))->GetData());
  }
  return INDEXITERATOR_TYPE(static_cast<LeafPage* >(cur_page), buffer_pool_manager_, cur_page->GetSize());  
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
std::cout << "InsertFromFile " << file_name << std::endl;
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
    std::cout << "file insert  " << key << std::endl;
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << "; parent: " << leaf->GetParentPageId()
              << "; next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << "; parent: " << internal->GetParentPageId() << std::endl;
    // std::cout << "( :" << internal->array_[0].second << "),";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << "(" << internal->KeyAt(i) << ":" << internal->ValueAt(i) << ")" << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    // ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->array_[0].second)->GetData()), bpm);
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
