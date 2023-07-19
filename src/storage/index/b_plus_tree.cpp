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
      bpm_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  new_root_latch_ = new BPlusTreePage();
  new_root_latch_->page_id_ = INVALID_PAGE_ID;
  //  std::cout << "=========root_page_id_=========" << root_page_id_ << std::endl;
  auto *header_page = static_cast<HeaderPage *>(bpm_->FetchPage(HEADER_PAGE_ID));
  header_page->GetRootId(index_name_, &root_page_id_);
  bpm_->UnpinPage(HEADER_PAGE_ID, false);
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPlusTree() {
  delete new_root_latch_;
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
  // BPlusTreePage * page = reinterpret_cast<BPlusTreePage
  // *>(bpm_->FetchPage(root_page_id_)->GetData()); return page->GetSize() == 0;
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
  std::list<BPlusTreePage *> locked_list;
  auto *leaf_page = Find(key, Operation::FIND, &locked_list);
  if (leaf_page == nullptr) {
    UnlockPageList(&locked_list, false, Operation::FIND);
    return false;
  }

  int i = leaf_page->IndexOfKey(key, comparator_);
  if (i == -1) {
    UnlockPageList(&locked_list, false, Operation::FIND);
    return false;
  } else {
    result->push_back(leaf_page->ValueAt(i));
    UnlockPageList(&locked_list, false, Operation::FIND);
    return true;
  }
}

/**
 * remove 也会修改父节点，所以remove要锁定可能会修改的节点及其父亲节点
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Find(const KeyType &key, Operation op, std::list<BPlusTreePage *> *locked_list) -> LeafPage * {
  if (root_page_id_ == INVALID_PAGE_ID) return nullptr;

  BPlusTreePage * parent_page = nullptr, * grandparent_page = nullptr, * cur_page = nullptr;
  cur_page = new_root_latch_;
  if (op == Operation::FIND) {
    cur_page->latch_.RLock();
    locked_list->push_back(cur_page);
  } else if (op == Operation::INSERT) {
    cur_page->latch_.WLock();
    locked_list->push_back(cur_page);
  } else if (op == Operation::REMOVE) {
    cur_page->latch_.WLock();
  } 

  grandparent_page = parent_page;
  parent_page = cur_page;
 
  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(root_page_id_)->GetData());
  cur_page = root_page;
  if (op == Operation::FIND) {
    UnlockPageList(locked_list, false, op);
    cur_page->latch_.RLock();
    locked_list->push_back(cur_page);
  } else if (op == Operation::INSERT){
    cur_page->latch_.WLock();
    if (cur_page->GetSize() < cur_page->GetMaxSize() - 1) {
      UnlockPageList(locked_list, false, op);
    }
    locked_list->push_back(cur_page);
  } else if (op == Operation::REMOVE) {
    cur_page->latch_.WLock();
    // locked_list->push_back(parent_page);
  }

  while (!cur_page->IsLeafPage()) {
    grandparent_page = parent_page;
    parent_page = cur_page;

    InternalPage *cur_inter_page = static_cast<InternalPage *>(cur_page);
    int i = cur_inter_page->IndexOfKey(key, comparator_);
    BUSTUB_ASSERT(i >= 0, "Find error, invalide index %d", i);
    cur_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(cur_inter_page->ValueAt(i))->GetData());
    if (op == Operation::FIND) {
      UnlockPageList(locked_list, false, op);
      cur_page->latch_.RLock();
      locked_list->push_back(cur_page);
    }
    else if (op == Operation::INSERT) {
      cur_page->latch_.WLock();
      if (cur_page->GetSize() < cur_page->GetMaxSize() - 1) {
        UnlockPageList(locked_list, false, op);
      }
      
      locked_list->push_back(cur_page);
    } else if (op == Operation::REMOVE) {
      cur_page->latch_.WLock();
      if (cur_page->GetSize() > cur_page->GetMinSize()) {
        UnlockPageList(locked_list, false, op);
      }
      // 释放grandparent以前的锁
      locked_list->push_back(grandparent_page);
    }
  }
  if (op == Operation::REMOVE) {
    locked_list->push_back(parent_page);
    locked_list->push_back(cur_page);
  }
  LeafPage *cur_leaf_page = static_cast<LeafPage *>(cur_page);

  return cur_leaf_page;
}
 
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPageList(std::list<BPlusTreePage *> *locked_list, bool dirty, const Operation op) {
  // std::cout << "-------------- UnlockPageList -------------" << std::endl;
  if (locked_list == nullptr) {
    return;
  }

  while (!locked_list->empty()) {
    BPlusTreePage *page = locked_list->front();
// std::cout << page->GetPageId() << " | ";
    if(op == Operation::FIND){
      page->latch_.RUnlock();
    } else {
      page->latch_.WUnlock();
    }
    
    if(page != new_root_latch_) {
      bpm_->UnpinPage(page->GetPageId(), dirty);
    }
    locked_list->pop_front();
  }

// std::cout << "\n------------------------------------------" << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintLockedPageList(std::list<BPlusTreePage *> *locked_list) {
  std::cout << "-------------- UnlockPageList -------------" << std::endl;
  if (locked_list != nullptr) {
    for(BPlusTreePage *page : *locked_list){
      LOG_DEBUG("unlock pageid=%d\n", page->GetPageId() );
    }
  }
  std::cout << "------------------------------------------" << std::endl;
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
  // std::cout << "insert " << key << std::endl;
  if (root_page_id_ == INVALID_PAGE_ID) {
    new_root_latch_->latch_.WLock();
    if (root_page_id_ == INVALID_PAGE_ID) {
      page_id_t root_page_id;
      LeafPage *root_page = reinterpret_cast<LeafPage *>(bpm_->NewPage(&root_page_id)->GetData());
      root_page->latch_.WLock();
      std::cout << "root_page_id=" << root_page_id << std::endl;
      root_page->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
      root_page->SetNextPageId(INVALID_PAGE_ID);
      root_page_id_ = root_page_id;
      UpdateRootPageId(1);
      root_page->latch_.WUnlock();
      bpm_->UnpinPage(root_page_id, true);
    }
    new_root_latch_->latch_.WUnlock();
  } 
   

  std::list<BPlusTreePage *> locked_list;

  LeafPage *page = Find(key, Operation::INSERT, &locked_list);
  if (page->IndexOfKey(key, comparator_) != -1) {
    UnlockPageList(&locked_list, false, Operation::INSERT);
    return false;
  }
  InsertInLeafPage(page, key, value);
  UnlockPageList(&locked_list, true, Operation::INSERT);
// bpm_->Print();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeafPage(LeafPage *page, const KeyType &key, const ValueType &value) {
  page->Insert(key, value, comparator_);
  if (page->GetSize() < page->GetMaxSize()) {
    return;
  }
  // split
  page_id_t page_r_id;
  LeafPage *page_r = reinterpret_cast<LeafPage *>(bpm_->NewPage(&page_r_id)->GetData());
  // page_r->latch_.WLock();
  page_r->Init(page_r_id, page->GetParentPageId(), leaf_max_size_);
  int mid = page->GetMinSize();
  for (int i = mid, j = 0; i < page->GetSize(); i++, j++) {
    page_r->array_[j] = page->array_[i];
  }
  page->SetSize(mid);
  page_r->SetSize(page->GetMaxSize() - mid);
  page_r->SetNextPageId(page->GetNextPageId());
  page->SetNextPageId(page_r->GetPageId());
  if (page->IsRootPage()) {
    InsertInNewRoot(page->KeyAt(0), page, page_r->KeyAt(0), page_r);
  } else {
    InternalPage *parent_page =
        reinterpret_cast<InternalPage *>(bpm_->FetchPage(page_r->GetParentPageId())->GetData());
    InsertInInternalPage(parent_page, page_r->KeyAt(0), page_r_id);
    bpm_->UnpinPage(parent_page->GetPageId(), true);
  }
  bpm_->UnpinPage(page_r->GetPageId(), true);
  // page_r->latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInInternalPage(InternalPage *page, const KeyType &key, const page_id_t &value) {
  page->Insert(key, value, comparator_);
  if (page->GetSize() < page->GetMaxSize()) {
    return;
  }
  // split
  page_id_t page_r_id;
  InternalPage *page_r = reinterpret_cast<InternalPage *>(bpm_->NewPage(&page_r_id)->GetData());
  // page_r->latch_.WLock();
  page_r->Init(page_r_id, page->GetParentPageId(), leaf_max_size_);
  int mid = page->GetMinSize();
  for (int i = mid, j = 0; i < page->GetSize(); i++, j++) {
    page_r->array_[j] = page->array_[i];
  }
  page->SetSize(mid);
  page_r->SetSize(page->GetMaxSize() - mid);
  ChangeParentOfChildrenIn(page_r);

  if (page->IsRootPage()) {
    InsertInNewRoot(page->KeyAt(0), page, page_r->KeyAt(0), page_r);
  } else {
    InternalPage *parent_page =
        reinterpret_cast<InternalPage *>(bpm_->FetchPage(page_r->GetParentPageId())->GetData());
    InsertInInternalPage(parent_page, page_r->KeyAt(0), page_r->GetPageId());
    bpm_->UnpinPage(parent_page->GetPageId(), true);
  }
  bpm_->UnpinPage(page_r->GetPageId(), true);
  // page_r->latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInNewRoot(const KeyType &key, BPlusTreePage *page, const KeyType &key_r,
                                     BPlusTreePage *page_r) {
  page_id_t root_page_id;
  InternalPage *root_page = reinterpret_cast<InternalPage *>(bpm_->NewPage(&root_page_id)->GetData());
  root_page->Init(root_page_id, INVALID_PAGE_ID, internal_max_size_);
  page->SetParentPageId(root_page_id);
  page_r->SetParentPageId(root_page_id);
  root_page->Insert(key, page->GetPageId(), comparator_);
  root_page->Insert(key_r, page_r->GetPageId(), comparator_);
  root_page_id_ = root_page_id;
  UpdateRootPageId(0);
  bpm_->UnpinPage(root_page_id, true);
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
// std::cout << "Remove: " << key << std::endl;
  std::list<BPlusTreePage *> locked_list;
  LeafPage *leaf_page = Find(key, Operation::REMOVE, &locked_list);
  if (leaf_page == nullptr) {
    UnlockPageList(&locked_list, false, Operation::REMOVE );
    return;
  }

  int i = leaf_page->IndexOfKey(key, comparator_);
  if (i == -1) {
    UnlockPageList(&locked_list, false, Operation::REMOVE);
    return;
  }
  RemoveInLeafPage(leaf_page, i, key);
  UnlockPageList(&locked_list, true, Operation::REMOVE);
  return;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInLeafPage(LeafPage *m_page, int index, const KeyType &key) {
  LeafPage *l_page = nullptr, *r_page = nullptr;
  m_page->RemoveAt(index);
  if (m_page->IsRootPage()) {
    return;
  }
  page_id_t parent_page_id = m_page->GetParentPageId();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(bpm_->FetchPage(parent_page_id)->GetData());
  int indexOfMPage = parent_page->IndexOfKey(key, comparator_);
  int min = m_page->GetMinSize();

  if (m_page->GetSize() >= min) {
    parent_page->SetKeyAt(indexOfMPage, m_page->KeyAt(0));
    bpm_->UnpinPage(parent_page_id, true);
    return;
  }

  if (indexOfMPage + 1 < parent_page->GetSize()) {
    r_page = reinterpret_cast<LeafPage *>(bpm_->FetchPage(parent_page->ValueAt(indexOfMPage + 1))->GetData());
    r_page->latch_.WLock();
  }
  if (indexOfMPage - 1 >= 0) {
    l_page = reinterpret_cast<LeafPage *>(bpm_->FetchPage(parent_page->ValueAt(indexOfMPage - 1))->GetData());
    l_page->latch_.WLock();
  }

  if (l_page != nullptr && l_page->GetSize() > min) {
    // borrow from left
    m_page->Insert(l_page->At(l_page->GetSize() - 1), comparator_);
    l_page->RemoveAt(l_page->GetSize() - 1);
    parent_page->SetKeyAt(indexOfMPage, m_page->KeyAt(0));

    l_page->latch_.WUnlock();
    bpm_->UnpinPage(l_page->GetPageId(), true);
    if(r_page != nullptr ) {
      r_page->latch_.WUnlock();
      bpm_->UnpinPage(r_page->GetPageId(), false);
    }
     
  } else if (r_page != nullptr && r_page->GetSize() > min) {
    // borrow from right
    m_page->Insert(r_page->At(0), comparator_);
    r_page->RemoveAt(0);
    // if the original minest key was the one which is removed
    parent_page->SetKeyAt(indexOfMPage, m_page->KeyAt(0));
    parent_page->SetKeyAt(indexOfMPage + 1, r_page->KeyAt(0));

    if(l_page != nullptr ) {
      l_page->latch_.WUnlock();
      bpm_->UnpinPage(l_page->GetPageId(), false);
    }
    r_page->latch_.WUnlock();
    bpm_->UnpinPage(r_page->GetPageId(), true);
  } else {
    // Coalesce
    if (l_page != nullptr) {
      l_page->Coalesce(m_page, comparator_);
      l_page->SetNextPageId(m_page->GetNextPageId());

      l_page->latch_.WUnlock();
      bpm_->UnpinPage(l_page->GetPageId(), true);
      if(r_page != nullptr ) {
        r_page->latch_.WUnlock();
        bpm_->UnpinPage(r_page->GetPageId(), false);
      }

      RemoveInInternalPage(parent_page, indexOfMPage, key);
      bpm_->DeletePage(m_page->GetPageId());
    } else if (r_page != nullptr) {
      // l_page == null
      r_page->Coalesce(m_page, comparator_);
      // if the original minest key was the one which is removed
      parent_page->SetKeyAt(indexOfMPage+1, r_page->KeyAt(0));

       
      r_page->latch_.WUnlock();
      bpm_->UnpinPage(r_page->GetPageId(), false);
      RemoveInInternalPage(parent_page, indexOfMPage , key);
      bpm_->DeletePage(m_page->GetPageId());
      
    }
  }
  bpm_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInInternalPage(InternalPage *m_page, int index, const KeyType &key) {
  InternalPage *l_page = nullptr, *r_page = nullptr;
  m_page->RemoveAt(index);
  if (m_page->IsRootPage()) {
    if (m_page->GetSize() == 1) {
      page_id_t old_root_page_id = root_page_id_;
      root_page_id_ = m_page->ValueAt(0);
      // bpm_->UnpinPage(old_root_page_id, false);
      bpm_->DeletePage(old_root_page_id);
      BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(root_page_id_)->GetData());
      root_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      bpm_->UnpinPage(root_page_id_, true);
    }
    return;
  }

  page_id_t parent_page_id = m_page->GetParentPageId();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(bpm_->FetchPage(parent_page_id)->GetData());
  int indexOfMPage = parent_page->IndexOfKey(key, comparator_);
  int min = m_page->GetMinSize();

  if (m_page->GetSize() >= min) {
    parent_page->SetKeyAt(indexOfMPage, m_page->KeyAt(0));
    bpm_->UnpinPage(parent_page_id, true);
    return;
  }

  if (indexOfMPage + 1 < parent_page->GetSize()) {
    r_page = reinterpret_cast<InternalPage *>( bpm_->FetchPage(parent_page->ValueAt(indexOfMPage + 1))->GetData());
    r_page->latch_.WLock();
  }
  if (indexOfMPage - 1 >= 0) {
    l_page = reinterpret_cast<InternalPage *>( bpm_->FetchPage(parent_page->ValueAt(indexOfMPage - 1))->GetData());
    l_page->latch_.WLock();
  }

  if (l_page != nullptr && l_page->GetSize() > min) {
    // borrow from left
    m_page->Insert(l_page->At(l_page->GetSize() - 1), comparator_);
    l_page->RemoveAt(l_page->GetSize() - 1);
    parent_page->SetKeyAt(indexOfMPage, m_page->KeyAt(0));

    l_page->latch_.WUnlock();
    bpm_->UnpinPage(l_page->GetPageId(), true);
    if(r_page != nullptr ){
      r_page->latch_.WUnlock();
      bpm_->UnpinPage(r_page->GetPageId(), false);
    }
      
  } else if (r_page != nullptr && r_page->GetSize() > min) {
    // borrow from right
    m_page->Insert(r_page->At(0), comparator_);
    r_page->RemoveAt(0);
    // if the original minest key was the one which is removed
    parent_page->SetKeyAt(indexOfMPage, m_page->KeyAt(0));
    parent_page->SetKeyAt(indexOfMPage + 1, r_page->KeyAt(0));

    if(l_page != nullptr ) {
      l_page->latch_.WUnlock();
      bpm_->UnpinPage(l_page->GetPageId(), false);
    }
    r_page->latch_.WUnlock();
    bpm_->UnpinPage(r_page->GetPageId(), true);
  } else {
    // Coalesce
    if (l_page != nullptr) {
      l_page->Coalesce(m_page, comparator_);
      ChangeParentOfChildrenIn(l_page);

      l_page->latch_.WUnlock();
      bpm_->UnpinPage(l_page->GetPageId(), true);
      if(r_page != nullptr ) {
        r_page->latch_.WUnlock();
        bpm_->UnpinPage(r_page->GetPageId(), false);
      }

      RemoveInInternalPage(parent_page, indexOfMPage, key);
      bpm_->DeletePage(m_page->GetPageId());
    } else if (r_page != nullptr) {
      // l_page == nullptr
      r_page->Coalesce(m_page, comparator_);
      // if the original minest key was the one which is removed
      parent_page->SetKeyAt(indexOfMPage+1, r_page->KeyAt(0));
      ChangeParentOfChildrenIn(r_page);
      
      r_page->latch_.WUnlock();
      bpm_->UnpinPage(r_page->GetPageId(), false);
      
      RemoveInInternalPage(parent_page, indexOfMPage , key);
      bpm_->DeletePage(m_page->GetPageId());
      
    }
  }
  bpm_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ChangeParentOfChildrenIn(InternalPage *page) {
  page_id_t page_id = page->GetPageId();
  for (int i = 0; i < page->GetSize(); i++) {
    BPlusTreePage *page_tmp =
        reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(page->ValueAt(i))->GetData());
    page_tmp->SetParentPageId(page_id);
    bpm_->UnpinPage(page_tmp->GetPageId(), true);
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
  BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(root_page_id_)->GetData());
  while (!cur_page->IsLeafPage()) {
    InternalPage *cur_inter_page = static_cast<InternalPage *>(cur_page);
    bpm_->UnpinPage(cur_page->GetPageId(), false);
    cur_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(cur_inter_page->ValueAt(0))->GetData());
  }
  return INDEXITERATOR_TYPE(static_cast<LeafPage *>(cur_page), bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::list<BPlusTreePage *> locked_list;
  auto *leaf_page = Find(key, Operation::FIND, &locked_list);
  int i = leaf_page->IndexOfKey(key, comparator_);
  UnlockPageList(&locked_list, false, Operation::FIND);

  return INDEXITERATOR_TYPE(leaf_page, bpm_, i == -1 ? 0 : i);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(root_page_id_)->GetData());
  while (!cur_page->IsLeafPage()) {
    InternalPage *cur_inter_page = static_cast<InternalPage *>(cur_page);
    bpm_->UnpinPage(cur_page->GetPageId(), false);
    cur_page = reinterpret_cast<BPlusTreePage *>( bpm_->FetchPage(cur_inter_page->ValueAt(cur_inter_page->GetSize() - 1))->GetData());
  }
  return INDEXITERATOR_TYPE(static_cast<LeafPage *>(cur_page), bpm_, cur_page->GetSize());
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
  auto *header_page = static_cast<HeaderPage *>(bpm_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  bpm_->UnpinPage(HEADER_PAGE_ID, true);
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
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Check() const{
  if (IsEmpty()) {
    LOG_WARN("Check an empty tree");
    return;
  }
  Check_(reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(root_page_id_)->GetData()));
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Check_(BPlusTreePage *page) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    if(!page->IsRootPage()){
      BUSTUB_EXPECT(leaf->GetSize() >= leaf->GetMinSize() && leaf->GetSize() < leaf->GetMaxSize(), "Invalide page_size");
    }
    for (int i = 1; i < leaf->GetSize(); i++) {
      BUSTUB_EXPECT(comparator_(leaf->KeyAt(i), leaf->KeyAt(i-1)) >= 0, "Invalide key order");
    }
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    if(!page->IsRootPage()){
      BUSTUB_EXPECT(internal->GetSize() >= internal->GetMinSize() 
        && internal->GetSize() < internal->GetMaxSize(), 
        "Invalide page_size");
    }
    
    for (int i = 2; i < internal->GetSize(); i++) {
      BUSTUB_EXPECT(comparator_(internal->KeyAt(i), internal->KeyAt(i-1)) >= 0, "Invalide key order");
    }
    for (int i = 0; i < internal->GetSize(); i++) {
      Check_(reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(internal->ValueAt(i))->GetData()));
    }
  }
  bpm_->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
