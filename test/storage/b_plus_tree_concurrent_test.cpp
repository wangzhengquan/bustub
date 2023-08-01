//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <thread>  // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT
#include "common/util/tasks_util.h"
#include <filesystem>
namespace fs = std::filesystem;

namespace bustub {

  std::mutex console_mutex;

class FramesCheck {

  BufferPoolManager *bpm ;
  size_t pool_size;

public:
  FramesCheck(BufferPoolManager *bpm_, size_t pool_size_ ) : bpm(bpm_), pool_size(pool_size_){}

  bool operator() (){
    bool suc = true;
    Page* frames = bpm->GetFrames();
    for(size_t i = 0; i < pool_size; i++) {
      Page &frame = frames[i];
      // EXPECT_EQ(frame.GetPinCount(), 0);
      if(frame.GetPinCount() != 0){
        std::cout << "Expect PinCount==0 but not, frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
        suc = false;
      }
    }
    
    return suc;
  }

};

class InsertSucCheck{
  
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> &tree;
  std::set<int>& keys;
public:
  InsertSucCheck( BPlusTree<GenericKey<8>, RID, GenericComparator<8>>  &tree_, std::set<int>& keys_):
   tree(tree_), keys(keys_){

  }
  bool operator() () {
    bool suc = true;
    GenericKey<8> index_key;
    std::vector<RID> rids;
    for (int key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      bool exist = tree.GetValue(index_key, &rids);
      
      if(!exist){
        std::cout << "Expect found key " << key << " , but not" << std::endl;
        suc = false;
        continue;
      }

      int value = rids[0].GetSlotNum();
      if(value != key){
        std::cout << "Expect value = "<< key  << ",but it was " << value  << std::endl;
        suc = false;
      }

    }

    size_t size = 0;
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      ++size;
    }

    if(size != keys.size()){
      std::cout << "Tree's size should be  " << keys.size()  
        << " , but it was " << size << std::endl;
      suc = false;
    }
    
    
    return suc;
  }

};


class DeleteSucCheck{
  
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> &tree;
  std::set<int>& keys;
  std::set<int>& keys_deleted;

public:
  DeleteSucCheck( BPlusTree<GenericKey<8>, RID, GenericComparator<8>>  &tree_, std::set<int>& keys_, std::set<int>& keys_deleted_):
   tree(tree_), keys(keys_), keys_deleted(keys_deleted_){

  }
  bool operator() () {
    bool suc = true;
    GenericKey<8> index_key;
    std::vector<RID> rids;
    for (int key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      bool exist = tree.GetValue(index_key, &rids);
      if(keys_deleted.find(key) == keys_deleted.end()){
        // not deleted
        if(!exist){
          std::cout << "Expect found not deleted" << key << " but not" << std::endl;
          suc = false;
          continue;
        }

        int value = rids[0].GetSlotNum();
        if(value != key){
          std::cout << "Expect value = "<< key << ",but it was " << value  << std::endl;
          suc = false;
        }

      } else{
        // deleted
        if(exist){
          std::cout << "Expect deleted " << key << " but not" << std::endl;
          suc = false;
        }
      }
      
    }

    size_t size = 0;
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      ++size;
    }
   
    if(size != keys.size() - keys_deleted.size()){
      std::cout << "tree's size should be " << keys.size() - keys_deleted.size() 
         << ", but it was " << size << std::endl;
      suc = false;
    }
    
    return suc;
  }

};

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr, num_threads));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}
std::mutex mutex_;
// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_id = 0, uint64_t num_threads = 1) {
  GenericKey<8> index_key;
  RID rid;
  size_t num_elements = keys.size();
  size_t elements_per_task = (num_elements + num_threads - 1) / num_threads;
  size_t start = elements_per_task * thread_id;
  size_t end = std::min(start + elements_per_task, num_elements);
  // create transaction
  auto *transaction = new Transaction(0);
  for (size_t i = start; i < end; i++) {
    int64_t key = keys[i];
    // mutex_.lock();
    // std::cout << "thread " << thread_id << " insert " << key << std::endl;
    // mutex_.unlock();

    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }

  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr,
                       __attribute__((unused)) uint64_t num_threads) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_id = 0, uint64_t num_threads = 1) {
  GenericKey<8> index_key;
  size_t num_elements = keys.size();
  size_t elements_per_task = (num_elements + num_threads - 1) / num_threads;
  size_t start = elements_per_task * thread_id;
  size_t end = std::min(start + elements_per_task, num_elements);
 
  // create transaction
  auto *transaction = new Transaction(0);
  for (size_t i = start; i < end; i++) {
    auto & key = keys[i];
    //  mutex_.lock();
    // std::cout << "thread " << thread_id << " delete " << key << std::endl;
    // mutex_.unlock();
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr, __attribute__((unused)) uint64_t num_threads) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_MixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

 
TEST(BPlusTreeConcurrentTest, DISABLED_ConcurrentInsert1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size = 50;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);

  int64_t num = 100;
  std::vector<int64_t> keys;
  for (int64_t i = 0; i < num; i++) {
    keys.push_back(i);
  }
  // concurrent insert
  LaunchParallelTest(4, InsertHelper, &tree, keys);

  EXPECT_EQ(tree.Check(), true);
  
  int64_t size = 0;
  GenericKey<8> pre ;
  pre.SetFromInteger(-1);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto & cur = iterator->first;
    EXPECT_GT(comparator(cur, pre), 0);
    std::cout << cur << " ";
    pre = cur;
    size = size + 1;
  }
  std::cout << std::endl;
  EXPECT_EQ(size, num);

  tree.Draw(bpm, "tree-ConcurrentInsert.dot");
  // tree.Print(bpm);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  
  Page* frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
  }
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


 

//DISABLED_
TEST(BPlusTreeConcurrentTest, DISABLED_RandomInsertThenDelete_1) {

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size = 100;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  
  // create transaction
  auto *transaction = new Transaction(0);

  TasksUtil t(8);

  int scale = 10000;

  std::srand(std::time(nullptr));

  fs::remove_all("output");
  fs::create_directory("output");
  
  std::set<int> keys;
  
  // ================= insert ========================

  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = std::rand() % scale;
        GenericKey<8> index_key;
        RID rid;
        rid.Set(key, key);
        index_key.SetFromInteger(key);
        
        if(tree.Insert(index_key, rid, transaction)){
          console_mutex.lock();
          keys.insert(key);
          console_mutex.unlock();
        }

      }
        
  }, 4, scale);
  
  t.run();

  // ----check ----

  // std::cout << "inserted keys:";
  // for(int key : keys){
  //   std::cout << key << " ";
  // }
  // std::cout << std::endl;

  tree.Draw(bpm, "output/tree.dot");
  EXPECT_EQ(tree.Check(), true);
  EXPECT_EQ(InsertSucCheck(tree, keys)(), true);
  // tree.Print(bpm);

  // =========== delete ================================

  std::set<int> keys_deleted;
  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = std::rand() % scale;;
        GenericKey<8> index_key;
        index_key.SetFromInteger(key);
        if(tree.Remove(index_key, transaction)){
          console_mutex.lock();
          keys_deleted.insert(key);
          console_mutex.unlock();
        }
      }
        
  }, 4, scale);
  t.run();

  // ----check ----
  tree.Draw(bpm, "output/tree2.dot");
  EXPECT_EQ(tree.Check(), true);
  EXPECT_EQ(DeleteSucCheck(tree, keys, keys_deleted)(), true);

  
  // ======== end ===================================

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  EXPECT_EQ(FramesCheck(bpm, pool_size)(), true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
   
}

//DISABLED_
TEST(BPlusTreeConcurrentTest, RandomInsertThenDelete_2) {

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size = 100;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  
  // create transaction
  auto *transaction = new Transaction(0);

  TasksUtil t(8);

  int scale = 10000;

  std::srand(std::time(nullptr));

  fs::remove_all("output");
  fs::create_directory("output");

  std::set<int> keys_set;
  for(int i = 0; i < scale; i++){
    int key = std::rand() % scale;
    keys_set.insert(key);
  }

  std::vector<int> keys;
  for(int key : keys_set){
    keys.push_back(key);
  }
  
  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = keys[i];
        GenericKey<8> index_key;
        RID rid;
        rid.Set(key, key);
        index_key.SetFromInteger(key);
        
        EXPECT_EQ(tree.Insert(index_key, rid, transaction), true);
      }
        
  }, 4, keys.size());
  
  t.run();

  // ========== check =======================================

  // std::cout << "inserted keys:";
  // for(int key : keys){
  //   std::cout << key << " ";
  // }
  // std::cout << std::endl;

  tree.Draw(bpm, "output/tree.dot");
  EXPECT_EQ(tree.Check(), true);
  EXPECT_EQ(InsertSucCheck(tree, keys_set)(), true);
  // tree.Print(bpm);
   
  // ================delete ===================================

  std::set<int> keys_deleted;
  // t.addTask([&](size_t from, size_t to){
  //     for (size_t i = from; i < to; i++) {
  //       int key = std::rand() % scale;;
  //       GenericKey<8> index_key;
  //       index_key.SetFromInteger(key);
  //       if(tree.Remove(index_key, transaction)){
  //         console_mutex.lock();
  //         keys_deleted.insert(key);
  //         console_mutex.unlock();
  //       }
  //     }
        
  // }, 4, scale);

  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        if(i %2 == 0){
          continue;
        }
        int key = keys[i];
        GenericKey<8> index_key;
        index_key.SetFromInteger(key);
        EXPECT_EQ(tree.Remove(index_key, transaction), true);

        console_mutex.lock();
        keys_deleted.insert(key);
        console_mutex.unlock();
      }
        
  }, 4, keys.size());

  t.run();

  // ---- check ----

  tree.Draw(bpm, "tree3.dot");
  EXPECT_EQ(tree.Check(), true);
  EXPECT_EQ(DeleteSucCheck(tree, keys_set, keys_deleted)(), true);
  
  // ============== end ===========================================
  bpm->UnpinPage(HEADER_PAGE_ID, true);

  EXPECT_EQ(FramesCheck(bpm, pool_size)(), true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
   
}



//DISABLED_

TEST(BPlusTreeConcurrentTest, RandomInsertAndDelete) {

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size = 1000;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  
  // create transaction
  auto *transaction = new Transaction(0);

  TasksUtil t(8);

  int scale = 10000;

  std::srand(std::time(nullptr));

  fs::remove_all("output");
  fs::create_directory("output");

   
  // ---- insert ---

  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = std::rand() % scale;
        GenericKey<8> index_key;
        RID rid;
        rid.Set(key, key);
        index_key.SetFromInteger(key);
        
        tree.Insert(index_key, rid, transaction);
      }
        
  }, 4, scale);

  // t.run();
  // ASSERT_EQ(tree.Check(), true);

  // --- delete ----

  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = std::rand() % scale;
        GenericKey<8> index_key;
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
      }
        
  }, 4, scale);
  
  t.run();


  // ========== check ===============
  tree.Draw(bpm, "output/tree2.dot");
  EXPECT_EQ(tree.Check(), true);
  // tree.Print(bpm);
   

  int size = 0;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    ++size;
  }
  std::cout << "\nsize = " << size << std::endl;

  // ======== end =======

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  EXPECT_EQ(FramesCheck(bpm, pool_size)(), true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
   
}



 

}  // namespace bustub
