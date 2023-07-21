//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdlib>
#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT


#include <filesystem>
namespace fs = std::filesystem;

namespace bustub {
//DISABLED_
TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  

  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
   
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    std::cout << iterator->first << " ";
  }
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

 

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// DISABLED_
TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


TEST(BPlusTreeTests, InsertOnAscent_DeleteOnDecent_Test) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size = 100;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t num_keys = 100;
  for (int64_t key = 0; key <= num_keys; ++key) {

    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    bool suc = tree.Insert(index_key, rid, transaction);
    // std::cout << "----insert------" << key << std::endl;
    ASSERT_EQ(suc, true);
  }
  
  int64_t num_remove_keys = 100;
  for (int64_t key = num_remove_keys; key >= 0; --key) {
    index_key.SetFromInteger(key);
    bool suc = tree.Remove(index_key, transaction);
    // std::cout << "----remove------" << key << std::endl;
    ASSERT_EQ(suc, true);
  }

  tree.Draw(bpm, "ascent.dot");
  // tree.Print(bpm);
  tree.Check();

  int64_t size = 0;
  GenericKey<8> pre ;
  pre.SetFromInteger(-1);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto & cur = iterator->first;
    EXPECT_GT(comparator(cur, pre), 0);
    std::cout << cur << " ";
    pre = cur;
    ++size;
  }
  std::cout << "\nsize = " << size << std::endl;
  EXPECT_EQ(size, num_keys - num_remove_keys);

  
  bpm->UnpinPage(HEADER_PAGE_ID, true);


  Page *frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    if(frame.GetPinCount() != 0)
      std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
  }

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

}
 

TEST(BPlusTreeTests, InsertOnDescent_DeleteOnAscent_Degree5_Test) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size=20;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  fs::remove_all("output1-1");
  fs::create_directory("output1-1");
 
  std::vector<int64_t> keys;
  for (int64_t key = 100; key > 0; key--) {
   
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    bool suc = tree.Insert(index_key, rid, transaction);
    ASSERT_EQ(suc, true);
    if(suc) {
      // std::cout << " ---- insert ----" << key << std::endl;
      keys.push_back(key);
      std::ostringstream out;
      // out << "output1-1/tree " << key << ".dot";
      // tree.Draw(bpm, out.str());
      // tree.Print(bpm);
      ASSERT_EQ(tree.Check(), true) ;
    }
    
  }

  

  fs::remove_all("output1-2");
  fs::create_directory("output1-2");
  tree.Draw(bpm, "output1-2/tree.dot");
  std::vector<int64_t> keys_removed;
 
  for (int64_t key = 1; key <= 20; ++key) {
    
    index_key.SetFromInteger(key);
    bool suc = tree.Remove(index_key, transaction);
    ASSERT_EQ(suc, true);
    if(suc){
      // std::cout << "----remove------" << key << std::endl;
      keys_removed.push_back(key);
      std::ostringstream out;
      // out << "output1-2/tree " << key << ".dot";
      // tree.Draw(bpm, out.str());
      ASSERT_EQ(tree.Check(), true);
    }
  }

  // for (int64_t key = 1; key < 20; ++key) {
  //   int64_t value = key & 0xFFFFFFFF;
  //   rid.Set(static_cast<int32_t>(key >> 32), value);
  //   index_key.SetFromInteger(key);
  //   tree.Insert(index_key, rid, transaction);
  // }

  tree.Draw(bpm, "tree-descent.dot");

  int64_t size = 0;
  GenericKey<8> pre ;
  pre.SetFromInteger(-1);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto & cur = iterator->first;
    EXPECT_GT(comparator(cur, pre), 0);
    std::cout << cur << " ";
    pre = cur;
    ++size;
  }
  std::cout << "\nsize = " << size << std::endl;
  EXPECT_EQ(size, keys.size() - keys_removed.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  Page* frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
  }

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
 

//DISABLED_
TEST(BPlusTreeTests, DeleteInterleave_Test) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  size_t pool_size = 100;
  BufferPoolManager *bpm = new BufferPoolManagerInstance(pool_size, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t num_keys = 100;
  for (int64_t key = 0; key < num_keys; ++key) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.Insert(index_key, rid, transaction), true);
  }

  fs::remove_all("output2-2");
  fs::create_directory("output2-2");
   
  std::ostringstream out;
  out << "output/tree" << ".dot";
  tree.Draw(bpm, out.str());

  int64_t num_remove_keys = 0;
  for (int64_t key = 0; key < num_keys; ++key) {
    if(key % 2 == 0 ){
      index_key.SetFromInteger(key);
      ASSERT_EQ(tree.Remove(index_key, transaction), true);
      // std::cout << "---remove--- " << key << std::endl;
      ++num_remove_keys;
      std::ostringstream out;
      // out << "output2-2/tree " << key << ".dot";
      // tree.Draw(bpm, out.str());
      ASSERT_EQ(tree.Check(), true);
       
    }
  }

  tree.Draw(bpm, "tree-DeleteInterleaved.dot");
  // tree.Print(bpm);
  // tree.Check();

  int64_t size = 0;
  GenericKey<8> pre ;
  pre.SetFromInteger(-1);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto & cur = iterator->first;
    EXPECT_GT(comparator(cur, pre), 0);
    std::cout << cur << " ";
    pre = cur;
    ++size;
  }
  std::cout << "\nsize = " << size << std::endl;
  EXPECT_EQ(size, num_keys - num_remove_keys);

  
  bpm->UnpinPage(HEADER_PAGE_ID, true);


  Page *frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
  }

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


TEST(BPlusTreeTests, RandomInsertAndDelete) {
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
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int> keys;
  std::set<int> keys_removed;
  std::srand(std::time(nullptr));
  int scale = 1000;

  for(int i = 0; i< scale; i++){
    int key = std::rand() % scale;
    rid.Set(key, key);
    index_key.SetFromInteger(key);
    if(tree.Insert(index_key, rid, transaction)){
      keys.push_back(key);
    }
  }
   

  
  for (int i = 0, end = scale*2; i < end; i++) {
    int key = std::rand() % scale;
    
    index_key.SetFromInteger(key);
    if(tree.Remove(index_key, transaction)){
      // std::cout << "---remove--- " << key << std::endl;
      keys_removed.insert(key);
    }
  }

  tree.Draw(bpm, "random.dot");
  EXPECT_EQ(tree.Check(), true);

  std::vector<RID> rids;
  for(auto key: keys){
    rids.clear();
    index_key.SetFromInteger(key);
    bool finded = tree.GetValue(index_key, &rids);
    if(keys_removed.find(key) == keys_removed.end()){
      EXPECT_EQ(finded, true);
    } else {
      EXPECT_EQ(finded, false);
    }
  }

  int size = 0;
  GenericKey<8> pre ;
  pre.SetFromInteger(-1);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto & cur = iterator->first;
    EXPECT_GT(comparator(cur, pre), 0);
    std::cout << cur << " ";
    pre = cur;
    ++size;
  }
  std::cout << "\nsize = " << size << std::endl;
  EXPECT_EQ(size, keys.size() - keys_removed.size());

  

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  Page *frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
  }
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


}  // namespace bustub
