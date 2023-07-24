//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <random>
#include <string>
#include <set>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "common/util/tasks_util.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, DISABLED_BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, DISABLED_SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}



TEST(BufferPoolManagerInstanceTest, NewAndNew) {
  const std::string db_name = "test.db";
  const size_t pool_size = 20;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(pool_size, disk_manager, k);

  std::mutex page_ids_mutex;
  std::vector<page_id_t> page_ids;

  std::mutex console_mutex;
 
  TasksUtil t(32);

   
  t.addTask([&](size_t from, size_t to){
    // std::cout << "newpage task 1: from = " << from << ", to = " << to << std::endl;
    for (size_t i = from; i < to; i++) {
      page_id_t page_id;
      auto *page = bpm->NewPage(&page_id);
      ASSERT_NE(nullptr, page);
      snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "Hello %d" , page_id);
      // EXPECT_EQ(0, strcmp(page->GetData(), "Hello %d", page_id));
      page_ids_mutex.lock();
      page_ids.push_back(page_id);
      page_ids_mutex.unlock();
      bpm->UnpinPage(page_id, true);
    }
  }, 4, pool_size*2);
  

  t.addTask([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "newpage task 2: from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    for (size_t i = from; i < to; i++) {
      page_id_t page_id;
      auto *page = bpm->NewPage(&page_id);
      ASSERT_NE(nullptr, page);
      snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "Hello %d" , page_id);
      // EXPECT_EQ(0, strcmp(page->GetData(), "Hello %d", page_id));
      bpm->UnpinPage(page_id, true);
    }
  }, 4, pool_size*2);

   

  t.run();

  Page* frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // if(frame.GetPinCount() != 0){
      // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
    // }
    
  }

   
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, FetchAndFetch) {
  const std::string db_name = "test.db";
  const size_t pool_size = 20;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(pool_size, disk_manager, k);

  std::mutex page_ids_mutex;
  std::vector<page_id_t> page_ids;

  std::mutex console_mutex;
 
  TasksUtil t(32);

  std::vector<TaskID> dep1;
  TaskID task_id = t.addTask([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "newpage task 1: from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    for (size_t i = from; i < to; i++) {
      page_id_t page_id;
      auto *page = bpm->NewPage(&page_id);
      ASSERT_NE(nullptr, page);
      snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "Hello %d" , page_id);
      // EXPECT_EQ(0, strcmp(page->GetData(), "Hello %d", page_id));
      page_ids_mutex.lock();
      page_ids.push_back(page_id);
      page_ids_mutex.unlock();
      bpm->UnpinPage(page_id, true);
    }
  }, 4, pool_size*2);
  dep1.push_back(task_id);

 
  // std::list<TaskID> dep2;
   t.addTaskWithDeps([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "fetch task 1 : from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    EXPECT_EQ(page_ids.size(), pool_size*2);
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
      ASSERT_NE(nullptr, page);
      // ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      char str[BUSTUB_PAGE_SIZE];
      snprintf(str, BUSTUB_PAGE_SIZE, "Hello %d" , page->GetPageId());
      EXPECT_EQ(0, strcmp(page->GetData(), str));
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*2, dep1);

  t.addTaskWithDeps([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "fetch task 2 : from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    EXPECT_EQ(page_ids.size(), pool_size*2);
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
      ASSERT_NE(nullptr, page);
      // ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      char str[BUSTUB_PAGE_SIZE];
      snprintf(str, BUSTUB_PAGE_SIZE, "Hello %d" , page->GetPageId());
      EXPECT_EQ(0, strcmp(page->GetData(), str));
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*2, dep1);
  

  t.run();

  Page* frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // if(frame.GetPinCount() != 0){
      std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
    // }
    
  }

   
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, FetchAndDelete) {
  const std::string db_name = "test.db";
  const size_t pool_size = 20;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(pool_size, disk_manager, k);

  std::mutex page_ids_mutex;
  std::vector<page_id_t> page_ids;

  std::mutex console_mutex;
 
  TasksUtil t(32);

  std::vector<TaskID> dep1;
  TaskID task_id = t.addTask([&](size_t from, size_t to){
    // std::cout << "newpage task 1: from = " << from << ", to = " << to << std::endl;
    for (size_t i = from; i < to; i++) {
      page_id_t page_id;
      auto *page = bpm->NewPage(&page_id);
      ASSERT_NE(nullptr, page);
      snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "Hello %d" , page_id);
      // EXPECT_EQ(0, strcmp(page->GetData(), "Hello %d", page_id));
      page_ids_mutex.lock();
      page_ids.push_back(page_id);
      page_ids_mutex.unlock();
      bpm->UnpinPage(page_id, true);
    }
  }, 4, pool_size*2);
  dep1.push_back(task_id);

 
  t.addTaskWithDeps([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "delete task : from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    EXPECT_EQ(page_ids.size(), pool_size*2);
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
      ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      char str[BUSTUB_PAGE_SIZE];
      snprintf(str, BUSTUB_PAGE_SIZE, "Hello %d" , page->GetPageId());
      EXPECT_EQ(0, strcmp(page->GetData(), str));
      bpm->DeletePage(page_id);
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*2, dep1);


  t.addTaskWithDeps([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "fetch task 2 : from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    EXPECT_EQ(page_ids.size(), pool_size*2);
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
      ASSERT_NE(nullptr, page);
      // ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      // char str[BUSTUB_PAGE_SIZE];
      // snprintf(str, BUSTUB_PAGE_SIZE, "Hello %d" , page->GetPageId());
      // EXPECT_EQ(0, strcmp(page->GetData(), str));
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*2, dep1);

  t.run();

  Page* frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // if(frame.GetPinCount() != 0){
      // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
    // }
    
  }

   
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, New_Fetch_Delete) {
  const std::string db_name = "test.db";
  const size_t pool_size = 100;
  const size_t scale = 100;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(pool_size, disk_manager, k);

  std::mutex page_ids_mutex;
  std::vector<page_id_t> page_ids;

  std::mutex console_mutex;
 
  TasksUtil t(32);

  std::vector<TaskID> dep1;
  TaskID task_id = t.addTask([&](size_t from, size_t to){
    std::cout << "newpage task 1: from = " << from << ", to = " << to << std::endl;
    for (size_t i = from; i < to; i++) {
      page_id_t page_id;
      auto *page = bpm->NewPage(&page_id);
      ASSERT_NE(nullptr, page);
      snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "Hello %d" , page_id);
      // EXPECT_EQ(0, strcmp(page->GetData(), "Hello %d", page_id));
      page_ids_mutex.lock();
      page_ids.push_back(page_id);
      page_ids_mutex.unlock();
      bpm->UnpinPage(page_id, true);
    }
  }, 4, pool_size*scale);
  dep1.push_back(task_id);

 
   std::vector<TaskID> dep2;
   task_id = t.addTaskWithDeps([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "fetch task 1 : from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    EXPECT_EQ(page_ids.size(), pool_size*scale);
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
      
      ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      char str[BUSTUB_PAGE_SIZE];
      snprintf(str, BUSTUB_PAGE_SIZE, "Hello %d" , page->GetPageId());
      EXPECT_EQ(0, strcmp(page->GetData(), str));
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*scale, dep1);
  dep2.push_back(task_id);

  task_id = t.addTaskWithDeps([&](size_t from, size_t to){
    // std::cout << "fetch task 2 : from = " << from << ", to = " << to << std::endl;
    EXPECT_EQ(page_ids.size(), pool_size*scale);
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
       
      ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      char str[BUSTUB_PAGE_SIZE];
      snprintf(str, BUSTUB_PAGE_SIZE, "Hello %d" , page->GetPageId());
      EXPECT_EQ(0, strcmp(page->GetData(), str));
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*scale, dep1);
  dep2.push_back(task_id);
  

  t.addTaskWithDeps([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "delete task : from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    for (size_t i = from; i < to; i++) {
      page_id_t page_id = page_ids[i];

      auto *page = bpm->FetchPage(page_id);
      ASSERT_NE(nullptr, page);
      EXPECT_EQ(page_id, page->GetPageId());
      bpm->DeletePage(page_id);
      bpm->UnpinPage(page->GetPageId(), true);
    }
  }, 4, pool_size*scale, dep2);

  t.addTask([&](size_t from, size_t to){
    // console_mutex.lock();
    // std::cout << "newpage task 2: from = " << from << ", to = " << to << std::endl;
    // console_mutex.unlock();
    for (size_t i = from; i < to; i++) {
      page_id_t page_id;
      auto *page = bpm->NewPage(&page_id);
      ASSERT_NE(nullptr, page);
      snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "Hello %d" , page_id);
      // EXPECT_EQ(0, strcmp(page->GetData(), "Hello %d", page_id));
      bpm->UnpinPage(page_id, true);
    }
  }, 4, pool_size*scale);

   

  t.run();

  Page* frames = bpm->GetFrames();
  for(size_t i = 0; i < pool_size; i++) {
    Page &frame = frames[i];
    EXPECT_EQ(frame.GetPinCount(), 0);
    // if(frame.GetPinCount() != 0){
      // std::cout << "frame_id: " << i << ", page_id: " <<  frame.GetPageId() << ", pin_count: " << frame.GetPinCount() << std::endl;
    // }
    
  }

   
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
