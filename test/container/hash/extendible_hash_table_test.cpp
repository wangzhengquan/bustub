/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "common/util/tasks_util.h"

namespace bustub {
std::mutex mutex_;
template <class Function, class... Args>
void LaunchParallelTest(uint64_t num_threads, Function&& fun, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(fun, args..., thread_itr, num_threads));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(ExtendibleHashTable<int, int> *table, const std::vector<int> &keys,
                  __attribute__((unused)) uint64_t thread_id = 0, uint64_t num_threads = 1) {
   
  size_t num_elements = keys.size();
  size_t elements_per_task = (num_elements + num_threads - 1) / num_threads;
  size_t start = elements_per_task * thread_id;
  size_t end = std::min(start + elements_per_task, num_elements);
  // create transaction
  
  for (size_t i = start; i < end; i++) {
    int key = keys[i];
    // mutex_.lock();
    // std::cout << "insert " << key << std::endl;
    // mutex_.unlock();
    table->Insert(key, key);
  }
}

void DeleteHelper(ExtendibleHashTable<int, int> *table, const std::vector<int> &keys,
                  __attribute__((unused)) uint64_t thread_id = 0, uint64_t num_threads = 1) {
   
  size_t num_elements = keys.size();
  size_t elements_per_task = (num_elements + num_threads - 1) / num_threads;
  size_t start = elements_per_task * thread_id;
  size_t end = std::min(start + elements_per_task, num_elements);
  // create transaction
  
  for (size_t i = start; i < end; i++) {
    int key = keys[i];
    // mutex_.lock();
    // std::cout << "insert " << key << std::endl;
    // mutex_.unlock();
    table->Remove(key);
  }
}



TEST(ExtendibleHashTableTest, DISABLED_SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  table->Show();

  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}


 TEST(ExtendibleHashTableTest, DISABLED_MyConcurrentTest) {
  ExtendibleHashTable<int, int> table(4);
  std::cout << "====MyConcurrentTest" << std::endl;
  // 16,4,6,22,24,10,31,7,9,20,26.
  std::vector<int> keys;
  std::vector<int> delete_keys={10, 12, 13, 15, 8, 9, 1, 3};
  for(int i=0; i<1000; i++){
    keys.push_back(i);
  }
  LaunchParallelTest(4, InsertHelper, &table, keys);
  LaunchParallelTest(4, DeleteHelper, &table, delete_keys);

  table.Show();
  int result;
  table.Find(2, result);
  EXPECT_EQ(2, result);
  EXPECT_EQ(table.Find(9, result), false);
   
}

class Check{
  std::set<int>& keys_inserted;
  ExtendibleHashTable<int, int>  &table;
public:
  Check(std::set<int>& keys_, ExtendibleHashTable<int, int> &table_):
   keys_inserted(keys_), table(table_){

  }
  bool operator() () {
    bool suc = true;
    for(int key : keys_inserted){
      int value = 0;
      bool exist = table.Find(key, value);
      if(!exist){
        std::cout << "indexof " << key << " = " << table.IndexOf(key) << std::endl;
        suc = false;
      }
      if(value != key){
        std::cout << "key= " << key << " , value= " << value << std::endl;
        suc = false;

      }
     
      // EXPECT_EQ(exist, true);
      // EXPECT_EQ(value, key);
      
    }
    return suc;
  }

};


TEST(ExtendibleHashTableTest, DISABLED_AscentInterleaveInsert) {
  ExtendibleHashTable<int, int> table(4);
  std::set<int> keys_inserted;
   
  int scale = 10;

  for (int i = 0; i < scale; i++) {
    if(i%2 == 1){
      int key = static_cast<int>(i);
      
      table.Insert(key, key);
      keys_inserted.insert(key);
      
      table.Show();
      ASSERT_EQ(table.Check(), true);
      ASSERT_EQ(Check(keys_inserted, table)(), true);
      ASSERT_EQ(keys_inserted.size(), table.GetSize());
      
    }
   
  }

  
  // table.Show();

  EXPECT_EQ(keys_inserted.size(), table.GetSize());
   
}

//DISABLED_
TEST(ExtendibleHashTableTest, DISABLED_DescentInterleaveInsert) {
  ExtendibleHashTable<int, int> table(4);
  std::set<int> keys_inserted;
   
  size_t scale = 100;
  for (int i = scale; i >= 0; i--) {
    if(i%2==0){
      int key = i;

      std::cout << "insesrt " << key << std::endl;
      table.Insert(key, key);
      keys_inserted.insert(key);

      table.Show();
      ASSERT_EQ(table.Check(), true);
      ASSERT_EQ(Check(keys_inserted, table)(), true);
      ASSERT_EQ(keys_inserted.size(), table.GetSize());
    }
   
  }

   
  // table.Show();

  EXPECT_EQ(keys_inserted.size(), table.GetSize());
   
}

//DISABLED_
TEST(ExtendibleHashTableTest, RandomInsert) {
  ExtendibleHashTable<int, int> table(4);
  std::set<int> keys_inserted;
   
  size_t scale = 10000;
  std::srand(std::time(nullptr));
  TasksUtil t(4);
  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = std::rand() % scale;
        table.Insert(key, key);

        std::unique_lock lock(mutex_);
        std::cout << "insert " << key << std::endl;
        keys_inserted.insert(key);
        lock.unlock();
        

        // table.Show();
        // ASSERT_EQ(table.Check(), true);
        // ASSERT_EQ(Check(keys_inserted, table)(), true);
        // ASSERT_EQ(keys_inserted.size(), table.GetSize());
      }
  }, 1, scale);
  t.run();

   
  table.Show();
  ASSERT_EQ(table.Check(), true);
  ASSERT_EQ(Check(keys_inserted, table)(), true);
  ASSERT_EQ(keys_inserted.size(), table.GetSize());

   
}


TEST(ExtendibleHashTableTest, DISABLED_MyConcurrentTest2) {
  // 16,4,6,22,24,10,31,7,9,20,26.
  ExtendibleHashTable<int, int> table(4);
  std::vector<int> keys;
  for(int i=0; i<20000; i++){
    keys.push_back(i);
  }
  TasksUtil t(8);
  
  TaskID task_id = t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = keys[i];
        // std::unique_lock<std::shared_mutex> lock(mutex_);
        // std::cout << "insert " << key << std::endl;
        // lock.unlock();
        table.Insert(key, key);
      }
  }, 2, keys.size());
  std::vector<TaskID> dep1;
  dep1.push_back(task_id);

  
  task_id = t.addTaskWithDeps([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = keys[i];
        // std::unique_lock<std::shared_mutex> lock(mutex_);
        // std::cout << "insert " << key << std::endl;
        // lock.unlock();
        EXPECT_EQ(table.Remove(key), true);
      }
  }, 2, keys.size(), dep1);

  std::vector<TaskID> dep2;
  dep2.push_back(task_id);

  task_id = t.addTaskWithDeps([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = keys[i];
        // std::unique_lock<std::shared_mutex> lock(mutex_);
        // std::cout << "insert " << key << std::endl;
        // lock.unlock();
        table.Insert(key, key);
      }
  }, 2, keys.size(), dep2);

  std::vector<TaskID> dep3;
  dep3.push_back(task_id);
  task_id = t.addTaskWithDeps([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = keys[i];
        int value;
        bool exist = table.Find(key, value);
        EXPECT_EQ(value, key);
        EXPECT_EQ(exist, true);
      }
  }, 2, keys.size(), dep3);
  t.run();
  // table.Show();

  EXPECT_EQ(keys.size(), table.GetSize());
   
}

TEST(ExtendibleHashTableTest, DISABLED_MyConcurrentTest3) {
  std::mutex mutex;
  ExtendibleHashTable<int, int> table(4);
  std::vector<int> keys;
  std::vector<int> delete_keys;
  for(int i=0; i<1000; i++){
    keys.push_back(i);
  }
  for(int i=0; i<1000; i++){
    delete_keys.push_back(i);
  }
  TasksUtil t(8);
  //InsertTask task1(&table, keys);
  // DeleteTask task2(&table, delete_keys);
 
 // t.addTask(task1, 4, keys.size());
  // t.addTask(task2, 100);

  std::set<int> deleted_keys;

  // std::vector<int> delete_keys={10, 12, 13, 15, 8, 9, 1, 3};
  t.addTask([&](size_t from, size_t to){
      for (size_t i = from; i < to; i++) {
        int key = delete_keys[i];
        // std::unique_lock<std::shared_mutex> lock(mutex_);
        // std::cout << "remove " << key << std::endl;
        // lock.unlock();
        if(table.Remove(key)){
          mutex.lock();
          deleted_keys.insert(key);
          mutex.unlock();
        }
      }
  }, 4, delete_keys.size());
  t.run();
  // t.sync();
  
  // table.Show();
  for(int key: keys){
    int value;
    bool exist = table.Find(key, value);
    if(deleted_keys.find(key) == deleted_keys.end()){
      EXPECT_EQ(value, key);
    } else {
      EXPECT_EQ(exist, false);
    }
  }
}




 

}  // namespace bustub
