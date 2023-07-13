/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "common/util/task_util.h"

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

TEST(ExtendibleHashTableTest, MyTest1) {
  
  std::cout << "====Mytest" << std::endl;
  // 16,4,6,22,24,10,31,7,9,20,26.
  ExtendibleHashTable<int, int> table(4);
  std::vector<int> keys;
  for(int i=0; i<20; i++){
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &table, keys);
  table.Show();
  int result;
  table.Find(10, result);
  EXPECT_EQ(10, result);
}

class InsertTask  {
private:
  // std::shared_mutex mutex_;
  ExtendibleHashTable<int, int> *table_;
  std::vector<int> &keys_;
public:
    
    // InsertTask(InsertTask & other): table_(other.table_), keys_(other.keys_){
    // }

    InsertTask(ExtendibleHashTable<int, int> *table,  std::vector<int> &keys)
        : table_(table), keys_(keys) {}
    ~InsertTask() {}

    static inline int multiply_task(int iters, int input) {
        int accumulator = 1;
        for (int i = 0; i < iters; ++i) {
            accumulator *= input;
        }
        return accumulator;
    }

    void operator()(int task_id, int num_total_tasks) {
      size_t num_elements = keys_.size();
      size_t elements_per_task = (num_elements + num_total_tasks - 1) / num_total_tasks;
      size_t start = elements_per_task * task_id;
      size_t end = std::min(start + elements_per_task, num_elements);
      for (size_t i = start; i < end; i++) {
        int key = keys_[i];
        // std::unique_lock<std::shared_mutex> lock(mutex_);
        std::cout << "insert " << key << std::endl;
        // lock.unlock();
        table_->Insert(key, key);
      }
    }
};

class DeleteTask  {
  // std::shared_mutex mutex_;
  ExtendibleHashTable<int, int> *table_;
  std::vector<int> &keys_;
public:
    // DeleteTask(DeleteTask & other): table_(other.table_), keys_(other.keys_){
    // }
    DeleteTask(ExtendibleHashTable<int, int> *table, std::vector<int> &keys)
        : table_(table), keys_(keys) {}
    ~DeleteTask() {}

    static inline int multiply_task(int iters, int input) {
        int accumulator = 1;
        for (int i = 0; i < iters; ++i) {
            accumulator *= input;
        }
        return accumulator;
    }

    void operator()(int task_id, int num_total_tasks) {
      size_t num_elements = keys_.size();
      size_t elements_per_task = (num_elements + num_total_tasks - 1) / num_total_tasks;
      size_t start = elements_per_task * task_id;
      size_t end = std::min(start + elements_per_task, num_elements);
      // create transaction
      
      for (size_t i = start; i < end; i++) {
        int key = keys_[i];
        // std::unique_lock<std::shared_mutex> lock(mutex_);
        std::cout << "remove " << key << std::endl;
        // lock.unlock();
        table_->Remove(key);
      }
    }
};

TEST(ExtendibleHashTableTest, MyConcurrentTest2) {
  ExtendibleHashTable<int, int> table(4);
  std::vector<int> keys;
  std::vector<int> delete_keys;
  for(int i=0; i<1000; i++){
    keys.push_back(i);
  }
  for(int i=0; i<1000; i++){
    delete_keys.push_back(i);
  }
  TaskUtil t(4);
  InsertTask task1(&table, keys);
  DeleteTask task2(&table, delete_keys);
  t.run();
  t.addTask(task1, 2);
  t.addTask(task2, 2);

  t.sync();
  
  table.Show();
  int result;
  table.Find(100, result);
  EXPECT_EQ(100, result);
 // t->run(&second, num_tasks);
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

TEST(ExtendibleHashTableTest, DISABLED_ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

}  // namespace bustub
