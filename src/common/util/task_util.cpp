#include "common/util/task_util.h"
#include <unistd.h>
#include <chrono>
#include <iostream>

// IRunnable::~IRunnable() {}
/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskUtil::name() { return "Parallel + Thread Pool + Sleep"; }
TaskUtil::TaskUtil(int num_threads) : num_threads_(num_threads) {};

void TaskUtil::run(){
  if(ready_queue.state_ != State::Ready){
    return;
  }
  _threads_pool.reserve(num_threads_);

  for (int i = 0; i < num_threads_; i++) {
    _threads_pool.emplace_back([&]() {
      while (true) {
        std::unique_lock<std::mutex> ready_queue_lock(ready_queue.mutex);
        if (ready_queue.state_ == State::Terminated) {
          // std::cout << "stop "<< my_stopi << std::endl;
          my_stopi++;
          ready_queue_lock.unlock();
          break;
        }

        if (ready_queue.queue.empty()) {
          ready_queue.cv.wait(ready_queue_lock);
          ready_queue_lock.unlock();
        } else {
          std::shared_ptr<ReadyTask> ready_task = ready_queue.queue.front();
          int worker_id = ready_task->worker_id_++;
          if (worker_id >= ready_task->num_task_workers_) {
            ready_queue.queue.pop();
            ready_queue_lock.unlock();
          } else {
            ready_queue_lock.unlock();
            ready_task->runnable(worker_id, ready_task->num_task_workers_);

            std::unique_lock<std::mutex> ready_task_lock(ready_task->mutex);
            ready_task->num_workers_completed++;
            if (ready_task->num_workers_completed == ready_task->num_task_workers_) {
              ready_task_lock.unlock();

              std::unique_lock<std::mutex> ready_queue_num_tasks_complete(ready_queue.num_tasks_complete_mutex);
              ready_queue.num_tasks_complete++;
              if (ready_queue.num_tasks_complete == ready_queue.num_total_tasks) {
                ready_queue_num_tasks_complete.unlock();
                ready_queue.num_tasks_complete_cv.notify_one();
              } else {
                ready_queue_num_tasks_complete.unlock();
              }

              std::unique_lock<std::mutex> complete_queue_lock(complete_queue.mutex);
              complete_queue.queue.insert(ready_task->task_id_);
              complete_queue_lock.unlock();

              // finished bulk tasks, search the bulk  which has dependence with this bulk
              std::unique_lock<std::mutex> wait_queue_lock(wait_queue.mutex);
              for (auto wait_queue_it = wait_queue.queue.begin(); wait_queue_it != wait_queue.queue.end();) {
                WaitTask &wait_bulk = *wait_queue_it;
                for (auto it = wait_bulk.deps.begin(); it != wait_bulk.deps.end();) {
                  if (*it == ready_task->task_id_) {
                    it = wait_bulk.deps.erase(it);
                    break;
                  } else
                    ++it;
                }

                if (wait_bulk.deps.empty()) {
                  ready_queue_lock.lock();
                  ready_queue.queue.push(std::shared_ptr<ReadyTask>(
                      new ReadyTask{wait_bulk.task_id_, wait_bulk.runnable, wait_bulk.num_task_workers_}));
                  ready_queue_lock.unlock();
                  ready_queue.cv.notify_all();
                  wait_queue_it = wait_queue.queue.erase(wait_queue_it);

                } else {
                  ++wait_queue_it;
                }
              }
              wait_queue_lock.unlock();
            } else {
              ready_task_lock.unlock();
            }
          }
        }
      }
    });
  }
  sync();
}

TaskUtil::~TaskUtil() {
  // std::cout << "=========~TaskUtil"  << std::endl ;
  sync();
  std::unique_lock<std::mutex> lock(ready_queue.mutex);
  // ready_queue.stop = true;
  ready_queue.state_ = State::Terminated;
  lock.unlock();
  ready_queue.cv.notify_all();
  for (auto &thread : _threads_pool) {
    thread.join();
  }
}

void TaskUtil::addTask(std::function<void(int, int)> runnable, int num_task_workers) {
  const std::vector<TaskID> deps;
  addTaskWithDeps(runnable, num_task_workers, deps);
}
//runAsyncWithDeps
void TaskUtil::addTaskWithDeps(std::function<void(int, int)> runnable_fun, int num_task_workers, const std::vector<TaskID> &deps) {
  fun_queue_.push_back(std::move(runnable_fun));
  std::function<void(int, int)>  &runnable = fun_queue_.back();

  ready_queue.num_total_tasks++;
  std::vector<TaskID> newdeps;
  std::unique_lock<std::mutex> complete_queue_lock(complete_queue.mutex);
  for (TaskID dep : deps) {
    if (complete_queue.queue.find(dep) == complete_queue.queue.end()) {
      newdeps.push_back(dep);
    }
  }

  if (newdeps.empty()) {
    complete_queue_lock.unlock();
    std::unique_lock<std::mutex> lock(ready_queue.mutex);
    int task_id = next_task_id_++;
    ready_queue.queue.push(std::shared_ptr<ReadyTask>(new ReadyTask{task_id, runnable, num_task_workers}));
    lock.unlock();
    ready_queue.cv.notify_all();
    return ;
  } else {
    std::unique_lock<std::mutex> lock(wait_queue.mutex);
    int task_id = next_task_id_++;
    wait_queue.queue.push_back(WaitTask{task_id, runnable, num_task_workers, newdeps});

    complete_queue_lock.unlock();
    lock.unlock();
    return ;
  }
}

void TaskUtil::sync() {
  std::unique_lock<std::mutex> lock(ready_queue.num_tasks_complete_mutex);
  // std::cout << "=========sync"  << std::endl ;
  while (ready_queue.num_tasks_complete < ready_queue.num_total_tasks) {
    // std::cout << "wait before ready_queue.num_bulks_complete="  <<  ready_queue.num_bulks_complete  << ",
    // num_total_bulks= " << ready_queue.num_total_bulks << std::endl;
    ready_queue.num_tasks_complete_cv.wait(lock);
    // std::cout << "wait after bulks.num_bulks_complete="  <<  bulks.num_bulks_complete  << ", num_total_bulks= " <<
    // bulks.num_total_bulks << std::endl;
  }
  lock.unlock();
  return;
}