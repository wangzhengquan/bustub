#include "common/util/tasks_util.h"
#include <unistd.h>
#include <chrono>
#include <iostream>

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

TasksUtil::TasksUtil(int num_threads) : num_threads_(num_threads) {};

void TasksUtil::run(){
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
          ready_queue_lock.unlock();
          break;
        }

        if (ready_queue.queue.empty()) {
          ready_queue.cv.wait(ready_queue_lock);
          ready_queue_lock.unlock();
        } else {
          std::shared_ptr<ReadyTask> ready_task = ready_queue.queue.front();
          size_t worker_id = ready_task->worker_id_++;
          if (worker_id >= ready_task->num_workers_) {
            ready_queue.queue.pop();
            ready_queue_lock.unlock();
          } else {
            ready_queue_lock.unlock();
             
            size_t works_per_worker = (ready_task->total_amount_works_ + ready_task->num_workers_ - 1) / ready_task->num_workers_;
            size_t from = works_per_worker * worker_id;
            size_t to = std::min(from + works_per_worker, ready_task->total_amount_works_);
            ready_task->work_(from, to);

            std::unique_lock<std::mutex> ready_task_lock(ready_task->mutex);
            ready_task->num_workers_completed++;
            if (ready_task->num_workers_completed == ready_task->num_workers_) {
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
                WaitTask &wait_task = *wait_queue_it;
                for (auto it = wait_task.deps.begin(); it != wait_task.deps.end();) {
                  if (*it == ready_task->task_id_) {
                    it = wait_task.deps.erase(it);
                    break;
                  } else
                    ++it;
                }

                if (wait_task.deps.empty()) {
                  ready_queue_lock.lock();
                  ready_queue.queue.push(std::shared_ptr<ReadyTask>(
                      new ReadyTask{wait_task.task_id_, wait_task.work_, wait_task.num_workers_, wait_task.total_amount_works_}));
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

TasksUtil::~TasksUtil() {
  // std::cout << "=========~TasksUtil"  << std::endl ;
  sync();
  std::unique_lock<std::mutex> lock(ready_queue.mutex);
  ready_queue.state_ = State::Terminated;
  lock.unlock();
  ready_queue.cv.notify_all();
  for (auto &thread : _threads_pool) {
    thread.join();
  }
}

void TasksUtil::addTask(WorkFunction work, size_t num_workers, size_t total_amount_works) {
  const std::vector<TaskID> deps;
  addTaskWithDeps(std::move(work), num_workers, total_amount_works, deps);
}
//runAsyncWithDeps
void TasksUtil::addTaskWithDeps(WorkFunction work, size_t num_workers, size_t total_amount_works, const std::vector<TaskID> &deps) {
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
    ready_queue.queue.push(std::shared_ptr<ReadyTask>(new ReadyTask{next_task_id_++, work, num_workers, total_amount_works}));
    lock.unlock();
    ready_queue.cv.notify_all();
    return ;
  } else {
    std::unique_lock<std::mutex> lock(wait_queue.mutex);
    wait_queue.queue.push_back(WaitTask{next_task_id_++, work, num_workers, total_amount_works, newdeps});
    complete_queue_lock.unlock();
    lock.unlock();
    return ;
  }
}

void TasksUtil::sync() {
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