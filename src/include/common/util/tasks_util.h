#pragma once
#include <limits.h>
#include <atomic>
#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <vector>
#include <functional>
#include <deque>

typedef int TaskID;


class TasksUtil {
  //< from, to>
  using WorkFunction =  std::function<void(size_t, size_t)>;
 private:
  enum class State { Ready = 0, Running, Terminated };
  class ReadyTask {
   public:
    TaskID task_id_;
    
    WorkFunction work_;
     
    size_t worker_id_{0};
    const size_t num_workers_;
    std::mutex mutex;
    std::condition_variable cv;
    size_t num_workers_completed{0};

    const size_t total_amount_works_;

    ReadyTask(int task_id, WorkFunction &work, size_t num_workers, size_t total_amount_works)
        : task_id_(task_id), 
          work_(std::move(work)), 
          num_workers_(num_workers), 
          total_amount_works_(total_amount_works) {}
  };

  class WaitTask {
   public:
    TaskID task_id_;
    WorkFunction work_;
    const size_t num_workers_;
    const size_t total_amount_works_;
    std::set<TaskID> deps_{};
    WaitTask(TaskID task_id, WorkFunction &work, size_t num_workers, size_t total_amount_works, std::set<TaskID> &deps)
        : task_id_(task_id), 
          work_(std::move(work)), 
          num_workers_(num_workers), 
          total_amount_works_(total_amount_works),
          deps_(std::move(deps)) {}
  };

  class CompleteQueue {
   public:
    std::set<TaskID> elements{};
    std::mutex mutex;
  };

  class ReadyQueue {
   public:
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::shared_ptr<ReadyTask> > elements{};
    State state_ = State::Ready;

    std::mutex num_tasks_complete_mutex;
    std::condition_variable num_tasks_complete_cv;
    int num_tasks_complete{0};
    int num_total_tasks{0};
  };

  class WaitQueue {
   public:
    std::list<WaitTask> elements;
    std::mutex mutex;
    std::condition_variable cv;
  };

  std::vector<std::thread> workers_;
  TaskID next_task_id_{0};
  ReadyQueue ready_queue;
  WaitQueue wait_queue;
  CompleteQueue complete_queue;
  const size_t total_num_workers_;
  

 public:
  /*
    Instantiates a task system.

      - total_num_workers: the maximum number of threads that the task system
        can use.
    */
  explicit TasksUtil(int total_num_workers);
  ~TasksUtil();

  void run();
  /*
    Executes a bulk task launch of num_total_tasks.  Task
    execution is synchronous with the calling thread, so run()
    will return only when the execution of all tasks is
    complete.
  */
  TaskID addTask(WorkFunction work, size_t num_workers, size_t total_amount_works);

  /*
    Executes an asynchronous bulk task launch of
    num_total_tasks, but with a dependency on prior launched
    tasks.


    The task runtime must complete execution of the tasks
    associated with all bulk task launches referenced in the
    array `deps` before beginning execution of *any* task in
    this bulk task launch.

    The caller must invoke sync() to guarantee completion of the
    tasks in this bulk task launch.

    Returns an identifer that can be used in subsequent calls to
    runAsnycWithDeps() to specify a dependency of some future
    bulk task launch on this bulk task launch.
    */
  TaskID addTaskWithDeps(WorkFunction work, size_t num_workers, size_t total_amount_works, const std::vector<TaskID> &deps);

  /*
    Blocks until all tasks created as a result of **any prior**
    runXXX calls are done.
    */
  void sync();
};