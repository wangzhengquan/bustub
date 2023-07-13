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

// class IRunnable {
//  public:
//   virtual ~IRunnable();

//   /*
//     Executes an instance of the task as part of a bulk task launch.

//       - task_id: the current task identifier. This value will be
//         between 0 and num_total_tasks-1.

//       - num_total_tasks: the total number of tasks in the bulk
//         task launch.
//     */
//   virtual void runTask(int task_id, int num_total_tasks) = 0;
// };

class TaskUtil {
 private:
  enum class State { Ready = 0, Running, Terminated };
  class ReadyTask {
   public:
    TaskID task_id_;
    //< worker_id, int num_task_workers>
    std::function<void(int, int)> &runnable;
     
    int worker_id_{0};
    int num_task_workers_{0};
    std::mutex mutex;
    std::condition_variable cv;
    int num_workers_completed{0};

    ReadyTask(int task_id, std::function<void(int, int)> &_runnable, int num_task_workers)
        : task_id_(task_id), runnable(_runnable), num_task_workers_(num_task_workers) {}
  };

  class WaitTask {
   public:
    TaskID task_id_;
    std::function<void(int, int)> &runnable;
    int num_task_workers_;
    std::list<TaskID> deps{};
    WaitTask(TaskID task_id, std::function<void(int, int)> &_runnable, int num_task_workers, const std::vector<TaskID> &_deps)
        : task_id_(task_id), runnable(_runnable), num_task_workers_(num_task_workers) {
      for (TaskID dep : _deps) {
        deps.push_back(dep);
      }
    }
  };

  class CompleteQueue {
   public:
    std::set<TaskID> queue{};
    std::mutex mutex;
  };

  class ReadyQueue {
   public:
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::shared_ptr<ReadyTask> > queue{};
    State state_ = State::Ready;
    // bool stop{false};

    std::mutex num_tasks_complete_mutex;
    std::condition_variable num_tasks_complete_cv;
    int num_tasks_complete{0};
    int num_total_tasks{0};
  };

  class WaitQueue {
   public:
    std::list<WaitTask> queue;
    std::mutex mutex;
    std::condition_variable cv;
  };

  std::vector<std::thread> _threads_pool;
  std::deque<std::function<void(int, int)> > fun_queue_;
  int next_task_id_{0};
  ReadyQueue ready_queue;
  WaitQueue wait_queue;
  CompleteQueue complete_queue;
  int my_stopi = 0;
  int num_threads_;
  

 public:
  /*
    Instantiates a task system.

      - num_threads: the maximum number of threads that the task system
        can use.
    */
  TaskUtil(int num_threads);
  ~TaskUtil();
  const char *name();

  void run();
  /*
    Executes a bulk task launch of num_total_tasks.  Task
    execution is synchronous with the calling thread, so run()
    will return only when the execution of all tasks is
    complete.
  */
  void addTask(std::function<void(int, int)> runnable, int num_task_workers);

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
  void addTaskWithDeps(std::function<void(int, int)> runnable, int num_task_workers, const std::vector<TaskID> &deps);

  /*
    Blocks until all tasks created as a result of **any prior**
    runXXX calls are done.
    */
  void sync();
};