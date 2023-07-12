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

typedef int TaskID;

class IRunnable {
 public:
  virtual ~IRunnable();

  /*
    Executes an instance of the task as part of a bulk task launch.

      - task_id: the current task identifier. This value will be
        between 0 and num_total_tasks-1.

      - num_total_tasks: the total number of tasks in the bulk
        task launch.
    */
  virtual void runTask(int task_id, int num_total_tasks) = 0;
};

class TaskUtil {
 private:
  class ReadyBulk {
   public:
    // std::queue<Task> queue;
    int bulk_id;
    IRunnable *runnable;
    int task_id{0};
    int num_total_tasks{0};
    std::mutex mutex;
    std::condition_variable cv;
    int num_tasks_completed{0};

    // std::atomic<int> num_tasks_completed{0};
    ReadyBulk(int _bulk_id, IRunnable *_runnable, int _num_total_tasks)
        : bulk_id(_bulk_id), runnable(_runnable), num_total_tasks(_num_total_tasks) {}
  };

  class WaitBulk {
   public:
    int bulk_id;
    IRunnable *runnable;
    int num_total_tasks;
    std::list<TaskID> deps{};
    WaitBulk(int _bulk_id, IRunnable *_runnable, int _num_total_tasks, const std::vector<TaskID> &_deps)
        : bulk_id(_bulk_id), runnable(_runnable), num_total_tasks(_num_total_tasks) {
      for (TaskID dep : _deps) {
        deps.push_back(dep);
      }
    }
  };

  class FinishQueue {
   public:
    std::set<TaskID> queue{};
    std::mutex mutex;
  };

  class ReadyQueue {
   public:
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::shared_ptr<ReadyBulk> > queue{};
    bool stop{false};

    std::mutex bulk_finished_mutex;
    std::condition_variable bulk_finished_cv;
    int num_bulks_complete{0};
    int num_total_bulks{0};
  };

  class WaitQueue {
   public:
    std::list<WaitBulk> queue;
    std::mutex mutex;
    std::condition_variable cv;
  };

  std::vector<std::thread> _threads_pool;
  TaskID _next_bulk_id{0};
  ReadyQueue ready_queue;
  WaitQueue wait_queue;
  FinishQueue finish_queue;
  int my_stopi = 0;

 public:
  /*
    Instantiates a task system.

      - num_threads: the maximum number of threads that the task system
        can use.
    */
  TaskUtil(int num_threads);
  ~TaskUtil();
  const char *name();

  /*
    Executes a bulk task launch of num_total_tasks.  Task
    execution is synchronous with the calling thread, so run()
    will return only when the execution of all tasks is
    complete.
  */
  void run(IRunnable *runnable, int num_total_tasks);

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
  int runAsyncWithDeps(IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps);

  /*
    Blocks until all tasks created as a result of **any prior**
    runXXX calls are done.
    */
  void sync();
};