/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-22 11:28
 * Filename      : threadpool.h
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_THREADPOOL_H_
#define CUCKOODB_THREADPOOL_H_

#include <unistd.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>


namespace cdb{

class Task{
  Task(){}
  virtual ~Task{}
  virtual void RunInLock(std::thread::id tid) = 0;
  virtual void run(std::thread::id tid) = 0;
}


class ThreadPool{

 public:
  ThreadPool(int thread_num):stop(false){
    thread_num_ = thread_num;
  }

  ~ThreadPool(){
    {
       std::unique_lock<std::mutex> lock(task_queue_mutex_);
       stop = true;
    }
    cond_val.notify_all();
    for (auto& thread:worker_threads_){
      thread.join();
    }
  }

  bool Append(Task* task){
    std::unique_lock<std::mutex> lock(task_queue_mutex_);
    task_queue_.push(task);
    cond_val.notify_one();
  }


  void Worker(){
    while(!stop){
      std::unique_lock<std:mutex> lock(task_queue_mutex_);
      if (task_queue_.empty()){
        cond_val.wait(lock);
	if (stop) continue;
      }

      if (task_queue_.empty()) continue;
      Task* task = task_queue_.front();
      task_queue_.pop();
      task->RunInLock(std::this_thread::get_id());
      lock.unlock();
      task->Run(std::this_thread::get_id());
      if (!IsStopRequested()) delete task;

    }
  }


  int Start(){
    for (int i = 0; i < thread_num_; ++i){
      worker_threads_.push_back(std::thread(&ThreadPool::Worker, this));          
    } 
    return 0;
  }

  void Stop(){
    return 0;
  }



 public:
  int thread_num_;
  std::queue<Task*> task_queue_;
  std::vector<std::thread> worker_threads_;
  std::mutex task_queue_mutex_;
  std::condition_variable cond_val;
  bool stop;

}

}//namespace cdb


#endif
