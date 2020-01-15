/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2019-12-21 14:43
 * Filename      : EventManager.h
 * Description   : 事件驱动器 通过条件变量 来进行事件的通知，并传输数据
 *                  由于数据类型不一样，因此使用模板
 *                  A 通知 B 则 B 需要先Wait()， A 再notify_and_wait，B处理完毕调用Done()
 * *******************************************************/

#ifndef CUCKOODB_EVENT_MANAGER_H_
#define CUCKOODB_EVENT_MANAGER_H_

#include <thread>
#include <condition_variable>
#include <unordered_map>
#include <vector>


namespace cdb {

template<typename T>
class Event {
  public:
    Event() { has_data = false; }

    void notify_and_wait(T& data) {
        std::unique_lock<std::mutex> lock_start(mutex_unique_);
        std::unique_lock<std::mutex> lock(mutex_);
        data_ = data;
        has_data = true;
        cv_ready_.notify_one();
        cv_done_.wait(lock);
    } 

    T Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!has_data) {
            cv_ready_.wait(lock);
        }
        return data_;
    }

    void Done() {
        std::unique_lock<std::mutex> lock(mutex_);
        has_data = false;
        cv_done_.notify_one();
    }

    void Notify() {
        cv_ready_.notify_one();
    }

  private:
    T data_;
    bool has_data;
    std::mutex mutex_;        
    std::mutex mutex_unique_; // 确保只有一个线程 进入notify_and_wait
    std::condition_variable cv_ready_;
    std::condition_variable cv_done_;
 
};

class EventManager {
  public:
    EventManager() {}
    //事件注册    
    Event<std::vector<Entry>> flush_cache;
    Event<std::multimap<uint64_t, uint64_t>> update_index;
    Event<int> clear_cache;
    Event<int> compaction_status;
};

}//end of namespace


#endif // end CUCKOODB_EVENT_MANAGER_H_