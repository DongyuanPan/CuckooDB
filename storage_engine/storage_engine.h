/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-12-19 10:40
 * Filename      : storage_engine.h
 * Description   : 
 * *******************************************************/
#ifndef CUCKOODB_STORAGE_ENGINE_H_
#define CUCKOODB_STORAGE_ENGINE_H_

#include <thread>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_multimap>

#include "data_file_manager.h"
#include "eventmanager.h"

#include ""file/file_resource_manager.h

#include "util/options.h"
#include "util/entry.h"
#include "util/status.h"
#include "util/logger.h"


namespace cdb{

class StorageEngine{
  public:
    StorageEngine(DatebaseOpention db_options,
                  std::string name，
                  EventManager *event_manager
                  )
	    :dbname_(name),
       db_options_(db_options),
       event_manager_(event_manager),
       date_file_manager_(db_options, dbname, read_only) {

      log::trace("StorageEngine:StorageEngine()", "dbname: %s", dbname_.c_str());
      stop_ = false;
      num_readers_ = 0;
      
      //启动事件循环 
      thread_data_ = std::thread(&StorageEngine::RunData, this);
      thread_index_ = std::thread(&StorageEngine::RunIndex, this);


	  };

    ~StorageEngine() {
      thread_index_.join();
      thread_data_.join();
    }

    bool IsStop() {
      return stop_;
    }
    void SetStop(){
      stop_ = true;
    }

    //处理数据写入的事件循环
    void RunData() {
      log::trace("StorageEngine::RunData()", "start to wait for handle data flush");
      //等待 swap cache 满了 通过 事件驱动器通知 进行处理
      while(true){
        //阻塞等待
        std::vector<Entry> entrys = event_manager_-> flush_cache.Wait();
        if (IsStop()) return;
        log::trace("StorageEngine::RunData()", "got %d entry", entrys.size());

        //获取写f
        AcquireWriteLock();
        //哈希表，存储索引
        std::unordered_multimap<uint64_t, uint64_t> indexs;
        //处理数据写入文件之中
        date_file_manager_.WriteEntrys(entrys, indexs);

        ReleaseWriteLock();

        //通知事件处理器  已经处理完毕 写入到文件 
        event_manager_->flush_cache.Done();
        //通知事件处理器 开始更新索引
        event_manager_->update_index(indexs);
      }
    }

    void RunIndex() {
      log::trace("StorageEngine::RunIndex()", "start to wait for handle index");
      
    }

  private:
    std::string dbname_;
    bool stop_;
    DatebaseOpention db_options_;
    EventManager *event_manager_;
    DateFileManager date_file_manager_;

    //事件循环线程
    std::thread thread_data_;
    std::thread thread_index_;

    //读写锁
    //to-do : 实现读写锁类
    std::mutex mutex_reader_;
    std::mutex mutex_writer_;
    int num_readers_;
    std::condition_varialbe cond_read_complete_;//读线程 全部结束

    //存在 活锁问题（饥饿）
    //to-do:实现读写锁类 写优先读写锁

    void AcquireWriteLock() {
      mutex_write_.lock();
      while(true) {
        std::unique<std::mutex> lock_read(mutex_read_);
        if (num_readers_ == 0) break;
        cond_read_complete_.wait(lock_read);
      }
    }

    void ReleaseWriteLock() {
        mutex_write_.unlock();
    }





};

}

#endif 
