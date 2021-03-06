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
#include <unordered_map>

#include "date_file_manager.h"
#include "util/event_manager.h"

#include "file/file_resource_manager.h"
#include "file/file_pool.h"

#include "util/options.h"
#include "util/entry.h"
#include "util/status.h"
#include "util/logger.h"
#include "util/xxhash.h"
#include "util/const_value.h"
#include "entry_format.h"



namespace cdb{

class StorageEngine{
  public:
    StorageEngine(Options db_options,
                  std::string dbname,
                  EventManager *event_manager
                  )
	    :dbname_(dbname),
       db_options_(db_options),
       event_manager_(event_manager),
       date_file_manager_(db_options, dbname, kUncompactedRegularType, false) {
      
      log::trace("StorageEngine:StorageEngine()", "dbname: %s", dbname_.c_str());
      stop_ = false;
      is_closed_ = false;
      num_readers_ = 0;
      file_pool_ = std::make_shared<FilePool>();
      
      //启动事件循环 
      thread_data_ = std::thread(&StorageEngine::RunData, this);
      thread_index_ = std::thread(&StorageEngine::RunIndex, this);


      Status s = date_file_manager_.LoadDatabase(dbname, index_);
      if (!s.IsOK()) {
        log::emerg("StorageEngine", "Could not load database: [%s]", s.ToString().c_str());
        Close();
      }      
    
    };

    ~StorageEngine() {
      //Close();
    }

    void Close() {
      std::unique_lock<std::mutex> lock(mutex_close_);
      if (is_closed_) return;
      is_closed_ = true;

      // Wait for readers to exit
      AcquireWriteLock();
      date_file_manager_.Close();
      SetStop();
      ReleaseWriteLock();


      log::trace("StorageEngine::Close()", "join start");
      //通知线程，子线程判断stop后返回
      event_manager_->update_index.Notify(); 
      event_manager_->flush_cache.Notify(); 
      thread_index_.join();
      thread_data_.join();

      log::trace("StorageEngine::Close()", "end");

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
        std::multimap<uint64_t, uint64_t> indexs;
        //处理数据写入文件之中
        date_file_manager_.WriteEntrys(entrys, indexs);

        ReleaseWriteLock();

        //通知事件处理器  已经处理完毕 写入到文件 
        event_manager_->flush_cache.Done();
        //通知事件处理器 开始更新索引
        event_manager_->update_index.notify_and_wait(indexs);
      }
    }

    void RunIndex() {
      log::trace("StorageEngine::RunIndex()", "start to wait for handle index");

      while(true) {
        //阻塞等待
        std::multimap<uint64_t, uint64_t> index_entrys = event_manager_-> update_index.Wait();
        if (IsStop()) return;
        log::trace("StorageEngine::RunIndex()", "got %d to update", index_entrys.size());
        
        //允许其他线程获取写锁
        int num_iterations_per_lock = db_options_.internal__num_iterations_per_lock;
        int counter_iterations = 0;
        for (auto& index:index_entrys){
          if (counter_iterations == 0) {
            AcquireWriteLock();
          }
          ++counter_iterations;
          index_.insert(std::pair<uint64_t, uint64_t>(index.first, index.second));
          if (counter_iterations >= num_iterations_per_lock){
            ReleaseWriteLock();
            counter_iterations = 0;
          }
        }

        if (counter_iterations) ReleaseWriteLock();

        event_manager_->update_index.Done();
        //写操作完成 通知 清除swap cache
        log::trace("StorageEngine::RunIndex()", "update index Done  then notify to clear cache");
        int tmp = 1;
        event_manager_->clear_cache.notify_and_wait(tmp);

      }
      
    }
               
    Status Get(ReadOptions& read_option,
               const std::string& key,
               std::string* value) {
      // uint64_t hasked_key = XXH64(key.data(), key.size(), 0);
      log::trace("StroageEngine::Get()", "key str : %s", key.c_str());
      mutex_write_.lock();
      mutex_read_.lock();
      num_readers_ += 1;
      mutex_read_.unlock();
      mutex_write_.unlock();

      bool has_compaction_index = false;
      mutex_compaction_.lock();
      has_compaction_index = is_compaction_in_progress_;
      mutex_compaction_.unlock();
      
      Status s;
      if (!has_compaction_index){
        //不在合并中
        s = GetWithIndex(read_option, index_, key, value);
      } else {
        s = GetWithIndex(read_option, index_compaction_, key, value);
        if (!s.IsOK() && !s.IsRemoveEntry()){
          s =GetWithIndex(read_option, index_, key, value);
        }
      }

      mutex_read_.lock();
      num_readers_ -= 1;
      log::trace("StroageEngine::Get()", "num_readers_ : %d", num_readers_);
      mutex_read_.unlock();
      cond_read_complete_.notify_one();

      return s;
    }

    Status GetWithIndex(ReadOptions& read_option, 
                        std::multimap<uint64_t, uint64_t>& index,
                        const std::string& key,
                        std::string* value) {
      log::trace("StroageEngine::GetWithIndex()", "key str : %s, index size: %d", key.c_str(), index.size());
      uint64_t hashed_key = XXH64(key.data(), key.size(), 0);

      log::trace("StroageEngine::GetWithIndex()","hashed_key : %llu", hashed_key);
      //查找键值
      auto range = index.equal_range(hashed_key);
      //直接读取最近对该key的操作
      if (range.first != range.second){
        auto cur = range.second;
        do {
          --cur;
          std::string key_cmp;
          Status s = GetEntry(read_option, cur->second, &key_cmp, value);
          //如果 这个位置 存的就是这个键值 就返回，否则是hash冲突，继续往前找
          if (key_cmp == key && (s.IsOK() || s.IsRemoveEntry())){
            log::trace("StroageEngine::GetWithIndex()", "find  ");
            return s;
          }
          log::trace("StroageEngine::GetWithIndex()", "not match");   
        } while(cur != range.first);
      }
      return Status::NotFound("Unable to find the entry in the storage engine");
    }

    //传指针避免拷贝
    Status GetEntry(ReadOptions& read_option,
                    uint64_t location,
                    std::string* key,
                    std::string* value) {              
      Status s = Status::OK();
      
      uint32_t fileid = (location & 0xFFFFFFFF00000000) >> 32;
      uint32_t offset_in_file = location & 0x00000000FFFFFFFF;
      log::trace("StroageEngine::GetEntry()", "fileid : %d", fileid); 
      log::trace("StroageEngine::GetEntry()", "offset_in_file : %d", offset_in_file);       
      //文件结尾偏移
      uint64_t filesize = 0;
      filesize = date_file_manager_.file_resource_manager.GetFileSize(fileid);

      //文件路径
      std::string filepath = date_file_manager_.GetFilepath(fileid);
      log::trace("StroageEngine::GetEntry()", "filepath : %s", filepath.c_str()); 
      //实现文件池  用于管理读写的文件
      FileResource file_resource_;
      file_pool_->GetFile(fileid, filepath, filesize, &file_resource_);

      struct EntryHeader entry_header;
      uint32_t size_header;
      s = EntryHeader::DecodeFrom(db_options_, 
                                  read_option, 
                                  file_resource_.mmap + offset_in_file, 
                                  filesize - offset_in_file, 
                                  &entry_header, 
                                  &size_header);
      if (!s.IsOK()) {
        return s;
        log::trace("StroageEngine::GetEntry()", "not find"); 
      }
      std::string key_out = std::string(file_resource_.mmap + offset_in_file + size_header, entry_header.size_key);
      std::string value_out = std::string(file_resource_.mmap + offset_in_file + size_header + entry_header.size_key, entry_header.size_value);
      
      if (entry_header.IsTypeDelete()) {
        s = Status::RemoveEntry();
        log::trace("StroageEngine::GetEntry()", "RemoveEntry"); 
      }      
      
      log::trace("StroageEngine::GetEntry()", "key_out : %s", key_out.c_str()); 
      *key = key_out;
      *value = value_out;

      return s;
    }




  private:
    std::string dbname_;
    bool stop_;
    cdb::Options db_options_;
    EventManager *event_manager_;
    DateFileManager date_file_manager_;


    //事件循环线程
    std::thread thread_data_;
    std::thread thread_index_;

    //读写锁
    //to-do : 实现读写锁类
    std::mutex mutex_read_;
    std::mutex mutex_write_;
    int num_readers_;
    std::condition_variable cond_read_complete_;//读线程 全部结束
    std::mutex mutex_compaction_;
    bool is_compaction_in_progress_;//是否在合并中
    std::shared_ptr<FilePool> file_pool_;

    std::multimap<uint64_t, uint64_t> index_;
    std::multimap<uint64_t, uint64_t> index_compaction_;
    //存在 活锁问题（饥饿）
    //to-do:实现读写锁类 写优先读写锁

    void AcquireWriteLock() {
      mutex_write_.lock();
      while(true) {
        std::unique_lock<std::mutex> lock_read(mutex_read_);
        if (num_readers_ == 0) break;
        cond_read_complete_.wait(lock_read);
      }
    }

    void ReleaseWriteLock() {
        mutex_write_.unlock();
    }


    bool is_closed_;
    std::mutex mutex_close_;

};

}

#endif 
