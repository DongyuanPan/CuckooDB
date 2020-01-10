/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-23 10:37
 * Filename      : cache.cc
 * Description   : 
 * *******************************************************/

#include "cache.h"

namespace cdb{

Cache::Cache(cdb::Options db_options, cdb::EventManager* event_manager){
  stop_ = false;
  num_readers_ = 0;
  max_size_ = 50;
  index_live_ = 0;
  index_copy_ = 1;
  sizes_[index_live_] = 0;
  sizes_[index_copy_] = 0;
  event_manager_ = event_manager;
  db_options_ = db_options;
  thread_cache_handler_ = std::thread(&Cache::Run, this);
}


Status Cache::Get(ReadOptions& write_options, const std::string &key, std::string* value){
  
  //read live cache	
  w_mutex_cache_live_l1.lock();
  mutex_live_size_l3.lock();
  
  auto& cache_live = caches_[index_live_];

  mutex_live_size_l3.unlock();
  w_mutex_cache_live_l1.unlock();

  bool found = false;
  Entry entry_found;
  for (auto& entry:cache_live){
    if (entry.key == key){
      found = true;
      entry_found = entry;
    }//no break, in order to find the latest item
  }

  if (found){
    if (entry_found.op_type == EntryType::Put_Or_Get){
      log::trace("Cache::Get()","found and op_type is match, can be return value:%s", entry_found.value.c_str());
      *value = entry_found.value;
      return Status::OK();
    }else if (entry_found.op_type == EntryType::Delete){
      return Status::RemoveEntry();
    }else
      return Status::NotFound("Unable to find entry");
  }

  log::trace("Cache::Get()","search in swap cache");
  //read swap cache
  w_mutex_cache_swap_l4.lock();
  r_mutex_cache_swap_l5.lock();

  ++num_readers_;
  auto& cache_swap = caches_[index_copy_];
  w_mutex_cache_swap_l4.unlock();
  r_mutex_cache_swap_l5.unlock();

  for (auto& entry:cache_swap){
    if (entry.key == key){
      found = true;
      entry_found = entry;
    }//no break, in order to find the latest item
  }

  Status s;
  found = false;
  if (found){
    if (entry_found.op_type == EntryType::Put_Or_Get){
      *value = entry_found.value;
      s = Status::OK();
    }else if (entry_found.op_type == EntryType::Delete){
      s = Status::RemoveEntry();
    }else
      s = Status::NotFound("Unable to find entry");
  }
  
  r_mutex_cache_swap_l5.lock();

  --num_readers_;
  r_mutex_cache_swap_l5.unlock();
  cond_reader.notify_one();

  return s;
}


Status Cache::Put(WriteOptions& write_options, const std::string &key, const std::string& value){
  return Additem(write_options,
        EntryType::Put_Or_Get,
	      key,
	      value);
}


Status Cache::Delete(WriteOptions& write_options, const std::string& key){
  std::string value = "";
  return Additem(write_options,
              EntryType::Delete,
              key,
              value);

}


Status Cache::Additem(WriteOptions& write_options, const EntryType& op_type, const std::string &key, const std::string& value){
  uint64_t kv_size = key.size() + value.size();
  log::trace("Cache::Add()","kvsize:%d", kv_size);
  log::trace("Cache::Add()","key %s, value %s", key.c_str(), value.c_str());
  std::unique_lock<std::mutex> lock_cache_live_(w_mutex_cache_live_l1);


  caches_[index_live_].push_back(Entry{std::this_thread::get_id(),
		  		write_options,
		  		op_type,
		  		key,
		  		value});
  mutex_live_size_l3.lock();
  sizes_[index_live_] += kv_size;
  uint64_t cache_live_size = sizes_[index_live_];
  log::trace("Cache::Add()", "live_size_ %d",cache_live_size);
  mutex_live_size_l3.unlock();

  if (cache_live_size > max_size_){
    mutex_flush_l2.lock();
    std::unique_lock<std::mutex> lock_swap(mutex_live_size_l3);
    log::trace("Cache::Add()", "swap and cache");
    cond_flush.notify_one();
    mutex_flush_l2.unlock();

  }

  //unlock w_mutex_cache_live_l1
  return Status::OK();
}

void Cache::Run(){
  while(true){
    std::unique_lock<std::mutex> lock_flush(mutex_flush_l2);
    
    //使用循环 判断条件 防止虚假唤醒
    while(sizes_[index_live_] == 0){
      cond_flush.wait(lock_flush);
      if (IsStop()) return;
    }
    
    mutex_live_size_l3.lock();
    //如果 swap cache 大小为0 说明当 live cache满了就可以进行覆盖了 
    if (sizes_[index_copy_] == 0){
      //阻塞，等待live cache 满了就 notify 
      log::trace("Cache::Run", "swap cahe");
      std::swap(index_live_, index_copy_);
    }
    mutex_live_size_l3.unlock();

    //to-do:notify 通知StorageEngine 可以固化swap cache 到硬盘上，并更新索引 
    log::trace("Cache::Run", " notify 通知StorageEngine 可以固化swap cache 到硬盘上，并更新索引");
    event_manager_->flush_cache.notify_and_wait(caches_[index_copy_]);

    log::trace("Cache::Run", "wait clear cache ");
    event_manager_->clear_cache.Wait();
    event_manager_->clear_cache.Done();
    log::trace("Cache::Run", "clear cache has benn done");
    log::trace("Cache::Add()", "live_size_ %d",sizes_[index_live_]);
    log::trace("Cache::Add()", "swap_size_ %d",sizes_[index_copy_]);
    //to-do:等待所有读swap cahce 的线程结束，然后清空swap cache
    w_mutex_cache_swap_l4.lock();
    while(true) {
      std::unique_lock<std::mutex> lock_read(r_mutex_cache_swap_l5);
      if (num_readers_ == 0) break;
      cond_reader.wait(lock_read);
    }
 
    sizes_[index_copy_] = 0;
    caches_[index_copy_].clear();
    w_mutex_cache_swap_l4.unlock();
  }
}


};
