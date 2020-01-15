/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-23 09:06
 * Filename      : cache.h
 * Description   : 缓存
 * *******************************************************/

#ifndef CUCKOODB_CACHE_H
#define CUCKOODB_CACHE_H

#include <thread>
#include <string>
#include <vector>
#include <map>

#include "util/entry.h"
#include "util/logger.h"
#include "util/status.h"
#include "util/options.h"
#include "util/event_manager.h"
#include <condition_variable>

namespace cdb{

class Cache{
  public:
    Cache(cdb::Options db_options, EventManager* event_manager_);
    ~Cache();

    Status Get(ReadOptions& write_options, const std::string &key, std::string* value);
    Status Put(WriteOptions& write_options, const std::string &key, const std::string& value);
    Status Delete(WriteOptions& write_options, const std::string& key);

    Status Additem(WriteOptions& write_options, const EntryType& op_type, const std::string &key, const std::string& value);
    void set_max_size_(int max_size){
      max_size_ = max_size;
    }

    void Close(); 
    void Flush();

  private:
    void Run();//event loop
    bool IsStop();
    void SetStop();
    
    int index_live_;
    int index_copy_;
    std::array<std::vector<Entry>,2> caches_;
    std::array<int, 2> sizes_;
    int max_size_;
    bool stop_;	
    int num_readers_;

    cdb::Options db_options_;
    cdb::EventManager* event_manager_;


    std::mutex w_mutex_cache_live_l1;
    std::mutex mutex_flush_l2;
    std::mutex mutex_live_size_l3;
    std::mutex w_mutex_cache_swap_l4;
    std::mutex r_mutex_cache_swap_l5;

    std::condition_variable cond_flush;
    std::condition_variable cond_reader;
    std::condition_variable cv_flush_done_;

    std::thread thread_cache_;

    bool is_closed_;
    std::mutex mutex_close_;
};
};

#endif
