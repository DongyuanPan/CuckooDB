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
#include <condition_variable>

namespace cdb{

class Cache{
  public:
    Cache();
    ~Cache(){}

    Status Get(const std::string &key, std::string* value);
    Status Put(const std::string &key, const std::string& value);
    Status Delete(const std::string& key);

    Status Additem(const EntryType& op_type, const std::string &key, const std::string& value);
    void set_max_size_(int max_size){
      max_size_ = max_size;
    }

  private:
    void Run();//event loop
    bool IsStop(){ return stop_;}
    void SetStop(){ stop_ = true;}
    

    std::vector<Entry> cache_live_;
    int live_size_;
    std::vector<Entry> cache_swap_;
    int swap_size_;
    int max_size_;
    bool stop_;	
    int num_readers_;


    std::mutex w_mutex_cache_live_l1;
    std::mutex mutex_flush_l2;
    std::mutex mutex_live_size_l3;
    std::mutex w_mutex_cache_swap_l4;
    std::mutex r_mutex_cache_swap_l5;

    std::condition_variable cond_flush;
    std::condition_variable cond_reader;

    std::thread thread_cache_handler_;
};
};

#endif
