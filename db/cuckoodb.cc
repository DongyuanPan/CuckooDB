/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-12-19 09:48
 * Filename      : cuckoodb.cc
 * Description   : 
 * *******************************************************/
#include "cuckoodb.h"

namespace cdb{

CuckooDB::CuckooDB(cdb::Options db_options, 
                   std::string name):
  name_(name) {
  event_manager_ = new EventManager();
  cache_ = new Cache(db_options, event_manager_);
  stroage_engine_ = new StorageEngine(db_options, name, event_manager_);

}

Status CuckooDB::Get(ReadOptions& read_options, const std::string &key, std::string* value) {
  log::trace("CuckooDB::Get()","key:%s", key.c_str());

  //查找Cache
  Status s = cache_->Get(read_options, key, value);

  if (s.IsRemoveEntry()){
    return Status::NotFound("Has been Remove, Unable to find");
  } else if (s.IsNotFound()){
    //find in StorageEngine
    log::trace("CuckooDB::Get()", "not found in cahce, search in StorageEngine");
    s = stroage_engine_->Get(read_options, key, value);
    if (s.IsNotFound()) {
      log::trace("CuckooDB::Get()", "not found in StorageEngine");
      return s;
    } else if (s.IsOK()){
      log::trace("CuckooDB::Get()", "found in StorageEngine");
      return s;
    }
    return Status::NotFound("Unable to find");
  }


  log::trace("CuckooDB::Get()", "found key in cahce, return value");
  return s;
} 

Status CuckooDB::Put(WriteOptions& write_options, const std::string &key, const std::string& value) {
  log::trace("CuckooDB::Put", "Put key:%s, value:%s", key.c_str(), value.c_str());
  return cache_->Put(write_options, key, value);
}

Status CuckooDB::Delete(WriteOptions& write_options, const std::string& key) {
  log::trace("CuckooDB::Delete()","delete key:%s", key.c_str());
  return cache_->Delete(write_options, key);
}

/*
Status CuckooDB::Additem(const EntryType& op_type, const std::string &key, const std::string& value) {
  return Status::OK();
}
*/
}
