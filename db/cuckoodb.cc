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

CuckooDB::CuckooDB(DatebaseOpention db_options, 
                   std::string name,
                   EventManager *event_manager):
  name_(name) {
  event_manager_ = new EventManager();
  cache_ = new Cache(db_options, event_manager_);
  stroage_engine_ = new StorageEngine(db_options, name, event_manager);

}

Status CuckooDB::Get(const std::string &key, std::string* value) {
  log::trace("CuckooDB::Get()","key:%s", key.c_str());
  Status s = cache_.Get(key, value);

  if (s.IsRemoveEntry()){
    return Status::NotFound("Has been Remove, Unable to find");
  }

  //to-do:find in StorageEngine
  if (s.IsNotFound()){
    //find in StorageEngine
    return Status::NotFound("Unable to find");
  }


  log::trace("CuckooDB::Get()", "found key, return value");
  return s;
} 

Status CuckooDB::Put(const std::string &key, const std::string& value) {
  log::trace("CuckooDB::Put", "Put key:%s, value:%s", key.c_str(), value.c_str());
  return cache_.Put(key, value);
}

Status CuckooDB::Delete(const std::string& key) {
  log::trace("CuckooDB::Delete()","delete key:%s", key.c_str());
  return cache_.Delete(key);
}

/*
Status CuckooDB::Additem(const EntryType& op_type, const std::string &key, const std::string& value) {
  return Status::OK();
}
*/
}
