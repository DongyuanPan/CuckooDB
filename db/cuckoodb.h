/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-22 20:35
 * Filename      : cdb.h
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_IMPL_H_
#define CUCKOODB_IMPL_H_

#include <unistd.h>
#include <string>
#include <thread>
#include <memory>

#include "db.h"
#include "util/entry.h"
#include "util/logger.h"
#include "util/status.h"
#include "cache/cache.h"
#include "storage_engine/storage_engine.h"
#include "util/event_manager.h"
#include "util/options.h"

namespace cdb{

class CuckooDB:public DB{
  public:
    CuckooDB(cdb::Options db_options, std::string name);
    virtual ~CuckooDB(){}

    virtual Status Get(ReadOptions& write_options, const std::string &key, std::string* value) override;
    virtual Status Put(WriteOptions& write_options, const std::string &key, const std::string& value) override;
    virtual Status Delete(WriteOptions& write_options, const std::string& key) override;

  private:
    std::string name_;//database name
    std::mutex mutex_;

    cdb::StorageEngine *stroage_engine_;
    cdb::Cache *cache_;
    cdb::EventManager *event_manager_;
    // cdb::CRC32 crc32_;

};

};


#endif 
