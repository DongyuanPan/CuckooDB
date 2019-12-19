/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-22 17:47
 * Filename      : cuckoodb.h
 * Description   : 数据库的接口 
 * *******************************************************/

#ifndef CUCKOODB_DB_H_
#define CUCKOODB_DB_H_

#include <unistd.h>
#include <string>
#include "util/status.h"

namespace cdb{

class DB{
  public:
    DB(){}

    virtual  ~DB(){}

    virtual Status Get(const std::string &key, std::string* value) = 0;
    virtual Status Put(const std::string &key, const std::string& value) = 0;
    virtual Status Delete(const std::string& key) = 0;

};


};


#endif

