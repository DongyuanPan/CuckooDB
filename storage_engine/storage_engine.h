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

namespace cdb{

class StorageEngine{
  public:
    StorageEngine(std::string name)
	    :dbname_(name){
	    };

  private:
    std::string dbname_;
};

}

#endif 
