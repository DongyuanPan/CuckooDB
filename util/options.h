/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-18 14:53
 * Filename      : options.h
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_INCLUDE_OPTIONS_H_
#define CUCKOODB_INCLUDE_OPTIONS_H_

#include "comparator.h"

namespace cdb{

class Options{
 public:
  Options():
    comparator_(nullptr) { }

  ~Options(){}

  Comparator* comparator_;
  uint64_t write_buffer__size;
  std::string log_target;

};


class ReadOptions{
  bool checksum;
  ReadOptions()
	  :checksum(false){}

};

class WriteOptioins{
  bool sync;
  WriteOptions()
	  :sync(false){}
};

}//namespace cdb

#endif  //CUCKOODB_INCLUDE_OPTIONS_H_
