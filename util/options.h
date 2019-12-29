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


namespace cdb{

class Options{
 public:
  Options(){
    internal__datafile_header_size = 4096;
    write_buffer__size = 4096;
  }

  ~Options(){}

  uint64_t write_buffer__size;
  std::string log_target;
  uint32_t internal__datafile_header_size;

};


struct ReadOptions{
  bool checksum;
  ReadOptions()
	  :checksum(false){}

};

struct WriteOptions{
  bool sync;
  WriteOptions()
	  :sync(false){}
};

}//namespace cdb

#endif  //CUCKOODB_INCLUDE_OPTIONS_H_
