/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-12-17 22:32
 * Filename      : entry.h
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_ENTRY_H_
#define CUCKOODB_ENTRY_H_

#include <unistd.h>
#include <thread>
#include <string.h>

namespace cdb{

//日志结构存储 不会删除 而是将删除的条目一样加入到后面
//使用 EntrType作为标记 
enum class EntryType{
  Put_Or_Get,
  Delete
};

struct Entry{
  std::thread::id tid;
  //WriteOptions write_options;
  EntryType op_type;
  std::string key;
  std::string value;//可能过大被拆分

  //TO-DO 支持大value的写 可以作拆分
  uint64_t offset;//该entry对于其所属的某个大value的偏移
  bool is_large;

  uint64_t crc32;
};


}//namespace cdb

#endif
