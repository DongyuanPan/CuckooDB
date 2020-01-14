/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2020-01-14 17:00
 * Filename      : load_datebase.cc
 * Description   : 
 * *******************************************************/
#include <iostream>
#include <thread>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include "util/logger.h"
#include "util/status.h"
#include "db/cuckoodb.h"
#include "util/options.h"

char get(){
  return static_cast<char>('a' + rand()%('z'-'a'+1));
}

int main(){
  std::unordered_map<std::string, std::string> map;
  cdb::Logger::set_current_level("trace");
  cdb::Options db_options;
  cdb::CuckooDB db(db_options, "testdb");
  cdb::WriteOptions write_options;
  cdb::ReadOptions read_options;

  sleep(2);

  bool flag = true;
  for (int i = 0; i < 5; ++i){
    std::string key = std::to_string(i);
    std::string value;
    db.Get(read_options, key, &value);
    std::cout << value<< std::endl;
    if (value == "")
      flag = false;
  }
  
  if (flag)
    std::cout << "success load" << std::endl;
  else
    std::cout << "faild load" << std::endl;

  return 0;
}
