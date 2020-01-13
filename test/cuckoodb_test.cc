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
  int n = 5;
  for (int i = 0; i < n; ++i){
    std::string s;
    for (int j = 0; j < 10; ++j){
      s += get();
    }
    std::string key = std::to_string(i);
    std::cout << key <<" "<< s<<std::endl;
    db.Put(write_options, key, s);
    map[key] = s;
  }

  sleep(1);

  bool flag = true;
  for (int i = 0; i < n; ++i){
    std::string key = std::to_string(i);
    std::string value;
    db.Get(read_options, key, &value);
    if (map[key] != value)
      flag = false;
  }

  sleep(1);

  if (flag)
    std::cout << "success Put and Get" << std::endl;
  std::string ke = "0";
  std::string va;
  db.Delete(write_options, ke);
  cdb::Status s = db.Get(read_options, ke, &va);
  if (s.IsNotFound()){
	  std::cout << s.ToString()<<std::endl;
  }

  return 0;
}
