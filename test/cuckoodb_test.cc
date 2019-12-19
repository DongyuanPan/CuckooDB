#include <iostream>
#include <thread>
#include <string>
#include <unordered_map>
#include "util/logger.h"
#include "util/status.h"
#include "db/cuckoodb.h"

char get(){
  return static_cast<char>('a' + rand()%('z'-'a'+1));
}

int main(){
  std::unordered_map<std::string, std::string> map;
  cdb::Logger::set_current_level("trace");
  cdb::CuckooDB db("testdb");
  int n = 3;
  for (int i = 0; i < n; ++i){
    std::string s;
    for (int j = 0; j < 10; ++j){
      s += get();
    }
    std::string key = std::to_string(i);
    std::cout << key <<" "<< s<<std::endl;
    db.Put(key, s);
    map[key] = s;
  }

  bool flag = true;
  for (int i = 0; i < n; ++i){
    std::string key = std::to_string(i);
    std::string value;
    db.Get(key, &value);
    if (map[key] != value)
      flag = false;
  }

  if (flag)
    std::cout << "success Put and Get" << std::endl;
  std::string ke = "0";
  std::string va;
  db.Delete(ke);
  cdb::Status s = db.Get(ke, &va);
  if (s.IsNotFound()){
	  std::cout << s.ToString()<<std::endl;
  }
  return 0;
}
