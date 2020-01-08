/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2020-01-08 14:50
 * Filename      : file_pool.cc
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_FILE_POOL_H_
#define CUCKOODB_FILE_POOL_H_

#include <map>
#include <mutex>
#include <vector>
#include <cinttypes>

namespace cdb {

struct FileResource {
uint32_t fileid;
int fd;
uint64_t filesize;
char* mmap;
int num_references;
}

class FileManager {
 public:
  FileManager() {}
  ~FileManager() {}

  Status GetFile(uint32_t fileid, const edt::string& filepath. uint64_t filesize, FileResource* file) {
    std::unique_lock<std::mutex> lock(mutex_);
    bool found = false;

    auto it = files_unused.begin();
    //mmap 共享指针，文件大小改变的话 会导致 mmap访问非法的位置
    while(it != files_unused.end()) {
      if (it->fileid != fileid) {
        ++it;
      } else if (it->filesize == filesize){
        break;
      } else {
        //同一个 fileid 的文件 他们的大小不一样，那么发生了文件大小的改变，文件变小了
        //就会法导致mmap严重错误，因此释放该资源
        munmap(it->mmap, it->filesize);
        close(it->fd);
        it = files_unused.erase(it);
      }
    }
    
    //在 files_unused 中找到
    if (it != files_unused.end)()) {
      file = *it;
      files_unused.erase(it);
      file->num_references = 1;
      files_used.insert(std::pair<uint32_t, FileResource>(fileid, * file));
      found = true;

    } else {
    //在file_used 中找
      auto range = files_used.equal_range(fileid);
      auto it = range.first;
      for (; it != range.second; ++it) {
        if (it->second.filesize == filesize) break;
      }

      if (it != range.second) {
        it->second.num_references += 1;
        *file = it->second;
        found = true;
      }        
    }

    if (found) return Status::OK();

    //释放
    if (NumFiles() > MaxNumFiles() && !files_unused.empty()) {
      auto it = files_unused.begin();
      munmap(it->mmap, it->filesize);
      close(it->fd);
      files_unused.erase(it);  
    }

    int fd = 0;
    //打开文件
    if ((fd = open(filepath.c_str(), O_RDONLY)) < 0) {
      log::emerg("FileManager::Mmap()::ctor()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
      return Status::IOError("Could not open() file");        
    }
    
    char* datafile = static_cast<char*>(mmap(0,
                                             filesize,
                                             PROT_READ,
                                             MAP_SHARED,
                                             fd,
                                             0));

    if (datafile == MAP_FAILED){
      log::emerg("Could not mmap() file [%s]: %s", filepath.c_str(), strerror(errno));
      return Status::IOError("Could not mmap() file");      
    }

    file->fileid = fileid;
    file->filesize = filesize;
    file->fd = fd;
    file->mmap = datafile;
    file->num_references = 1;
    files_used.insert(std::pair<uint32_t, FileManager>(fileid, *file));

    return Status::OK();                                       
  }
}

  void ReleaseFile(uint32_t fileid, uint64_t filesize) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto range = files_used.equal_range(fileid);
    auto it = range.first;
    for (; it != range.second; ++it) {
      if (it->second.filesize == filesize) break;
    }
    if (it == range.second) return;

    if (it->second.num_references > 1) {
      it->second.num_references -= 1;
    } else {
      it->second.num_references = 0;
      files_unused.push_back(it->second);
      files_used.erase(it);
    }
  }

int NumFiles() {
return files_unused.size() + files_used.size();
}

int MaxNumFiles() {
return 2048; 
}

private:
std::mutex mutex_;
std::vector<FileResource> files_unused;
std::unordered_multimap<uint32_t, FileResource> files_used:
}


}//end nasepace cdb

#endif //CUCKOODB_FILE_POOL_H_