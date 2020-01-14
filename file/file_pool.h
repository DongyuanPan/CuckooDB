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
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

namespace cdb {

struct FileResource {
uint32_t fileid;
int fd;
uint64_t filesize;
char* mmap;
int num_references;
};

class FilePool {
 public:
  FilePool() {}
  ~FilePool() {}

  Status GetFile(uint32_t fileid, const std::string& filepath, uint64_t filesize, FileResource* file) {
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
    if (it != files_unused.end()) {
      *file = *it;
      files_unused.erase(it);
      file->num_references = 1;
      files_used.insert(std::pair<uint32_t, FileResource>(fileid, *file));
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
      log::emerg("FilePool::Mmap()::ctor()", "Could not open file [%s]: %s", filepath.c_str(), strerror(errno));
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
    files_used.insert(std::pair<uint32_t, FileResource>(fileid, *file));

    return Status::OK();                                       
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
  std::unordered_multimap<uint32_t, FileResource> files_used;
};

class FileUtil {
 public:

  static void increase_limit_open_files() {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
      // TODO: linux compatibility
      //rl.rlim_cur = OPEN_MAX;
      rl.rlim_cur = 4096;
      if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        fprintf(stderr, "Could not increase the limit of open files for this process");
      }
    }
  }

  static std::string cdb_getcwd() {
    static char *path = nullptr;
    if (path != nullptr) {
      return path;
    }
    char *buffer = nullptr;
    int size = 64;
    do {
      buffer = new char[size];
      if (getcwd(buffer, size) != NULL) {
        break;
      }
      size *= 2;
      delete[] buffer;
    } while(true);
    path = buffer;
    return path;
  }

  // NOTE: Pre-allocating disk space is very tricky: fallocate() and
  //       posix_fallocate() are not supported on all platforms, glibc sometimes
  //       overrides posix_fallocate() with no warnings at all, and for some
  //       filesystems that do not support any pre-allocation, the operating
  //       system can fall back on writing zero bytes in the entire file, making
  //       the operation painfully slow.
  //       The code below is what SQLite and glibc are doing to fake
  //       fallocate(), thus I am employing the same approach, not even checking
  //       if a version of fallocate() was already given by the OS. This may not
  //       be the fastest way to pre-allocate files, but at least it won't be
  //       as slow as zeroing entire files.
  static Status fallocate(int fd, int64_t length) {
    // The code below was copied from fcntlSizeHint() in SQLite (public domain),
    // and modified for clarity and to fit KingDB's needs.
    
    /* If the OS does not have posix_fallocate(), fake it. First use
    ** ftruncate() to set the file size, then write a single byte to
    ** the last byte in each block within the extended region. This
    ** is the same technique used by glibc to implement posix_fallocate()
    ** on systems that do not have a real fallocate() system call.
    */
    struct stat buf;
    if (fstat(fd, &buf) != 0) return Status::IOError("kingdb_fallocate() - fstat()", strerror(errno));
    if (buf.st_size >= length) return Status::IOError("kingdb_fallocate()", "buf.st_size >= length");

    const int blocksize = buf.st_blksize;
    if (!blocksize) return Status::IOError("kingdb_fallocate()", "Invalid block size");
    if (ftruncate(fd, length) != 0) return Status::IOError("kingdb_fallocate() - ftruncate()", strerror(errno));

    int num_bytes_written;
    int64_t offset_write = ((buf.st_size + 2 * blocksize - 1) / blocksize) * blocksize - 1;
    do {
      num_bytes_written = 0;
      if (lseek(fd, offset_write, SEEK_SET) == offset_write) {
        num_bytes_written = write(fd, "", 1);
      }
      offset_write += blocksize;
    } while (num_bytes_written == 1 && offset_write < length);
    if (num_bytes_written != 1) return Status::IOError("kingdb_fallocate() - write()", strerror(errno));
    return Status::OK();
  }

  static Status fallocate_filepath(std::string filepath, int64_t length) {
    int fd;
    if ((fd = open(filepath.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
      return Status::IOError("kingdb_fallocate_filepath() - open()", strerror(errno));
    }
    Status s = fallocate(fd, length);
    close(fd);
    return s;
  }


  static int64_t fs_free_space(const char *filepath) {
    struct statvfs stat;
    if (statvfs(filepath, &stat) != 0) {
      log::trace("disk_free_space()", "statvfs() error");
      return -1;
    }

    if (stat.f_frsize) {
      return stat.f_frsize * stat.f_bavail; 
    } else {
      return stat.f_bsize * stat.f_bavail; 
    }
  }

  static Status remove_files_with_prefix(const char *dirpath, const std::string prefix) {
    DIR *directory;
    struct dirent *entry;
    if ((directory = opendir(dirpath)) == NULL) {
      return Status::IOError("Could not open directory", dirpath);
    }
    char filepath[FileUtil::maximum_path_size()];
    Status s;
    struct stat info;
    while ((entry = readdir(directory)) != NULL) {
      int ret = snprintf(filepath, FileUtil::maximum_path_size(), "%s/%s", dirpath, entry->d_name);
      if (ret < 0 || ret >= FileUtil::maximum_path_size()) {
        log::emerg("remove_files_with_prefix()",
                  "Filepath buffer is too small, could not build the filepath string for file [%s]", entry->d_name); 
        continue;
      }
      if (   strncmp(entry->d_name, prefix.c_str(), prefix.size()) != 0
          || stat(filepath, &info) != 0
          || !(info.st_mode & S_IFREG)) {
        continue;
      }
      if (std::remove(filepath)) {
        log::warn("remove_files_with_prefix()", "Could not remove file [%s]", filepath);
      }
    }
    closedir(directory);
    return Status::OK();
  }

  static int64_t maximum_path_size() {
    return 4096;
  }

  static int sync_file(int fd) {
    int ret;
#ifdef F_FULLFSYNC
    // For Mac OS X
    ret = fcntl(fd, F_FULLFSYNC);
#else
    ret = fdatasync(fd);
#endif // F_FULLFSYNC
    return ret;
  }
};


}//end nasepace cdb

#endif //CUCKOODB_FILE_POOL_H_