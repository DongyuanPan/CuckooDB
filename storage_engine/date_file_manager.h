/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2019-12-21 14:07
 * Filename      : date_file_manager.h
 * Description   : 在任意时间点，只有一个文件是可写的，在Bitcask模型中称其为
 *                 active data file，而其他的已经达到限制大小的文件，称为older data file
 * *******************************************************/

#ifndef CUCKOODB_DATA_FILE_MANAGER_H_
#define UCKOODB_DATA_FILE_MANAGER_H_

#include <thread>
#include <mutex>
#include <unordered_mutilmap>

#include "util/options.h"
#include "util/entry.h"
#include "util/status.h"
#include "util/logger.h"

namespace cdb{

class DateFileManager {
  public:
  hstable_manager_(db_options, dbname, "", prefix_compaction_, dirpath_locks_, kUncompactedRegularType, read_only)
    DateFileManager(DatabaseOptions& db_options,
                    std::string dbname,
                    bool read_only = false)
            : db_options_(db_options),
              id_read_only_(read_only)
    {
        prefix_ = "";
        

    }

    ~DateFileManager() {
        std::unique_lock<std::mutex> lock(mutex_close_);
        if (is_read_only_ || is_close_) return;
        is_close_ = true;
        FlushCurrentFile();
        CloseCurrentFile();
        delete hash_;
    }

    //文件 ID 原子的加
    uint32_t IncrementSequenceFileId(uint32_t inc) {
        std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
        log::trace("HSTableManager::IncrementSequenceFileId", "sequence_fileid_:%u, inc:%u", sequence_fileid_, inc);
        sequence_fileid_ += inc;
        return sequence_fileid_;
    }

    uint32_t GetSequenceFileId() {
        std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
        return sequence_fileid_;
    }    

    //时间轴
    uint64_t IncrementSequenceTimestamp(uint64_t inc) {
        std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
        if (!is_locked_sequence_timestamp_) sequence_timestamp_ += inc;
        return sequence_timestamp_;
    }

    void OpenNewFile() {

        IncrementSequenceFileId(1);
        IncrementSequenceTimestamp(1);
        filepath_ = GetFilepath(GetSequenceFileId());        
        log::trace("ateFileManager::OpenNewFile()");
    }

    uint64_t write(Entry& entry, uint64_t hashed_key) {

    }

    void WriteEntrys(std::vector<Entry>& entrys, std::unordered_multimap<uint64_t, uint64_t> indexs>& map_index_out) {
      for (auto& entry:entrys){
          if (! has_file_) OpenNewFile();

          //文件大小 大于最大限制则 刷新
          if (offset_end_ > size_block_) {
            log::trace("DateFileManager::Write()", "About to flush - offset_end_: %llu | size_block_: %llu", offset_end_, size_block_);
            FlushCurrentFile(true, 0);        
          }

          //只考虑 小文件的情况下
          log::trace("DateFileManager::Write()", "key: [%s] size_value:%llu", entry.key.c_str(), entry.value->size());
          uint64_t hashed_key = HashFunction(entry.key.data(), entry.key.size());

          buffer_has_items_ = true;
          uint64_t location = 0;
          location = Write(entry, hashed_key);

          if (location != 0 ) {
            map_index_out.insert(std::pair<uint64_t, uint64_t>(hashed_key, location))
          } else {
            log::trace("DateFileManager::Write()", "Avoided catastrophic location error"); 
          }

      }

      log::trace("DateFileManager::Write()", "end flush");
      FlushCurrentFile(false, 0);
    }

 private:
  int sequence_fileid_;
  int size_block_;
  bool has_file_;
  int fd_;
  std::string filepath_;
  uint32_t fileid_;
  uint64_t offset_start_;
  uint64_t offset_end_;
  std::string dbname_;
  char *buffer_raw_;
  bool buffer_has_items_;
    


}

}//end namespace cdb


#endif