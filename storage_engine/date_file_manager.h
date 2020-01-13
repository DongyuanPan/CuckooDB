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
#define CUCKOODB_DATA_FILE_MANAGER_H_

#include <thread>
#include <mutex>
#include <unordered_map>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>

#include <chrono>
#include <vector>
#include <set>
#include <algorithm>
#include <cstdio>
#include <cinttypes>

#include "file/file_resource_manager.h"

#include "util/const_value.h"
#include "util/options.h"
#include "util/entry.h"
#include "util/status.h"
#include "util/logger.h"
#include "util/xxhash.h"
#include "util/const_value.h"
#include "data_file_format.h"
#include "entry_format.h"




namespace cdb{

class DateFileManager {
  public:
    DateFileManager(cdb::Options& db_options,
                    std::string dbname,
                    FileType filetype_default,
                    bool read_only = false)
            : dbname_(dbname),
              db_options_(db_options),
              is_read_only_(read_only),
              fileid_(0),
              sequence_fileid_(0),
              sequence_timestamp_(0),
              filetype_default_(filetype_default) {
      if (!is_read_only_) {
        buffer_raw_ = new char[size_block_*2];
        buffer_index_ = new char[size_block_*2];
      } 
      
      size_block_ = SIZE_DATA_FILE;
      has_file_ = false;
      buffer_has_items_ = false;
      has_sync_option_ = false;
      prefix_ = "";
      prefix_compaction_ = "";
      dirpath_locks_ = "";

    }

    ~DateFileManager() {
        std::unique_lock<std::mutex> lock(mutex_close_);
        if (is_read_only_ || is_close_) return;
        is_close_ = true;
        FlushCurrentFile();
        CloseFile();
    }


    static std::string num_to_hex(uint64_t num) {
      char buffer[20];
      sprintf(buffer, "%08" PRIx64, num);
      return std::string(buffer);
    }
  
    static uint32_t hex_to_num(char* hex) {
      uint32_t num;
      sscanf(hex, "%x", &num);
      return num;
    }

    //文件 ID 原子的加
    uint32_t IncrementSequenceFileId(uint32_t inc) {
        std::unique_lock<std::mutex> lock(mutex_sequence_fileid_);
        log::trace("DateFileManager::IncrementSequenceFileId", "sequence_fileid_:%u, inc:%u", sequence_fileid_, inc);
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

    void SetSequenceTimestamp(uint32_t seq) {
      std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
      if (!is_locked_sequence_timestamp_) sequence_timestamp_ = seq;
    }

    uint64_t GetSequenceTimestamp() {
      std::unique_lock<std::mutex> lock(mutex_sequence_timestamp_);
      return sequence_timestamp_;
    }

    std::string GetPrefix() {
      return prefix_;
    }

    std::string GetFilepath(uint32_t fileid) {
      return dbname_ + "/" + prefix_ + DateFileManager::num_to_hex(fileid); // TODO: optimize here
    }

    std::string GetLockFilepath(uint32_t fileid) {
      return dirpath_locks_ + "/" + DateFileManager::num_to_hex(fileid); // TODO: optimize here
    }    

    void OpenNewFile() {

        IncrementSequenceFileId(1);
        IncrementSequenceTimestamp(1);      

        filepath_ = GetFilepath(GetSequenceFileId());
        log::trace("DateFileManager::OpenNewFile()", "Opening file [%s]: %u", filepath_.c_str(), GetSequenceFileId());
        
        while (true) {
          if ((fd_ = open(filepath_.c_str(), O_WRONLY|O_CREAT, 0644)) < 0) {
            log::emerg("DateFileManager::OpenNewFile()", "Could not open file [%s]: %s", filepath_.c_str(), strerror(errno));
            wait_until_can_open_new_files_ = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
            continue;
          }
          wait_until_can_open_new_files_ = false;
          break;
        }

        has_file_ = true;
        fileid_ = GetSequenceFileId();
        timestamp_ = GetSequenceTimestamp();

        // 为头部 预留空间
        offset_start_ = 0;
        offset_end_ = db_options_.internal__datafile_header_size;

        // 填充 文件固定头部
        struct DataFileHeader datafileheader;
        datafileheader.filetype  = filetype_default_;
        datafileheader.timestamp = timestamp_;
        DataFileHeader::EncodeTo(&datafileheader, &db_options_, buffer_raw_);    
        log::trace("DateFileManager::OpenNewFile()", "Opening file [%s]: %u success", filepath_.c_str(), GetSequenceFileId());    
    }


    void CloseFile() {
      if (!has_file_) return;
      log::trace("DateFileManager::CloseFile()", "ENTER - fileid_:%d", fileid_);

      FlushHintDate();

      close(fd_);
      buffer_has_items_ = false;
      has_file_ = false;
    }      

    Status FlushHintDate() {
      if (!has_file_) return Status::OK();
      uint32_t num = file_resource_manager.GetNumWritesInProgress(fileid_);
      log::trace("DateFileManager::FlushHintArray()", "ENTER - fileid_:%d - num_writes_in_progress:%u", fileid_, num);
      if (file_resource_manager.GetNumWritesInProgress(fileid_) == 0) {
        uint64_t size_offarray;
        file_resource_manager.SetFileSize(fileid_, offset_end_);
        if (ftruncate(fd_, offset_end_) < 0) {
          return Status::IOError("DateFileManager::FlushHintArray()", strerror(errno));
        }
        Status s = WriteHintData(fd_, 
                                 file_resource_manager.GetHintData(fileid_), 
                                 &size_offarray, 
                                 filetype_default_, 
                                 file_resource_manager.HasPaddingInValues(fileid_), 
                                 false);
        uint64_t filesize = file_resource_manager.GetFileSize(fileid_);
        file_resource_manager.SetFileSize(fileid_, filesize + size_offarray);
        return s;
      }
      return Status::OK();
    }

    Status WriteHintData(int fd,
                          const std::vector< std::pair<uint64_t, uint32_t> >& offarray_current,
                          uint64_t* size_out,
                          FileType filetype,
                          bool has_padding_in_values,
                          bool has_invalid_entries) {
      uint64_t offset = 0;
      struct HintData row;

      //添加 HintDate 固化索引
      for (auto& p:offarray_current) {
        row.hashed_key = p.first;
        row.offset_entry = p.second;
        uint32_t length = HintData::EncodeTo(&row, buffer_index_ + offset);
        offset += length;
        log::trace("DateFileManager::WriteHintData()", "hashed_key:[0x%" PRIx64 "] offset:[0x%08x]", p.first, p.second);
      }

      //记录文件末尾（索引开始写入位置） 然后 构造Footer 最后 写入 HintDate和Footer
      int64_t position = lseek(fd, 0, SEEK_END);
      if (position < 0) {
        return Status::IOError("DateFileManager::WriteHintData()", strerror(errno));
      }
      log::trace("DateFileManager::WriteHintData()", "file position:[%" PRIu64 "]", position);    

      struct DateFileFooter footer;
      footer.filetype = filetype;
      footer.offset_indexes = position;
      footer.num_entries = offarray_current.size();
      if (has_invalid_entries) footer.SetFlagHasInvalidEntries();
      uint32_t length = DateFileFooter::EncodeTo(&footer, buffer_index_ + offset);
      offset += length;

      uint32_t crc32 = crc32c::Value(buffer_index_, offset - 4);
      EncodeFixed32(buffer_index_ + offset - 4, crc32);

      if (write(fd, buffer_index_, offset) < 0) {
        log::trace("DateFileManager::WriteHintData()", "Error write(): %s", strerror(errno));
      }

      // ftruncate() is necessary in case the file system space for the file was pre-allocated 
      if (ftruncate(fd, position + offset) < 0) {
        return Status::IOError("DateFileManager::WriteHintData()", strerror(errno));
      } 

      *size_out = offset;
      log::trace("DateFileManager::WriteHintData()", "offset_indexes:%u, num_entries:[%lu]", position, offarray_current.size());
      return Status::OK();     

    }


    uint32_t FlushCurrentFile(int force_new_file=0, uint64_t padding=0) {
      if (!has_file_)
        return 0;
      uint32_t fileid_out = fileid_;
      log::trace("DateFileManager::FlushCurrentFile()", "ENTER - fileid_:%d, has_file_:%d, buffer_has_items_:%d", fileid_, has_file_, buffer_has_items_);
      
      if (has_file_ && buffer_has_items_) {
        log::trace("DateFileManager::FlushCurrentFile()", "has_files && buffer_has_items_ - fileid_:%d", fileid_);
        if (write(fd_, buffer_raw_ + offset_start_, offset_end_ - offset_start_) < 0) {
          log::emerg("DateFileManager::FlushCurrentFile()", "Error write(): %s", strerror(errno));
          return 0;
        }
        //写入文件后 更新文件大小 （元数据）
        file_resource_manager.SetFileSize(fileid_, offset_end_);
        offset_start_ = offset_end_;
        buffer_has_items_ = false;
        log::trace("DateFileManager::FlushCurrentFile()", "items written - offset_end_:%d | size_block_:%d | force_new_file:%d", offset_end_, size_block_, force_new_file);
      }

      //强制操作系统立即直接刷新到硬盘上
      if (has_sync_option_) {
        has_sync_option_ = false;
        if (fdatasync(fd_) < 0) {
          log::emerg("DateFileManager::FlushCurrentFile()", "Error sync_file(): %s", strerror(errno));
        }
      }  

      //文件超出 规定大小 或者 强制新建文件 则关闭当前文件，write会自己新建文件
      if (offset_end_ >= size_block_ || (force_new_file && offset_end_ > db_options_.internal__datafile_header_size)) {
        log::trace("DateFileManager::FlushCurrentFile()", "file renewed - force_new_file:%d", force_new_file);
        file_resource_manager.SetFileSize(fileid_, offset_end_);
        CloseFile();
      } 

      log::trace("DateFileManager::FlushCurrentFile()", "done!");
      return fileid_out;
    }   

    uint64_t Write(Entry& entry, uint64_t hashed_key) {
      log::trace("DataFileManager::Write()", "entry key: %s, hashed_key: %llu", entry.key.c_str(), hashed_key);
      struct EntryHeader entry_header;
      uint64_t index = 0;

      if (entry.write_options.sync) {
        has_sync_option_ = true;
      } 

      if (entry.op_type == EntryType::Put_Or_Get){

        entry_header.SetPut();
        entry_header.crc32 = entry.crc32;
        entry_header.size_key = entry.key.size();
        entry_header.size_value = entry.value.size();
        entry_header.hash = hashed_key;

        entry_header.SetMerge(false);

        //序列化 写入Entry 头部
        uint32_t size_header = EntryHeader::EncodeTo(db_options_, &entry_header, buffer_raw_ + offset_end_);
        //写入 key 和 value
        memcpy(buffer_raw_ + offset_end_ + size_header, entry.key.data(), entry.key.size());
        memcpy(buffer_raw_ + offset_end_ + size_header + entry.key.size(), entry.value.data(), entry.value.size());
      
        //回传 索引信息    文件ID和文件中该Entry的偏移
        uint64_t file_id_hight = fileid_;
        file_id_hight = file_id_hight << 32;
        index = file_id_hight | offset_end_;
        log::trace("DataFileManager::Write()", "entry key: %s, offset_end_ % llu", entry.key.c_str(), offset_end_);
        //记录  索引数据  准备固化到硬盘
        file_resource_manager.AddHintData(fileid_, std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
        //更新 偏移
        offset_end_ += size_header + entry.key.size() + entry.value.size();
      } else {
        entry_header.SetDelete();
        entry_header.size_key = entry.key.size();
        entry_header.size_value = 0;
        // entry_header.hash = hashed_key;
        entry_header.crc32 = entry.crc32;
        entry_header.SetMerge(false);

        //序列化 写入Entry 头部
        uint32_t size_header = EntryHeader::EncodeTo(db_options_, &entry_header, buffer_raw_ + offset_end_);
        //写入 key
        memcpy(buffer_raw_ + offset_end_ + size_header, entry.key.c_str(), entry.key.size());

        //回传 索引信息    文件ID和文件中该Entry的偏移
        uint64_t file_id_hight = fileid_;
        file_id_hight = file_id_hight << 32;
        index = file_id_hight | offset_end_;
        log::trace("DataFileManager::Write()", "entry key: %s, offset_end_ % llu", entry.key.c_str(), offset_end_);
        
        //记录  索引数据  准备固化到硬盘
        file_resource_manager.AddHintData(fileid_, std::pair<uint64_t, uint32_t>(hashed_key, offset_end_));
        //更新 偏移
        offset_end_ += size_header + entry.key.size() + entry.value.size();
      }

      return index;
    }

    void WriteEntrys(std::vector<Entry>& entrys, std::multimap<uint64_t, uint64_t>& map_index_out) {
      log::trace("DateFileManager::WriteEntrys()", "got entrys size: %d", entrys.size());
      for (auto& entry:entrys){
          if (! has_file_) OpenNewFile();

          //文件大小 大于最大限制则 刷新
          if (offset_end_ > size_block_) {
            log::trace("DateFileManager::WriteEntrys()", "About to flush - offset_end_: %llu | size_block_: %llu", offset_end_, size_block_);
            FlushCurrentFile(true, 0);        
          }

          //只考虑 小文件的情况下
          log::trace("DateFileManager::WriteEntrys()", "key: [%s] size_value:%llu", entry.key.c_str(), entry.value.size());
          uint64_t hashed_key = XXH64(entry.key.data(), entry.key.size(), 0);

          buffer_has_items_ = true;
          uint64_t index = 0;
          index = Write(entry, hashed_key);

          if (index != 0 ) {
            map_index_out.insert(std::pair<uint64_t, uint64_t>(hashed_key, index));
          } else {
            log::trace("DateFileManager::WriteEntrys()", "Avoided catastrophic location error"); 
          }

      }

      log::trace("DateFileManager::WriteEntrys()", "end flush");
      FlushCurrentFile(false, 0);
    }

  private:
    cdb::Options db_options_;
    std::string dbname_;
    bool is_read_only_;
    bool is_close_;
    int sequence_fileid_;
    uint64_t timestamp_;
    uint64_t sequence_timestamp_;
    

    int size_block_;
    bool has_file_;
    int fd_;
    std::string filepath_;
    uint32_t fileid_;
    uint64_t offset_start_;
    uint64_t offset_end_;
    std::string prefix_;
    std::string prefix_compaction_;
    std::string dirpath_locks_;


    char *buffer_raw_;
    char *buffer_index_;
    bool buffer_has_items_;


    std::mutex mutex_close_;
    std::mutex mutex_sequence_fileid_;
    std::mutex mutex_sequence_timestamp_;
    bool is_locked_sequence_timestamp_;
    bool wait_until_can_open_new_files_;

    FileType filetype_default_;
    bool has_sync_option_;
    
 public:
    cdb::FileResourceManager file_resource_manager;

  

};

}//end namespace cdb


#endif