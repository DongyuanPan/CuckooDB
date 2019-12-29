/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2019-12-22 00:15
 * Filename      : data_file_format.h
 * Description   : 定义文件的头部和尾部 的格式和序列化、反序列化函数
 * *******************************************************/
#ifndef CUCKOODB_DATA_FILE_FORMAT_H_
#define CUCKOODB_DATA_FILE_FORMAT_H_

#include <thread>
#include <string>

#include "util/status.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/logger.h"
#include "util/options.h"


namespace cdb {

enum FileType {
  kUnknownType            = 0x0,
  kUncompactedRegularType = 0x1,
  kCompactedRegularType   = 0x2,
};

//数据文件 头部格式
struct DataFileHeader {
  //data
  uint32_t crc32;
  uint32_t version;
  uint32_t filetype;
  uint64_t timestamp; 


  DataFileHeader() {
    filetype = 0;
  }

  static uint32_t GetFixedSize() {
    return 20;
  }

  //data geter and setter
  bool IsTypeCompacted() {
    return (filetype & kCompactedRegularType);
  }  

  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct DataFileHeader *output,
                           struct Options *db_options_out=nullptr) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in, &(output->crc32));
    GetFixed32(buffer_in + 4, &(output->version));
    GetFixed32(buffer_in + 8, &(output->filetype));
    GetFixed64(buffer_in + 12, &(output->timestamp));
    
    //重新计算 CRC32 进行检验数据
    uint32_t crc32_computed = crc32c::Value(buffer_in + 4, 16);
    if (crc32_computed != output->crc32)   return Status::IOError("Invalid checksum");

    if (db_options_out == nullptr) return Status::OK();
    //Status s = Options::DecodeFrom(buffer_in + GetFixedSize(), num_bytes_max - GetFixedSize(), db_options_out);
    //return s;

  }


  static Status EncodeTo(const struct DataFileHeader *input, const struct Options* db_options, char* buffer) {
    EncodeFixed32(buffer + 4, input->version);//4
    EncodeFixed32(buffer + 8, input->filetype);//4
    EncodeFixed64(buffer + 12, input->timestamp);//8
    uint32_t crc32 = crc32c::Value(buffer + 4, 16);
    //头部4个字节放 CRC32校验码
    EncodeFixed32(buffer, crc32);

    //以上一共20字节
    return GetFixedSize();
  }  

};

//数据文件 尾部格式
struct DateFileFooter {
  //data
  uint32_t filetype;
  uint32_t flags;
  uint64_t offset_indexes;
  uint64_t num_entries;
  uint32_t crc32;  

  //一共36个字节
  static uint32_t GetFixedSize() {
    return 36;
  }

  bool IsTypeCompacted() {
    return (filetype & kCompactedRegularType);
  }  

  void SetFlagHasInvalidEntries() {
    flags = 1;
  }  

  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct DateFileFooter *output) {
    if (num_bytes_max < GetFixedSize()) return Status::IOError("Decoding error");
    GetFixed32(buffer_in,      &(output->filetype));
    GetFixed32(buffer_in +  4, &(output->flags));
    GetFixed64(buffer_in +  8, &(output->offset_indexes));
    GetFixed64(buffer_in + 16, &(output->num_entries));
    GetFixed32(buffer_in + 24, &(output->crc32));
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct DateFileFooter *input,
                           char* buffer) {
    EncodeFixed32(buffer,      input->filetype);
    EncodeFixed32(buffer +  4, input->flags);
    EncodeFixed64(buffer +  8, input->offset_indexes);
    EncodeFixed64(buffer + 16, input->num_entries);
    // the checksum is computed in the method that writes the footer
    return GetFixedSize();
  }  

};

// 数据文件 HintFile 可以读取这里快速构建索引
// 在重建hash表时，就不需要再扫描所有data file文件，而仅仅需要将hint file中的数据一行行读取并重建即可。
// 大大提高了利用数据文件重启数据库的速度。
struct HintFile {
  uint64_t hashed_key;
  uint32_t offset_entry;

  static Status DecodeFrom(const char* buffer_in,
                           uint64_t num_bytes_max,
                           struct HintFile *output,
                           uint32_t *num_bytes_read) {
    int length;
    char *ptr = const_cast<char*>(buffer_in);
    int size = num_bytes_max;

    length = GetVarint64(ptr, size, &(output->hashed_key));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    length = GetVarint32(ptr, size, &(output->offset_entry));
    if (length == -1) return Status::IOError("Decoding error");
    ptr += length;
    size -= length;

    *num_bytes_read = num_bytes_max - size;
    return Status::OK();
  }

  static uint32_t EncodeTo(const struct HintFile *input, char* buffer) {
    char *ptr;
    ptr = EncodeVarint64(buffer, input->hashed_key);
    ptr = EncodeVarint32(ptr, input->offset_entry);
    return (ptr - buffer);
  }    
};



}

#endif
