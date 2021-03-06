/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2019-12-22 14:25
 * Filename      : entry_format.h
 * Description   : 定义数据项的 格式和序列化/反序列化 函数
 * *******************************************************/

#ifndef CUCKOODB_ENTRY_ORMAT_H_
#define CUCKOODB_ENTRY_ORMAT_H_

#include <thread>
#include <string>

#include "util/status.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/logger.h"
#include "util/options.h"

namespace cdb {  

enum HeaderFlag {
  Delete = 0x1,
  Merge = 0x2,
  EntryFull = 0x4
};    

  struct EntryHeader {
    EntryHeader() { flags = 0; }
    //crc32 校验
    uint32_t crc32;
    //entry的状态
    uint32_t flags;
    uint64_t timestamp;
    uint64_t size_key;
    uint64_t size_value; 
    uint64_t hash;

    // key
    // value

    void SetDelete() {
      flags |= Delete;
    }

    bool IsTypeDelete() {
      log::trace("IsTypeDelete()", "flags %u", flags);
      return (flags & Delete);
    }

    void SetPut() {
        //
    }

    bool IsMerge() {
        return (flags & Merge);
    }

    void SetMerge(bool is_Merge) {
      if (is_Merge){
        //置位 表示已经 合并过了
        flags |= Merge;
      } else {
        //复位 待合并
        flags &= ~Merge;
      }
    }

    int32_t size_header_serialized;

    static uint32_t EncodeTo(const Options& db_options,
                            const struct EntryHeader *input,
                            char* buffer) {

        char *ptr = buffer;
        EncodeFixed32(ptr, input->crc32);
        ptr = EncodeVarint32(ptr + 4, input->flags);
        ptr = EncodeVarint64(ptr, input->timestamp);
        ptr = EncodeVarint64(ptr, input->size_key);
        ptr = EncodeVarint64(ptr, input->size_value);

        EncodeFixed64(ptr, input->hash);
        ptr += 8;

        return (ptr - buffer);
    }

    static Status DecodeFrom(const Options& db_options,
                            const ReadOptions& read_options,
                            const char* buffer_in,
                            uint64_t num_bytes_max,
                            struct EntryHeader *output,
                            uint32_t *num_bytes_read) {

        int length;
        char *buffer = const_cast<char*>(buffer_in);
        char *ptr = buffer;
        int size = num_bytes_max;
        
        GetFixed32(ptr, &(output->crc32));
        ptr += 4;
        size -= 4;

        length = GetVarint32(ptr, size, &(output->flags));
        if (length == -1) return Status::IOError("Decoding error");
        ptr += length;
        size -= length;


        length = GetVarint64(ptr, size, &(output->timestamp));
        if (length == -1) return Status::IOError("Decoding error");
        ptr += length;
        size -= length;

        length = GetVarint64(ptr, size, &(output->size_key));
        if (length == -1) return Status::IOError("Decoding error");
        ptr += length;
        size -= length;

        length = GetVarint64(ptr, size, &(output->size_value));
        if (length == -1) return Status::IOError("Decoding error");
        ptr += length;
        size -= length;

        if (size < 8) return Status::IOError("Decoding error");
        GetFixed64(ptr, &(output->hash));
        ptr += 8;
        size -= 8;

        *num_bytes_read = num_bytes_max - size;
        output->size_header_serialized = *num_bytes_read;

        log::trace("EntryHeader::DecodeFrom", "size:%u", *num_bytes_read);
        return Status::OK();
    }  

  };
}// end namespace cdb

#endif // CUCKOODB_ENTRY_ORMAT_H_
