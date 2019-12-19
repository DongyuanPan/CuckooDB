/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-21 16:28
 * Filename      : status.h
 * Description   : 
 * *******************************************************/
// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.
//
// The source code in this file was mostly derived from LevelDB.
// Copyright (c) 2011, The LevelDB Authors. All rights reserved.
// Use of the LevelDB source code is governed by a BSD-style license
// that can be found in the 3rdparty/leveldb/LICENSE file. See
// the 3rdparty/leveldb/AUTHORS file for the names of the LevelDB
// contributors.

#ifndef CUCKOODB_STATUS_H_
#define CUCKOODB_STATUS_H_

#include <string>

namespace cdb {

class Status {
 public:
  Status() { code_ = kOK; message1_ = ""; }
  ~Status() {}
  Status(int code) {
    code_ = code;
  }
  Status(int code, std::string message1, std::string message2)
  {
    code_ = code;
    message1_ = message1;
    message2_ = message2;
  }

  static Status OK() { return Status(); }

  static Status Done() { return Status(kDone); }

  static Status RemoveEntry() { return Status(kRemoveEntry); }

  static Status NotFound(const std::string& message1, const std::string& message2="") {
    return Status(kNotFound, message1, message2);
  }

  static Status InvalidArgument(const std::string& message1, const std::string& message2="") {
    return Status(kInvalidArgument, message1, message2);
  }

  static Status IOError(const std::string& message1, const std::string& message2="") {
    return Status(kIOError, message1, message2);
  }

  bool IsOK() const { return (code() == kOK); }
  bool IsNotFound() const { return code() == kNotFound; }
  bool IsRemoveEntry() const { return code() == kRemoveEntry; }
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }
  bool IsIOError() const { return code() == kIOError; }
  bool IsDone() const { return code() == kDone; }

  std::string ToString() const;


 private:
  int code_;
  std::string message1_;
  std::string message2_;

  int code() const { return code_; };

  enum Code {
    kOK = 0, 
    kNotFound = 1,
    kRemoveEntry = 2,
    kInvalidArgument = 3,
    kIOError = 4,
    kDone = 5
  };
};

}; // end namespace cdb

#endif // CUCKOODB_STATUS_H_
