/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-21 16:30
 * Filename      : status.cc
 * Description   : 
 * *******************************************************/
// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#include "util/status.h"

namespace cdb {

std::string Status::ToString() const {
  if (message1_ == "") {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOK:
        type = "OK";
        break;
      case kNotFound:
        type = "Not found: ";
        break;
      case kRemoveEntry:
        type = "Remove order: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code (%d): ",
                 static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    result.append(message1_);
    if (message2_.size() > 0) {
      result.append(" - ");
      result.append(message2_);
    }
    return result;
  }
}

};
