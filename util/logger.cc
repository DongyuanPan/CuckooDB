/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-21 16:27
 * Filename      : logger.cc
 * Description   : 
 * *******************************************************/

#include "util/logger.h"

namespace cdb {

bool Logger::is_syslog_open_ = false;
int Logger::level_ = Logger::kLogLevelSILENT;
int Logger::log_target_ = Logger::kLogTargetStderr;
std::string Logger::syslog_ident_ = "cdb";
std::mutex Logger::mutex_;

}
