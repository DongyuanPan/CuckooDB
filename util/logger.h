/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : dongyuanpan0@gmail.com
 * Last modified : 2019-10-21 15:59
 * Filename      : logger.h
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_LOGGER_H_
#define CUCKOODB_LOGGER_H_


#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <time.h>
#include <assert.h>
#include <syslog.h>
#include <string>
#include <sstream>
#include <thread>
#include <mutex>
#include <iostream>
#include <iomanip>
#include <cstdarg>
#include <utility>


namespace cdb{

class Logger {
 public:
  Logger(std::string name) { }
  virtual ~Logger() { }

  static void Logv(bool thread_safe,
                   int level,
                   int level_syslog,
                   const char* logname,
                   const char* format,
                   ...) {
    va_list args;
    va_start(args, format);
    Logv(thread_safe, level, level_syslog, logname, format, args);
    va_end(args);
  }

  static void Logv(bool thread_safe,
                   int level,
                   int level_syslog,
                   const char* logname,
                   const char* format,
                   va_list args) {
    if (log_target_ == Logger::kLogTargetStderr) {
      if (level > current_level()) return;
      if (thread_safe) mutex_.lock();
    }

    char buffer[512];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 1024 * 30;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      std::ostringstream ss;
      ss << std::this_thread::get_id();

      if (log_target_ == Logger::kLogTargetStderr) {
        struct timeval now_tv;
        gettimeofday(&now_tv, NULL);
        const time_t seconds = now_tv.tv_sec;
        struct tm t;
        localtime_r(&seconds, &t);
        p += snprintf(p, limit - p,
                      "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
                      t.tm_year + 1900,
                      t.tm_mon + 1,
                      t.tm_mday,
                      t.tm_hour,
                      t.tm_min,
                      t.tm_sec,
                      static_cast<int>(now_tv.tv_usec),
                      ss.str().c_str());
      } else if (log_target_ == Logger::kLogTargetSyslog) {
        p += snprintf(p, limit - p,
                      "%s ",
                      ss.str().c_str());
      }

      if (p < limit) {
        va_list backup_args;
        va_copy(backup_args, args);
        p += vsnprintf(p, limit - p, format, backup_args);
        va_end(backup_args);
      } else {
        // Truncate to available space if necessary
        if (iter == 0) {
          continue;
        } else {
          p = limit - 1;
        }
      }

      if (p == base || p[-1] != '\0') {
        *p++ = '\0';
      }

      if (log_target_ == Logger::kLogTargetStderr) {
        fprintf(stderr, "%s\n", base);
      } else if (log_target_ == Logger::kLogTargetSyslog) {
        if (!Logger::is_syslog_open_) {
          openlog(syslog_ident_.c_str(), 0, LOG_USER);
          Logger::is_syslog_open_ = true;
        }
        syslog(LOG_USER | level_syslog, "%s", base);
      }

      if (base != buffer) {
        delete[] base;
      }
      break;
    }

    if (log_target_ == Logger::kLogTargetStderr && thread_safe) {
      mutex_.unlock();
    }
  }

  static int current_level() { return level_; }
  static void set_current_level(int l) { level_ = l; }
  static int set_current_level(const char* l_in) {
    std::string l(l_in);
    std::transform(l.begin(), l.end(), l.begin(), ::tolower);
    if (l == "silent") {
      set_current_level(kLogLevelSILENT);
    } else if (l == "emerg") {
      set_current_level(kLogLevelEMERG);
    } else if (l == "alert") {
      set_current_level(kLogLevelALERT);
    } else if (l == "crit") {
      set_current_level(kLogLevelCRIT);
    } else if (l == "error") {
      set_current_level(kLogLevelERROR);
    } else if (l == "warn") {
      set_current_level(kLogLevelWARN);
    } else if (l == "notice") {
      set_current_level(kLogLevelNOTICE);
    } else if (l == "info") {
      set_current_level(kLogLevelINFO);
    } else if (l == "debug") {
      set_current_level(kLogLevelDEBUG);
    } else if (l == "trace") {
      set_current_level(kLogLevelTRACE);
    } else {
      return -1;
    }
    return 0;
  }

  static void set_target(std::string log_target) {
    if (log_target == "stderr") {
      cdb::Logger::log_target_ = cdb::Logger::kLogTargetStderr;
    } else {
      cdb::Logger::log_target_ = cdb::Logger::kLogTargetSyslog;
      cdb::Logger::syslog_ident_ = log_target;
    }
  }

  enum Loglevel {
    kLogLevelSILENT=0,
    kLogLevelEMERG=1,
    kLogLevelALERT=2,
    kLogLevelCRIT=3,
    kLogLevelERROR=4,
    kLogLevelWARN=5,
    kLogLevelNOTICE=6,
    kLogLevelINFO=7,
    kLogLevelDEBUG=8,
    kLogLevelTRACE=9
  };

  enum Logtarget {
    kLogTargetStderr=0,
    kLogTargetSyslog=1
  };

 private:
  static bool is_syslog_open_;
  static int level_;
  static std::mutex mutex_;
  static int log_target_;
  static std::string syslog_ident_;
};


class log {
 public:
  static void emerg(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(false, Logger::kLogLevelEMERG, LOG_EMERG, logname, format, args);
    va_end(args);
  }

  static void alert(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelALERT, LOG_ALERT, logname, format, args);
    va_end(args);
  }

  static void crit(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelCRIT, LOG_CRIT, logname, format, args);
    va_end(args);
  }

  static void error(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelERROR, LOG_ERR, logname, format, args);
    va_end(args);
  }

  static void warn(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelWARN, LOG_WARNING, logname, format, args);
    va_end(args);
  }

  static void notice(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelNOTICE, LOG_NOTICE, logname, format, args);
    va_end(args);
  }

    static void info(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelINFO, LOG_INFO, logname, format, args);
    va_end(args);
  }

  static void debug(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    Logger::Logv(true, Logger::kLogLevelDEBUG, LOG_DEBUG, logname, format, args);
    va_end(args);
  }

  static void trace(const char* logname, const char* format, ...) {
    va_list args;
    va_start(args, format);
    // No TRACE level in syslog, so using DEBUG instead
    Logger::Logv(true, Logger::kLogLevelTRACE, LOG_DEBUG, logname, format, args);
    va_end(args);
  }
};


}

#endif
