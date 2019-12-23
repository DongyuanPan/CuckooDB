/**********************************************************
 * Copyright (c) 2019 The CuckooDB Authors. All rights reserved.
 * Author        : Dongyuan Pan
 * Email         : 641234230@qq.com
 * Last modified : 2019-12-23 10:03
 * Filename      : const_value.h
 * Description   : 
 * *******************************************************/

#ifndef CUCKOODB_CONST_VALUE_H_
#define CUCKOODB_CONST_VALUE_H_

#define SIZE_CACHE_WRITE     1024*1024*32 // used by the BufferManager
#define SIZE_BUFFER_RECV      1024*256      // used by server to receive commands from clients
#define SIZE_BUFFER_SEND      1024*1024*32 // used by server to prepare data to send to clients
#define SIZE_BUFFER_CLIENT    1024*1024*65 // used by client to get data from server
#define SIZE_DATA_FILE_HEADER   1024*8       // padding at top of log files
#define SIZE_DATA_FILE    (SIZE_LOGFILE_HEADER + 1024*1024*32)  // maximum size log files can have for small items

#endif