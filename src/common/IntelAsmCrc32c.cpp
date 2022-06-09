/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cassert>
#include <cstdlib>
#include "IntelAsmCrc32c.h"

#if defined(__SSE4_2__) && defined(__LP64__)

extern "C" unsigned int crc_pcl ( unsigned char * buffer, int len, unsigned int crc_init );

namespace Hdfs {
namespace Internal {

void IntelAsmCrc32c::update(const void * b, int len) {
    unsigned char * buffer = const_cast<unsigned char *>(static_cast<const unsigned char *>(b));
    crc = crc_pcl(buffer, len, crc);
}

}
}

#endif
