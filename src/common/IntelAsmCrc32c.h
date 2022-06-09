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
#ifndef _HDFS_LIBHDFS3_COMMON_INTELASMCRC32C_H_
#define _HDFS_LIBHDFS3_COMMON_INTELASMCRC32C_H_

#if defined(__SSE4_2__) && defined(__LP64__)

#include "Checksum.h"

namespace Hdfs {
namespace Internal {

/**
 * Calculate CRC with Intel ASM(https://github.com/htot/crc32c).
 * Which is at least 2x faster than HWCrc32c.
 */
class IntelAsmCrc32c: public Checksum {
public:
    /**
     * Constructor.
     */
    IntelAsmCrc32c() :
        crc(0xFFFFFFFF) {
    }

    uint32_t getValue() {
        return ~crc;
    }

    /**
     * @ref Checksum#reset()
     */
    void reset() {
        crc = 0xFFFFFFFF;
    }

    /**
     * @ref Checksum#update(const void *, int)
     */
    void update(const void * b, int len);

    /**
     * Destory an HWCrc32 instance.
     */
    ~IntelAsmCrc32c() { }

private:
    uint32_t crc;
};


}
}
#endif
#endif
