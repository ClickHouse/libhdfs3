/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Bowen Yang
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
#ifndef _HDFS_LIBHDFS3_COMMON_CRC32_H_
#define _HDFS_LIBHDFS3_COMMON_CRC32_H_

#include "Checksum.h"

#include <boost/crc.hpp>

namespace Hdfs {
namespace Internal {

/**
 * Calculate CRC with boost::crc_32_type.
 */
class Crc32: public Checksum {
public:
    /**
     * Constructor.
     */
    Crc32() {
    }

    uint32_t getValue() {
        return crc.checksum();
    }

    /**
     * @ref Checksum#reset()
     */
    void reset() {
        crc.reset();
    }

    /**
     * @ref Checksum#update(const void *, int)
     */
    void update(const void * b, int len) {
        crc.process_bytes((const char*) b, len);
    }

    /**
     * Destory an Crc32 instance.
     */
    ~Crc32() {
    }

private:
    boost::crc_32_type crc;
};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_CRC32_H_ */
