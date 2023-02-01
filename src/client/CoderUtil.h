/********************************************************************
 * 2023 -
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

#ifndef _HDFS_LIBHDFS3_CODER_UTIL_H_
#define _HDFS_LIBHDFS3_CODER_UTIL_H_

#include "ECChunk.h"
#include "ByteBuffer.h"
#include "Memory.h"

#include <ostream>
#include <string>
#include <algorithm>
#include <vector>
#include <memory>

namespace Hdfs {
namespace Internal {

class CoderUtil {
public:
    static void resetBuffer(const shared_ptr<ByteBuffer> & buffer, int len);

    static void resetOutputBuffers(const std::vector<shared_ptr<ByteBuffer>> & buffers, int dataLen);

    static std::vector<shared_ptr<ByteBuffer>> toBuffers(const std::vector<shared_ptr<ECChunk>> & chunks);

    static std::vector<int> getValidIndexes(const std::vector<shared_ptr<ByteBuffer>> & inputs);

    static std::vector<int> copyOf(const std::vector<int> & original, int newLength);

    static shared_ptr<ByteBuffer> findFirstValidInput(const std::vector<shared_ptr<ByteBuffer>> & inputs);
};

}
}

#endif /* _HDFS_LIBHDFS3_CODER_UTIL_H_ */
