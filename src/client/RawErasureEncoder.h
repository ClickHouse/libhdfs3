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

#ifndef _HDFS_LIBHDFS3_CLIENT_RAW_ERASURE_ENCODER_H_
#define _HDFS_LIBHDFS3_CLIENT_RAW_ERASURE_ENCODER_H_

#include "ErasureCoderOptions.h"
#include "ByteBufferEncodingState.h"

#include <vector>

namespace Hdfs {
namespace Internal {

class RawErasureEncoder {
public:
    RawErasureEncoder();
    explicit RawErasureEncoder(ErasureCoderOptions & coderOptions);
    ~RawErasureEncoder() = default;

    virtual void doEncode(const shared_ptr<ByteBufferEncodingState> & encodingState);

    void encode(std::vector<shared_ptr<ByteBuffer>> & inputs,
                std::vector<shared_ptr<ByteBuffer>> & outputs);

    void encode(std::vector<shared_ptr<ECChunk>> & inputs,
                std::vector<shared_ptr<ECChunk>> & outputs);

    int getNumDataUnits() const;
    int getNumParityUnits() const;
    int getNumAllUnits() const;
    bool isAllowVerboseDump() const;

    virtual void release();
public:

    // relevant to schema and won't change during encode calls.
    std::vector<int8_t> encodeMatrix;

    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    std::vector<int8_t> gfTables;

    ErasureCoderOptions coderOptions;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_RAW_ERASURE_ENCODER_H_ */
