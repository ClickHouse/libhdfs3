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

#ifndef _HDFS_LIBHDFS3_BYTE_BUFFER_DECODING_STATE_H_
#define _HDFS_LIBHDFS3_BYTE_BUFFER_DECODING_STATE_H_

#include "GF256.h"
#include "RSUtil.h"
#include "Exception.h"
#include "ExceptionInternal.h"

#include <iostream>
#include <algorithm>
#include <memory>
#include <vector>

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

class RawErasureDecoder;

/**
 * A utility class that maintains decoding state during a decode call using
 * ByteBuffer inputs.
 */
class ByteBufferDecodingState {

public:
    ByteBufferDecodingState(RawErasureDecoder* decoder,
                            std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                            std::vector<int> & erasedIndexes,
                            std::vector<std::shared_ptr<ByteBuffer>> & outputs);

    ByteBufferDecodingState(RawErasureDecoder* decoder,
                            int decodeLength,
                            std::vector<int> & erasedIndexes,
                            std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                            std::vector<std::shared_ptr<ByteBuffer>> & outputs);

    void checkInputBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers);
    void checkOutputBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers);

public:
    std::vector<std::shared_ptr<ByteBuffer>> & inputs;
    std::vector<std::shared_ptr<ByteBuffer>> & outputs;
    std::vector<int> & erasedIndexes;
    RawErasureDecoder* decoder;
    int decodeLength;
};

}
}

#endif /* _HDFS_LIBHDFS3_BYTE_BUFFER_DECODING_STATE_H_ */