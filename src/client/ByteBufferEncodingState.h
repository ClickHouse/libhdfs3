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

#ifndef _HDFS_LIBHDFS3_BYTE_BUFFER_ENCODING_STATE_H_
#define _HDFS_LIBHDFS3_BYTE_BUFFER_ENCODING_STATE_H_

#include "RSUtil.h"
#include "CoderUtil.h"
#include "Exception.h"
#include "ExceptionInternal.h"

#include <vector>

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

class RawErasureEncoder;

/**
 * A utility class that maintains encoding state during an encode call using
 * ByteBuffer inputs.
 */
class ByteBufferEncodingState {
public:
    ByteBufferEncodingState();
    ByteBufferEncodingState(RawErasureEncoder* encoder,
                            std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                            std::vector<std::shared_ptr<ByteBuffer>> & outputs);

    ByteBufferEncodingState(RawErasureEncoder* encoder,
                            int encodeLength,
                            std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                            std::vector<std::shared_ptr<ByteBuffer>> & outputs);

    void checkBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers);

public:
    std::vector<std::shared_ptr<ByteBuffer>> & inputs;
    std::vector<std::shared_ptr<ByteBuffer>> & outputs;
    RawErasureEncoder* encoder;
    int encodeLength;
};

}
}

#endif /* _HDFS_LIBHDFS3_BYTE_BUFFER_ENCODING_STATE_H_ */