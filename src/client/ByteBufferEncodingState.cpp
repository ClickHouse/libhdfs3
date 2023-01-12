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

#include "ByteBufferEncodingState.h"
#include "RawErasureEncoder.h"

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

ByteBufferEncodingState::ByteBufferEncodingState(RawErasureEncoder* encoder,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & outputs) :
                                                 inputs(inputs), outputs(outputs) {
    this->encoder = encoder;
    std::shared_ptr<ByteBuffer> validInput = CoderUtil::findFirstValidInput(inputs);
    this->encodeLength = validInput->remaining();
}

ByteBufferEncodingState::ByteBufferEncodingState(RawErasureEncoder* encoder,
                                                 int encodeLength,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & outputs) :
                                                 inputs(inputs), outputs(outputs) {
    this->encoder = encoder;
    this->encodeLength = encodeLength;
}

/**
 * Check and ensure the buffers are of the desired length and type, direct
 * buffers or not.
 * @param buffers the buffers to check
 */
void ByteBufferEncodingState::checkBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers) {
    for (std::shared_ptr<ByteBuffer> buffer : buffers) {
        if (buffer == nullptr) {
            THROW(HadoopIllegalArgumentException, "Invalid buffer found, not allowing null");
        }

        if ((int)buffer->remaining() != encodeLength) {
            THROW(HadoopIllegalArgumentException, "Invalid buffer, not of length %d", encodeLength);
        }
    }
}

}
}