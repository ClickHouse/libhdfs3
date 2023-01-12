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

#include "ByteBufferDecodingState.h"
#include "RawErasureDecoder.h"
#include "CoderUtil.h"

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

ByteBufferDecodingState::ByteBufferDecodingState(RawErasureDecoder* decoder,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                                                 std::vector<int> & erasedIndexes,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & outputs) :
    inputs(inputs), outputs(outputs), erasedIndexes(erasedIndexes) {
    this->decoder = decoder;
    std::shared_ptr<ByteBuffer> validInput = CoderUtil::findFirstValidInput(inputs);
    this->decodeLength = validInput->remaining();
}

ByteBufferDecodingState::ByteBufferDecodingState(RawErasureDecoder* decoder,
                                                 int decodeLength,
                                                 std::vector<int> & erasedIndexes,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                                                 std::vector<std::shared_ptr<ByteBuffer>> & outputs) :
    inputs(inputs), outputs(outputs), erasedIndexes(erasedIndexes) {
    this->decoder = decoder;
    this->decodeLength = decodeLength;
}

/**
 * Check and ensure the buffers are of the desired length and type, direct
 * buffers or not.
 * @param buffers the buffers to check
 */
void ByteBufferDecodingState::checkInputBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers) {
    int validInputs = 0;

    for (std::shared_ptr<ByteBuffer> buffer : buffers) {
        if (buffer == nullptr) {
            continue;
        }

        if ((int)buffer->remaining() != decodeLength) {
            THROW(HadoopIllegalArgumentException, "Invalid buffer, not of length %d", decodeLength);
        }
        validInputs++;
    }

    if (validInputs < decoder->getNumDataUnits()) {
        THROW(HadoopIllegalArgumentException, "No enough valid inputs are provided, not recoverable");
    }
}

/**
 * Check and ensure the buffers are of the desired length and type, direct
 * buffers or not.
 * @param buffers the buffers to check
 */
void ByteBufferDecodingState::checkOutputBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers) {
    for (std::shared_ptr<ByteBuffer> buffer : buffers) {
        if (buffer == nullptr) {
            THROW(HadoopIllegalArgumentException, "Invalid buffer found, not allowing null");
        }

        if ((int)buffer->remaining() != decodeLength) {
            THROW(HadoopIllegalArgumentException, "Invalid buffer, not of length %d", decodeLength);
        }
    }
}

}
}
