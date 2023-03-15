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

#include "AbstractNativeRawDecoder.h"

namespace Hdfs {
namespace Internal {

AbstractNativeRawDecoder::AbstractNativeRawDecoder(ErasureCoderOptions & coderOptions) :
    RawErasureDecoder(coderOptions) {}

void AbstractNativeRawDecoder::doDecode(const shared_ptr<ByteBufferDecodingState> & decodingState) {
    std::shared_lock<std::shared_mutex> lock(mutex);

    if (!nativeCoder) {
        THROW(HdfsException, "AbstractNativeRawDecoder closed");
    }
    int inputOffsets[decodingState->inputs.size()];
    int outputOffsets[decodingState->outputs.size()];

    shared_ptr<ByteBuffer> buffer;
    int inputSize = static_cast<int>(decodingState->inputs.size());
    for (int i = 0; i < inputSize; ++i) {
        buffer = decodingState->inputs[i];
        if (buffer) {
            inputOffsets[i] = buffer->position();
        }
    }

    int outputSize = static_cast<int>(decodingState->outputs.size());
    for (int i = 0; i < outputSize; ++i) {
        buffer = decodingState->outputs[i];
        outputOffsets[i] = buffer->position();
    }

    performDecodeImpl(decodingState->inputs, inputOffsets,
                      decodingState->decodeLength, decodingState->erasedIndexes,
                      decodingState->outputs, outputOffsets);
}

void AbstractNativeRawDecoder::release() {

}

}
}