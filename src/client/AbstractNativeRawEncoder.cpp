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

#include "AbstractNativeRawEncoder.h"

namespace Hdfs {
namespace Internal {

AbstractNativeRawEncoder::AbstractNativeRawEncoder(ErasureCoderOptions & coderOptions) :
    RawErasureEncoder(coderOptions) {}

void AbstractNativeRawEncoder::doEncode(const shared_ptr<ByteBufferEncodingState> & encodingState) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (!nativeCoder) {
        THROW(HdfsException, "AbstractNativeRawEncoder closed");
    }
    int inputOffsets[encodingState->inputs.size()];
    int outputOffsets[encodingState->outputs.size()];
    int dataLen = encodingState->inputs[0]->remaining();

    shared_ptr<ByteBuffer> buffer;
    int inputSize = static_cast<int>(encodingState->inputs.size());
    for (int i = 0; i < inputSize; ++i) {
        buffer = encodingState->inputs[i];
        inputOffsets[i] = buffer->position();
    }

    int outputSize = static_cast<int>(encodingState->outputs.size());
    for (int i = 0; i < outputSize; ++i) {
        buffer = encodingState->outputs[i];
        outputOffsets[i] = buffer->position();
    }

    performEncodeImpl(encodingState->inputs, inputOffsets, dataLen,
                      encodingState->outputs, outputOffsets);
}

void AbstractNativeRawEncoder::release() {

}

}
}