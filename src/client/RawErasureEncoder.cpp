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

#include "RawErasureEncoder.h"
#include "ECChunk.h"
#include "StripeReader.h"

#include <iostream>

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

RawErasureEncoder::RawErasureEncoder() : coderOptions(6, 3) {
}

RawErasureEncoder::RawErasureEncoder(ErasureCoderOptions & coderOptions) :
    coderOptions(coderOptions) {
    if (getNumAllUnits() >= RSUtil::getGF()->getFieldSize()) {
        THROW(InvalidParameter, "Invalid getNumDataUnits() and numParityUnits");
    }

    encodeMatrix = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits());
    RSUtil::genCauchyMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());

    gfTables = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits() * 32);
    RSUtil::initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
        getNumDataUnits() * getNumDataUnits(), gfTables);
}

RawErasureEncoder::~RawErasureEncoder() {
}

int RawErasureEncoder::getNumDataUnits() {
    return coderOptions.getNumDataUnits();
}

int RawErasureEncoder::getNumParityUnits() {
    return coderOptions.getNumParityUnits();
}

int RawErasureEncoder::getNumAllUnits() {
    return coderOptions.getNumAllUnits();
}

/**
 * Tell if direct buffer is preferred or not. It's for callers to
 * decide how to allocate coding chunk buffers, using DirectByteBuffer or
 * bytes array. It will return false by default.
 * @return true if native buffer is preferred for performance consideration,
 * otherwise false.
 */
bool RawErasureEncoder::preferDirectBuffer() {
    return false;
}

/**
 * Allow change into input buffers or not while perform encoding/decoding.
 * @return true if it's allowed to change inputs, false otherwise
 */
bool RawErasureEncoder::allowChangeInputs() {
    return coderOptions.allowChangeInputs();
}

/**
 * Allow to dump verbose info during encoding/decoding.
 * @return true if it's allowed to do verbose dump, false otherwise.
 */
bool RawErasureEncoder::allowVerboseDump() {
    return coderOptions.allowVerboseDump();
}

/**
 * Should be called when release this coder. Good chance to release encoding
 * or decoding buffers
 */
void RawErasureEncoder::release() {
    // Nothing to do here.
}

void RawErasureEncoder::encode(std::vector<shared_ptr<ByteBuffer>> & inputs,
                               std::vector<shared_ptr<ByteBuffer>> & outputs) {
    shared_ptr<ByteBufferEncodingState> bbeState = shared_ptr<ByteBufferEncodingState>(
        new ByteBufferEncodingState(this, inputs, outputs));

    int dataLen = bbeState->encodeLength;
    if (dataLen == 0) {
        return;
    }

    std::vector<int> inputPositions(inputs.size());
    for (int i = 0; i < (int)inputPositions.size(); i++) {
    if (inputs[i]) {
            inputPositions[i] = inputs[i]->position();
        }
    }

    doEncode(bbeState.get());
    for (int i = 0; i < (int)inputs.size(); i++) {
        if (inputs[i]) {
            // dataLen bytes consumed
            inputs[i]->position(inputPositions[i] + dataLen);
        }
    }
}

void RawErasureEncoder::encode(std::vector<std::shared_ptr<ECChunk>> & inputs,
                               std::vector<std::shared_ptr<ECChunk>> & outputs) {
    std::vector<std::shared_ptr<ByteBuffer>> newInputs = CoderUtil::toBuffers(inputs);
    std::vector<std::shared_ptr<ByteBuffer>> newOutputs = CoderUtil::toBuffers(outputs);
    encode(newInputs, newOutputs);
}

void RawErasureEncoder::doEncode(ByteBufferEncodingState* encodingState) {
    CoderUtil::resetOutputBuffers(encodingState->outputs, encodingState->encodeLength);
    RSUtil::encodeData(gfTables, encodingState->inputs, encodingState->outputs);
}

}
}