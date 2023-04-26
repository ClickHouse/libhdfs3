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

#include <iostream>

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

int RawErasureEncoder::getNumDataUnits() const {
    return coderOptions.getNumDataUnits();
}

int RawErasureEncoder::getNumParityUnits() const {
    return coderOptions.getNumParityUnits();
}

int RawErasureEncoder::getNumAllUnits() const {
    return coderOptions.getNumAllUnits();
}

bool RawErasureEncoder::isAllowVerboseDump() const {
    return coderOptions.isAllowVerboseDump();
}

void RawErasureEncoder::encode(std::vector<shared_ptr<ByteBuffer>> & inputs,
                               std::vector<shared_ptr<ByteBuffer>> & outputs) {
    shared_ptr<ByteBufferEncodingState> bbeState = shared_ptr<ByteBufferEncodingState>(
        new ByteBufferEncodingState(inputs, outputs));

    int dataLen = bbeState->encodeLength;
    if (dataLen == 0) {
        return;
    }

    std::vector<int> inputPositions(inputs.size());
    for (int i = 0; i < static_cast<int>(inputPositions.size()); i++) {
    if (inputs[i]) {
            inputPositions[i] = static_cast<int>(inputs[i]->position());
        }
    }

    doEncode(bbeState);
    for (int i = 0; i < static_cast<int>(inputs.size()); i++) {
        if (inputs[i]) {
            // dataLen bytes consumed
            inputs[i]->position(inputPositions[i] + dataLen);
        }
    }
}

void RawErasureEncoder::encode(std::vector<shared_ptr<ECChunk>> & inputs,
                               std::vector<shared_ptr<ECChunk>> & outputs) {
    std::vector<shared_ptr<ByteBuffer>> newInputs = CoderUtil::toBuffers(inputs);
    std::vector<shared_ptr<ByteBuffer>> newOutputs = CoderUtil::toBuffers(outputs);
    encode(newInputs, newOutputs);
}

void RawErasureEncoder::doEncode(const shared_ptr<ByteBufferEncodingState> & encodingState) {
    CoderUtil::resetOutputBuffers(encodingState->outputs, encodingState->encodeLength);
    RSUtil::encodeData(gfTables, encodingState->inputs, encodingState->outputs);
}

void RawErasureEncoder::release() {

}

}
}
