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

#include "StripeReader.h"
#include "StripedBlockUtil.h"
#include "ECChunk.h"
#include "RawErasureDecoder.h"
#include "RSUtil.h"
#include "CoderUtil.h"

#include <future>

namespace Hdfs {
namespace Internal {

RawErasureDecoder::RawErasureDecoder() : coderOptions(6, 3), numErasedDataUnits(0) {
}

RawErasureDecoder::RawErasureDecoder(ErasureCoderOptions & coderOptions) :
    coderOptions(coderOptions), numErasedDataUnits(0) {
    int numAllUnits = getNumAllUnits();
    if (getNumAllUnits() >= RSUtil::getGF()->getFieldSize()) {
        THROW(InvalidParameter, "Invalid getNumDataUnits() and numParityUnits");
    }

    encodeMatrix.resize(numAllUnits * getNumDataUnits());
    RSUtil::genCauchyMatrix(encodeMatrix, numAllUnits, getNumDataUnits());
}

int RawErasureDecoder::getNumDataUnits() const {
    return coderOptions.getNumDataUnits();
}


int RawErasureDecoder::getNumAllUnits() const {
    return coderOptions.getNumAllUnits();
}

void RawErasureDecoder::doDecode(const shared_ptr<ByteBufferDecodingState> & decodingState) {
    CoderUtil::resetOutputBuffers(decodingState->outputs, decodingState->decodeLength);
    prepareDecoding(decodingState->inputs, decodingState->erasedIndexes);

    std::vector<shared_ptr<ByteBuffer>> realInputs(getNumDataUnits());
    for (int i = 0; i < getNumDataUnits(); i++) {
        realInputs[i] = shared_ptr<ByteBuffer>(decodingState->inputs[validIndexes[i]]);
    }
    RSUtil::encodeData(gfTables, realInputs, decodingState->outputs);
}

void RawErasureDecoder::decode(std::vector<shared_ptr<ByteBuffer>> & inputs,
                               std::vector<int> & erasedIndexes,
                               std::vector<shared_ptr<ByteBuffer>> & outputs) {

    shared_ptr<ByteBufferDecodingState> decodingState = shared_ptr<ByteBufferDecodingState>(
        new ByteBufferDecodingState(inputs, erasedIndexes, outputs));
    
    int dataLen = decodingState->decodeLength;
    if (dataLen == 0) {
        return;
    }

    std::vector<int> inputPositions(inputs.size());
    for (int i = 0; i < static_cast<int>(inputPositions.size()); i++) {
        if (inputs[i]) {
            inputPositions[i] = static_cast<int>(inputs[i]->position());
        }
    }

    doDecode(decodingState);
    for (int i = 0; i < static_cast<int>(inputs.size()); i++) {
        if (inputs[i]) {
            // dataLen bytes consumed
            inputs[i]->position(inputPositions[i] + dataLen);
        }
    }
}

void RawErasureDecoder::decode(std::vector<shared_ptr<ECChunk>> & inputs,
                               std::vector<int> & erasedIndexes,
                               std::vector<shared_ptr<ECChunk>> & outputs) {

    std::vector<shared_ptr<ByteBuffer>> newInputs = CoderUtil::toBuffers(inputs);
    std::vector<shared_ptr<ByteBuffer>> newOutputs = CoderUtil::toBuffers(outputs);
    decode(newInputs, erasedIndexes, newOutputs); 
}

void RawErasureDecoder::prepareDecoding(const std::vector<shared_ptr<ByteBuffer>> & inputs, const vector<int> & erasedIndexes) {
    vector<int> tmpValidIndexes = CoderUtil::getValidIndexes(inputs);
    if (cachedErasedIndexes == erasedIndexes && validIndexes == tmpValidIndexes) {
        return; // Optimization. Nothing to do
    }
    cachedErasedIndexes = CoderUtil::copyOf(erasedIndexes, static_cast<int>(erasedIndexes.size()));
    validIndexes = CoderUtil::copyOf(tmpValidIndexes, static_cast<int>(tmpValidIndexes.size()));

    processErasures(erasedIndexes);
}

void RawErasureDecoder::generateDecodeMatrix(const std::vector<int> & erasedIndexes) {
    int i, j, r, p;
    int8_t s;
    std::vector<int8_t> tmpMatrix(getNumAllUnits() * getNumDataUnits());

    // Construct matrix tmpMatrix by removing error rows
    for (i = 0; i < getNumDataUnits(); i++) {
        r = validIndexes[i];
        for (j = 0; j < getNumDataUnits(); j++) {
            tmpMatrix[getNumDataUnits() * i + j] = encodeMatrix[getNumDataUnits() * r + j];
        }
    }

    GF256::gfInvertMatrix(tmpMatrix, invertMatrix, getNumDataUnits());

    for (i = 0; i < numErasedDataUnits; i++) {
        for (j = 0; j < getNumDataUnits(); j++) {
            decodeMatrix[getNumDataUnits() * i + j] = invertMatrix[getNumDataUnits() * erasedIndexes[i] + j];
        }
    }

    for (p = numErasedDataUnits; p < static_cast<int>(erasedIndexes.size()); p++) {
        for (i = 0; i < getNumDataUnits(); i++) {
            s = 0;
            for (j = 0; j < getNumDataUnits(); j++) {
                s ^= GF256::gfMul(invertMatrix[j * getNumDataUnits() + i],
                    encodeMatrix[getNumDataUnits() * erasedIndexes[p] + j]);
            }
            decodeMatrix[getNumDataUnits() * p + i] = s;
        }
    }
}

void RawErasureDecoder::processErasures(const std::vector<int> & erasedIndexes) {
    decodeMatrix = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits());
    invertMatrix = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits());
    gfTables = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits() * 32);

    erasureFlags = std::deque<bool>(getNumAllUnits());
    numErasedDataUnits = 0;

    for (int index : erasedIndexes) {
        erasureFlags[index] = true;
        if (index < getNumDataUnits()) {
            numErasedDataUnits++;
        }
    }

    generateDecodeMatrix(erasedIndexes);

    RSUtil::initTables(getNumDataUnits(), static_cast<int>(erasedIndexes.size()),
        decodeMatrix, 0, gfTables);
}

}
}
