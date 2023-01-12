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

#include <iostream>
#include <future>

using namespace Hdfs;
using namespace Hdfs::Internal;
namespace Hdfs {
namespace Internal {

RawErasureDecoder::RawErasureDecoder() : coderOptions(6, 3) {
}

RawErasureDecoder::RawErasureDecoder(ErasureCoderOptions & coderOptions) :
    coderOptions(coderOptions) {
    int numAllUnits = getNumAllUnits();
    if (getNumAllUnits() >= RSUtil::getGF()->getFieldSize()) {
        THROW(InvalidParameter, "Invalid getNumDataUnits() and numParityUnits");
    }

    encodeMatrix.resize(numAllUnits * getNumDataUnits());
    RSUtil::genCauchyMatrix(encodeMatrix, numAllUnits, getNumDataUnits());
}

RawErasureDecoder::~RawErasureDecoder() {
}

int RawErasureDecoder::getNumParityUnits() {
    return coderOptions.getNumParityUnits();
}

int RawErasureDecoder::getNumDataUnits() {
    return coderOptions.getNumDataUnits();
}


int RawErasureDecoder::getNumAllUnits() {
    return coderOptions.getNumAllUnits();
}

/**
 * Tell if direct buffer is preferred or not. It's for callers to
 * decide how to allocate coding chunk buffers, using DirectByteBuffer or
 * bytes array. It will return false by default.
 * @return true if native buffer is preferred for performance consideration,
 * otherwise false.
 */
bool RawErasureDecoder::preferDirectBuffer() {
    return false;
}

/**
 * Allow change into input buffers or not while perform encoding/decoding.
 * @return true if it's allowed to change inputs, false otherwise
 */
bool RawErasureDecoder::allowChangeInputs() {
    return coderOptions.allowChangeInputs();
}

/**
 * Allow to dump verbose info during encoding/decoding.
 * @return true if it's allowed to do verbose dump, false otherwise.
 */
bool RawErasureDecoder::allowVerboseDump() {
    return coderOptions.allowVerboseDump();
}

/**
 * Should be called when release this coder. Good chance to release encoding
 * or decoding buffers
 */
void RawErasureDecoder::release() {
    // Nothing to do here.
}

void RawErasureDecoder::doDecode(ByteBufferDecodingState* decodingState) {
    CoderUtil::resetOutputBuffers(decodingState->outputs, decodingState->decodeLength);
    prepareDecoding(decodingState->inputs, decodingState->erasedIndexes);

    std::vector<std::shared_ptr<ByteBuffer>> realInputs(getNumDataUnits());
    for (int i = 0; i < getNumDataUnits(); i++) {
        realInputs[i] = std::shared_ptr<ByteBuffer>(decodingState->inputs[validIndexes[i]]);
    }
    RSUtil::encodeData(gfTables, realInputs, decodingState->outputs);
}

void RawErasureDecoder::decode(std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                               std::vector<int> & erasedIndexes,
                               std::vector<std::shared_ptr<ByteBuffer>> & outputs) {

    shared_ptr<ByteBufferDecodingState> decodingState = shared_ptr<ByteBufferDecodingState>(
        new ByteBufferDecodingState(this, inputs, erasedIndexes, outputs));
    
    int dataLen = decodingState->decodeLength;
    if (dataLen == 0) {
        return;
    }

    std::vector<int> inputPositions(inputs.size());
    for (int i = 0; i < (int)inputPositions.size(); i++) {
        if (inputs[i]) {
            inputPositions[i] = inputs[i]->position();
        }
    }

    doDecode(decodingState.get());
    for (int i = 0; i < (int)inputs.size(); i++) {
        if (inputs[i]) {
            // dataLen bytes consumed
            inputs[i]->position(inputPositions[i] + dataLen);
        }
    }
}

void RawErasureDecoder::decode(std::vector<std::shared_ptr<ECChunk>> & inputs,
                               std::vector<int> & erasedIndexes,
                               std::vector<std::shared_ptr<ECChunk>> & outputs) {

    std::vector<std::shared_ptr<ByteBuffer>> newInputs = CoderUtil::toBuffers(inputs);
    std::vector<std::shared_ptr<ByteBuffer>> newOutputs = CoderUtil::toBuffers(outputs);
    decode(newInputs, erasedIndexes, newOutputs); 
}

void RawErasureDecoder::prepareDecoding(std::vector<std::shared_ptr<ByteBuffer>> & inputs, vector<int> & erasedIndexes) {
    vector<int> tmpValidIndexes = CoderUtil::getValidIndexes(inputs);
    if (this->cachedErasedIndexes == erasedIndexes &&
        this->validIndexes == tmpValidIndexes) {
        return; // Optimization. Nothing to do
    }
    this->cachedErasedIndexes = CoderUtil::copyOf(erasedIndexes, erasedIndexes.size());
    this->validIndexes = CoderUtil::copyOf(tmpValidIndexes, tmpValidIndexes.size());

    processErasures(erasedIndexes);
}

void RawErasureDecoder::generateDecodeMatrix(std::vector<int> & erasedIndexes) {
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

    for (p = numErasedDataUnits; p < (int)erasedIndexes.size(); p++) {
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

void RawErasureDecoder::processErasures(std::vector<int> & erasedIndexes) {
    this->decodeMatrix = std::vector<int8_t>(this->getNumAllUnits() * getNumDataUnits());
    this->invertMatrix = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits());
    this->gfTables = std::vector<int8_t>(getNumAllUnits() * getNumDataUnits() * 32);

    this->erasureFlags = std::deque<bool>(getNumAllUnits());
    this->numErasedDataUnits = 0;

    for (int i = 0; i < (int)erasedIndexes.size(); i++) {
        int index = erasedIndexes[i];
        erasureFlags[index] = true;
        if (index < getNumDataUnits()) {
            numErasedDataUnits++;
        }
    }

    generateDecodeMatrix(erasedIndexes);

    RSUtil::initTables(getNumDataUnits(), erasedIndexes.size(),
        decodeMatrix, 0, gfTables);
}

}
}