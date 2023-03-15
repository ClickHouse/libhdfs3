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

#include "NativeRSRawDecoder.h"

namespace Hdfs {
namespace Internal {

NativeRSRawDecoder::NativeRSRawDecoder(ErasureCoderOptions & coderOptions) :
    AbstractNativeRawDecoder(coderOptions) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    initImpl(coderOptions.getNumDataUnits(),
             coderOptions.getNumParityUnits());
}

void NativeRSRawDecoder::performDecodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs,
                       int inputOffsets[], int dataLen,
                       std::vector<int> & erased, std::vector<shared_ptr<ByteBuffer>> & outputs,
                       int outputOffsets[]) {
    decodeImpl(inputs, inputOffsets, dataLen, erased, outputs, outputOffsets);
}

void NativeRSRawDecoder::release() {
    std::unique_lock<std::shared_mutex> lock(mutex);
    destroyImpl();
}

void NativeRSRawDecoder::initImpl(int numDataUnits, int numParityUnits) {
    RSDecoder * rsDecoder = (RSDecoder *)malloc(sizeof(RSDecoder));
    memset(rsDecoder, 0, sizeof(*rsDecoder));
    initDecoder(&rsDecoder->decoder, numDataUnits, numParityUnits, static_cast<bool>(isAllowVerboseDump()));
    nativeCoder = (void *)rsDecoder;
}

void NativeRSRawDecoder::decodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs,
                                    int inputOffsets[], int dataLen,
                                    std::vector<int> & erased, std::vector<shared_ptr<ByteBuffer>> & outputs,
                                    int outputOffsets[]) {
    RSDecoder * rsDecoder = (RSDecoder *)nativeCoder;
    if (!rsDecoder) {
        THROW(HdfsException, "NativeRSRawDecoder closed");
    }
    int numDataUnits = rsDecoder->decoder.coder.numDataUnits;
    int numParityUnits = rsDecoder->decoder.coder.numParityUnits;
    int chunkSize = dataLen;

    int numErased = erased.size();
    int tmpErasedIndexes[numErased];
    for (int i = 0; i < numErased; i++) {
        tmpErasedIndexes[i] = erased[i];
    }

    // getInputs
    int numInputs = inputs.size();
    if (numInputs != numDataUnits + numParityUnits) {
        THROW(HdfsException, "Invalid inputs");
    }
    shared_ptr<ByteBuffer> byteBuffer;
    for (int i = 0; i < numInputs; i++) {
        byteBuffer = inputs[i];
        if (byteBuffer != nullptr) {
            rsDecoder->inputs[i] = (unsigned char *)(byteBuffer->getBuffer());
            rsDecoder->inputs[i] += inputOffsets[i];
        } else {
            rsDecoder->inputs[i] = nullptr;
        }
    }

    // getOutputs
    int numOutputs = outputs.size();
    if (numOutputs != numErased) {
        THROW(HdfsException, "Invalid outputs");
    }
    for (int i = 0; i < numOutputs; i++) {
        byteBuffer = outputs[i];
        rsDecoder->outputs[i] = (unsigned char *)(byteBuffer->getBuffer());
        rsDecoder->outputs[i] += outputOffsets[i];
    }

    // decode
    innerDecode(&(rsDecoder->decoder), rsDecoder->inputs, tmpErasedIndexes,
                numErased, rsDecoder->outputs, chunkSize);
}

void NativeRSRawDecoder::destroyImpl() {
    RSDecoder * rsDecoder = (RSDecoder *)nativeCoder;
    if (rsDecoder) {
        free(rsDecoder);
        nativeCoder = nullptr;
    }
}

}
}