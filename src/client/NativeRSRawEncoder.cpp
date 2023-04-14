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

#include "NativeRSRawEncoder.h"

namespace Hdfs {
namespace Internal {

NativeRSRawEncoder::NativeRSRawEncoder(ErasureCoderOptions & coderOptions) :
    AbstractNativeRawEncoder(coderOptions) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    initImpl(coderOptions.getNumDataUnits(),
             coderOptions.getNumParityUnits());
}

void NativeRSRawEncoder::performEncodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs, int inputOffsets[], int dataLen,
                                           std::vector<shared_ptr<ByteBuffer>> & outputs, int outputOffsets[]) {
    encodeImpl(inputs, inputOffsets, dataLen, outputs, outputOffsets);
}

void NativeRSRawEncoder::release() {
    std::unique_lock<std::shared_mutex> lock(mutex);
    destroyImpl();
}

void NativeRSRawEncoder::initImpl(int numDataUnits, int numParityUnits) {
    RSEncoder * rsEncoder = (RSEncoder *) malloc(sizeof(RSEncoder));
    memset(rsEncoder, 0, sizeof(*rsEncoder));
    initEncoder(&rsEncoder->encoder, numDataUnits, numParityUnits, static_cast<bool>(isAllowVerboseDump()));
    nativeCoder = (void *)rsEncoder;
}

void NativeRSRawEncoder::encodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs, int inputOffsets[],
                                    int dataLen, std::vector<shared_ptr<ByteBuffer>> & outputs,
                                    int outputOffsets[]) {
    RSEncoder * rsEncoder = (RSEncoder *)nativeCoder;
    if (!rsEncoder) {
        THROW(HdfsException, "NativeRSRawEncoder closed");
    }
    int numDataUnits = rsEncoder->encoder.coder.numDataUnits;
    int numParityUnits = rsEncoder->encoder.coder.numParityUnits;
    int chunkSize = dataLen;

    // getInputs
    int numInputs = inputs.size();
    if (numInputs != numDataUnits) {
        THROW(HdfsException, "Invalid inputs");
    }
    shared_ptr<ByteBuffer> byteBuffer;
    for (int i = 0; i < numInputs; i++) {
        byteBuffer = inputs[i];
        if (byteBuffer != nullptr) {
            rsEncoder->inputs[i] = (unsigned char *)(byteBuffer->getBuffer());
            rsEncoder->inputs[i] += inputOffsets[i];
        } else {
            rsEncoder->inputs[i] = nullptr;
        }
    }

    // getOutputs
    int numOutputs = outputs.size();
    if (numOutputs != numParityUnits) {
        THROW(HdfsException, "Invalid outputs");
    }
    for (int i = 0; i < numOutputs; i++) {
        byteBuffer = outputs[i];
        rsEncoder->outputs[i] = (unsigned char *)(byteBuffer->getBuffer());
        rsEncoder->outputs[i] += outputOffsets[i];
    }

    // encode
    innerEncode(&(rsEncoder->encoder), rsEncoder->inputs, rsEncoder->outputs, chunkSize);
}

void NativeRSRawEncoder::destroyImpl() {
    RSEncoder * rsEncoder = (RSEncoder *)nativeCoder;
    if (rsEncoder) {
        free(rsEncoder);
        nativeCoder = nullptr;
    }
}

}
}