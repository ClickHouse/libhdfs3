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

#include "StripedInputStreamImpl.h"
#include "PositionStripeReader.h"
#include "RawErasureDecoder.h"
#include "ECChunk.h"
#include "Preconditions.h"
#include "Logger.h"

namespace Hdfs {
namespace Internal {

class CorruptedBlocks;
PositionStripeReader::PositionStripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                                           shared_ptr<ECPolicy> ecPolicy,
                                           std::vector<LocatedBlock> & targetBlocks,
                                           std::vector<StripeReader::BlockReaderInfo *> & readerInfos,
                                           shared_ptr<CorruptedBlocks> corruptedBlocks,
                                           shared_ptr<RawErasureDecoder> decoder,
                                           StripedInputStreamImpl * dfsStripedInputStream,
                                           shared_ptr<SessionConfig> conf) :
                                           StripeReader(alignedStripe, ecPolicy, targetBlocks,
                                                        readerInfos, corruptedBlocks, decoder,
                                                        dfsStripedInputStream, conf) {
}

PositionStripeReader::~PositionStripeReader() {
}

void PositionStripeReader::prepareDecodeInputs() {
    if (codingBuffer == nullptr) {
        decodeInputs = std::vector<shared_ptr<ECChunk>>(dataBlkNum + parityBlkNum);
        initDecodeInputs(alignedStripe);
    }
}

void PositionStripeReader::initDecodeInputs(StripedBlockUtil::AlignedStripe & alignedStripe) {
    int bufLen = (int) alignedStripe.getSpanInBlock();
    int bufCount = dataBlkNum + parityBlkNum;
    codingBuffer = shared_ptr<ByteBuffer>(new ByteBuffer(bufLen * bufCount));
    shared_ptr<ByteBuffer> buffer;
    for (int i = 0; i < dataBlkNum; i++) {
        buffer = shared_ptr<ByteBuffer>(codingBuffer->duplicate());
        decodeInputs[i] = shared_ptr<ECChunk>(new ECChunk(buffer, i * bufLen, bufLen));
    }

    for (int i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] == nullptr) {
            alignedStripe.chunks[i] =
                    shared_ptr<StripedBlockUtil::StripingChunk>(new StripedBlockUtil::StripingChunk(decodeInputs[i]->getBuffer()));
        }
    }
}

bool PositionStripeReader::prepareParityChunk(int index) {
    Preconditions::checkState(index >= dataBlkNum &&
                              alignedStripe.chunks[index] == nullptr);

    int bufLen = (int) alignedStripe.getSpanInBlock();
    decodeInputs[index] = shared_ptr<ECChunk>(new ECChunk(shared_ptr<ByteBuffer>(codingBuffer->duplicate()),
                                                                                 index * bufLen, bufLen));

    alignedStripe.chunks[index] =
            shared_ptr<StripedBlockUtil::StripingChunk>(new StripedBlockUtil::StripingChunk(decodeInputs[index]->getBuffer()));

    return true;
}

void PositionStripeReader::decode() {
    LOG(DEBUG1, "decoding!!!");
    finalizeDecodeInputs();
    decodeAndFillBuffer(true);
}

/**
 * Default close do nothing.
 */
void PositionStripeReader::close() {
}

}
}
