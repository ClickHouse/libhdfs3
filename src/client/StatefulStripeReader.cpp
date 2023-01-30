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
#include "StatefulStripeReader.h"
#include "RawErasureDecoder.h"
#include "ECChunk.h"
#include "Preconditions.h"
#include "Logger.h"

namespace Hdfs {
namespace Internal {

class CorruptedBlocks;
StatefulStripeReader::StatefulStripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                                           ECPolicy * ecPolicy,
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

StatefulStripeReader::~StatefulStripeReader() {
}

void StatefulStripeReader::prepareDecodeInputs() {
    std::shared_ptr<ByteBuffer> cur;
    {
        std::lock_guard<std::mutex> lk(mtx);
        cur = std::shared_ptr<ByteBuffer>(dfsStripedInputStream->getCurStripeBuf()->duplicate());
    }

    if (decodeInputs.empty()) {
        this->decodeInputs = std::vector<std::shared_ptr<ECChunk>>(dataBlkNum + parityBlkNum);
    }
    int bufLen = static_cast<int>(alignedStripe.getSpanInBlock());
    int bufOff = static_cast<int>(alignedStripe.getOffsetInBlock());
    for (int i = 0; i < dataBlkNum; i++) {
        cur->limit(cur->capacity());
        int pos = bufOff % cellSize + cellSize * i;
        cur->position(pos);
        cur->limit(pos + bufLen);
        decodeInputs[i] = std::shared_ptr<ECChunk>(
            new ECChunk(std::shared_ptr<ByteBuffer>(cur->slice()), 0, bufLen));
        if (alignedStripe.chunks[i] == NULL) {
            alignedStripe.chunks[i] =
                new StripedBlockUtil::StripingChunk(decodeInputs[i]->getBuffer());
        }
    }
}

bool StatefulStripeReader::prepareParityChunk(int index) {
    Preconditions::checkState(index >= dataBlkNum
        && alignedStripe.chunks[index] == nullptr);
    const int parityIndex = index - dataBlkNum;
    std::shared_ptr<ByteBuffer> buf =
        std::shared_ptr<ByteBuffer>(dfsStripedInputStream->getParityBuffer()->duplicate());
    buf->position(cellSize * parityIndex);
    buf->limit(cellSize * parityIndex + static_cast<int>(alignedStripe.range->spanInBlock));
    decodeInputs[index] =
        std::shared_ptr<ECChunk>(
            new ECChunk(std::shared_ptr<ByteBuffer>(buf->slice()), 0,
                        static_cast<int>(alignedStripe.range -> spanInBlock)));
    alignedStripe.chunks[index] =
        new StripedBlockUtil::StripingChunk(decodeInputs[index]->getBuffer());
    return true;
}

void StatefulStripeReader::decode() {
    LOG(DEBUG1, "decoding!!!");
    finalizeDecodeInputs();
    decodeAndFillBuffer(false);
}

/**
 * Default close do nothing.
 */
void StatefulStripeReader::close() {
}

}
}
