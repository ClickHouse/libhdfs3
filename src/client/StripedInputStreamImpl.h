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

#ifndef _HDFS_LIBHDFS3_CLIENT_STRIPEDINPUTSTREAMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_STRIPEDINPUTSTREAMIMPL_H_

#include "InputStreamImpl.h"
#include "ECPolicy.h"
#include "StripedBlockUtil.h"
#include "ByteBuffer.h"
#include "StripeReader.h"

namespace Hdfs {

namespace Internal {

class StripedInputStreamImpl : public InputStreamImpl {
public:
    explicit StripedInputStreamImpl(shared_ptr<LocatedBlocks> lbs);
    ~StripedInputStreamImpl() override;

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    int32_t read(char * buf, int32_t size) override;

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     */
    void seek(int64_t pos) override;

    /**
     * Close the stream.
     */
    void close() override;

    bool createBlockReader(LocatedBlock & block,
                           long offsetInBlock,
                           std::vector<LocatedBlock> & targetBlocks,
                           std::vector<StripeReader::BlockReaderInfo *> & readerInfos,
                           int chunkIndex);
    void closeReader(StripeReader::BlockReaderInfo * readerInfo);

private:
    int64_t getOffsetInBlockGroup();
    int64_t getOffsetInBlockGroup(int64_t pos);
    int32_t getStripedBufOffset(int64_t offsetInBlockGroup);
    int32_t copyToTarget(char * buf, int32_t size);

    void readOneStripe();
    void seekToBlock(const LocatedBlock & lb);
    void setCurBlock();
    void resetCurStripeBuffer(bool force);
    void closeCurrentBlockReaders();

    bool isLocalNode(const DatanodeInfo * info);
    const DatanodeInfo * choseBestNode(LocatedBlock & lb);

public:
    ByteBuffer * getCurStripeBuf() const { return curStripeBuf; }
    ByteBuffer * getParityBuffer();

private:
    int32_t cellSize;
    int8_t dataBlkNum;
    int8_t parityBlkNum;
    int32_t groupSize;
    shared_ptr<ECPolicy> ecPolicy;
    

private:
    shared_ptr<StripedBlockUtil::StripeRange> curStripeRange;
    ByteBuffer * curStripeBuf;
    ByteBuffer * parityBuf;
    std::vector<StripeReader::BlockReaderInfo *> blockReaders;
    shared_ptr<RawErasureDecoder> decoder;
};


}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_STRIPEDINPUTSTREAMIMPL_H_ */
