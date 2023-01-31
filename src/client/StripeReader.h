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
#ifndef _HDFS_LIBHDFS3_CLIENT_STRIPEDREADER_H_
#define _HDFS_LIBHDFS3_CLIENT_STRIPEDREADER_H_

#include "BlockReader.h"
#include "Checksum.h"
#include "DataTransferProtocol.h"
#include "Memory.h"
#include "network/BufferedSocketReader.h"
#include "network/TcpSocket.h"
#include "PacketHeader.h"
#include "PeerCache.h"
#include "server/DatanodeInfo.h"
#include "server/LocatedBlocks.h"
#include "SessionConfig.h"
#include "StripedBlockUtil.h"
#include "ThreadPool.h"
#include "ECChunk.h"
#include "RawErasureDecoder.h"

#include <future>
#include <algorithm>


namespace Hdfs {
namespace Internal {
    
class StripedInputStreamImpl;
class ReaderStrategy {
public:
    ReaderStrategy(ByteBuffer & buf);
    ReaderStrategy(ByteBuffer & buf, int length);
    ~ReaderStrategy();

    ByteBuffer & readBuf;
    int targetLength;

public:
    ByteBuffer & getReadBuf();
    int getTargetLength();
};

class StripeReader {
public:
    class CorruptedBlocks;
    class BlockReaderInfo;
    StripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                 std::vector<LocatedBlock> & targetBlocks,
                 std::vector<BlockReaderInfo *> & readerInfos,
                 shared_ptr<SessionConfig> c);
    StripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                 shared_ptr<ECPolicy> ecPolicy,
                 std::vector<LocatedBlock> & targetBlocks,
                 std::vector<BlockReaderInfo *> & readerInfos,
                 shared_ptr<CorruptedBlocks> corruptedBlocks,
                 shared_ptr<RawErasureDecoder> decoder,
                 StripedInputStreamImpl * dfsStripedInputStream,
                 shared_ptr<SessionConfig> c);

    virtual ~StripeReader();

    /**
     * read the whole stripe. do decoding if necessary
     */
    void readStripe();

    class ReaderRetryPolicy {
    public:
        ReaderRetryPolicy() {}
        ~ReaderRetryPolicy() {}

    };

    class BlockReaderInfo {
    public:
        BlockReaderInfo();
        BlockReaderInfo(shared_ptr<BlockReader> reader, DatanodeInfo node, long offset);
        virtual ~BlockReaderInfo() {}

        shared_ptr<BlockReader> reader;
        DatanodeInfo datanode;

        long blockReaderOffset;
        bool shouldSkip = false;

        void setOffset(int64_t offset);
        void skip();
    };

private:
    /**
     * Prepare all the data chunks.
     */
    virtual void prepareDecodeInputs() = 0;

    /**
     * Prepare the parity chunk and block reader if necessary.
     */
    virtual bool prepareParityChunk(int index) = 0;

    /**
     * Decode to get the missing data.
     */
    virtual void decode() = 0;

    /**
     * Default close do nothing.
     */
    virtual void close() = 0;

    void updateState4SuccessRead(StripedBlockUtil::StripingChunkReadResult & result);

    void checkMissingBlocks();

    /**
     * We need decoding. Thus go through all the data chunks and make sure we
     * submit read requests for all of them.
     */
    void readDataForDecoding();

    void readParityChunks(int num);

    std::vector<shared_ptr<ReaderStrategy>> getReadStrategies(StripedBlockUtil::StripingChunk & chunk);

    int32_t readToBuffer(const LocatedBlock & curBlock,
                         shared_ptr<BlockReader> blockReader,
                         shared_ptr<ReaderStrategy> strategy,
                         const DatanodeInfo & currentNode);

    int32_t readFromBlock(const LocatedBlock & curBlock,
                          const DatanodeInfo & currentNode,
                          shared_ptr<BlockReader> blockReader,
                          char * buf, int32_t size);

    bool readChunk(LocatedBlock& block, int chunkIndex);

    /**
     * Prepare erased indices.
     */
    std::vector<int> prepareErasedIndices();

    void clearFutures();
    
public:
    /**
     * Some fetched StripingChunk might be stored in original application
     * buffer instead of prepared decode input buffers. Some others are beyond
     * the range of the internal blocks and should correspond to all zero bytes.
     * When all pending requests have returned, this method should be called to
     * finalize decode input buffers.
     */
    void finalizeDecodeInputs();

    /**
     * Decode based on the given input buffers and erasure coding policy.
     */
    void decodeAndFillBuffer(bool fillBuffer);

protected:
    StripedBlockUtil::AlignedStripe & alignedStripe;
    std::vector<LocatedBlock> & targetBlocks;
    shared_ptr<CorruptedBlocks> corruptedBlocks;
    std::vector<BlockReaderInfo *> & readerInfos;
    shared_ptr<ECPolicy> ecPolicy;
    shared_ptr<RawErasureDecoder> decoder;
    StripedInputStreamImpl * dfsStripedInputStream;
    std::vector<shared_ptr<ECChunk>> decodeInputs;
    ThreadPool & threadPool;
    int8_t dataBlkNum;
    int8_t parityBlkNum;
    int cellSize;

private:
    std::map<int, std::future<StripedBlockUtil::BlockReadStats>> futures;
};


}
}
#endif /* _HDFS_LIBHDFS3_CLIENT_STRIPEDREADER_H_ */
