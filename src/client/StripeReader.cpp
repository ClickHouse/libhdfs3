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
#include "StripedInputStreamImpl.h"
#include "Preconditions.h"
#include "CoderUtil.h"

#include <future>

namespace Hdfs {
namespace Internal {
// ReaderStrategy
ReaderStrategy::ReaderStrategy(ByteBuffer & buf) : readBuf(buf) {
    targetLength = buf.remaining();
}

ReaderStrategy::ReaderStrategy(ByteBuffer & buf, int length) : readBuf(buf), targetLength(length) {
}

ReaderStrategy::~ReaderStrategy() {
}

ByteBuffer & ReaderStrategy::getReadBuf() {
    return readBuf;
}

int ReaderStrategy::getTargetLength() {
    return targetLength;
}

// StripeReader
StripeReader::StripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                           std::vector<LocatedBlock> & targetBlocks,
                           std::vector<BlockReaderInfo *> & readerInfos,
                           shared_ptr<SessionConfig> conf) :
                           alignedStripe(alignedStripe),
                           targetBlocks(targetBlocks),
                           readerInfos(readerInfos),
                           ecPolicy(nullptr),
                           threadPool(ThreadPool::getInstance(
                                   min(static_cast<long>(conf->getStripeReaderThreadPoolSize()),
                                       sysconf(_SC_NPROCESSORS_ONLN) * 2))) {
}

StripeReader::StripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                           shared_ptr<ECPolicy> ecPolicy,
                           std::vector<LocatedBlock> & targetBlocks,
                           std::vector<BlockReaderInfo *> & readerInfos,
                           shared_ptr<CorruptedBlocks> corruptedBlocks,
                           shared_ptr<RawErasureDecoder> decoder,
                           StripedInputStreamImpl * dfsStripedInputStream,
                           shared_ptr<SessionConfig> conf) :
                           alignedStripe(alignedStripe),
                           targetBlocks(targetBlocks),
                           corruptedBlocks(corruptedBlocks),
                           readerInfos(readerInfos),
                           ecPolicy(ecPolicy),
                           decoder(decoder),
                           dfsStripedInputStream(dfsStripedInputStream),
                           threadPool(ThreadPool::getInstance(
                                   min(static_cast<long>(conf->getStripeReaderThreadPoolSize()),
                                       sysconf(_SC_NPROCESSORS_ONLN) * 2))) {
    dataBlkNum = ecPolicy->getNumDataUnits();
    parityBlkNum = ecPolicy->getNumParityUnits();
    cellSize = ecPolicy->getCellSize();
}

StripeReader::~StripeReader() {
}

void StripeReader::updateState4SuccessRead(StripedBlockUtil::StripingChunkReadResult & result) {
    Preconditions::checkArgument(
        result.state == StripedBlockUtil::StripingChunkReadResult::SUCCESSFUL);

    readerInfos[result.index]->setOffset(alignedStripe.getOffsetInBlock() + alignedStripe.getSpanInBlock());
}

void StripeReader::checkMissingBlocks() {
    if (alignedStripe.missingChunksNum > parityBlkNum) {
        clearFutures();
        THROW(HdfsIOException, "Missing blocks, missingChunksNum: %d, parityBlkNum: %d",
              alignedStripe.missingChunksNum, parityBlkNum);
    }
}

/**
 * We need decoding. Thus go through all the data chunks and make sure we
 * submit read requests for all of them.
 */
void StripeReader::readDataForDecoding() {
    prepareDecodeInputs();
    for (int i = 0; i < dataBlkNum; i++) {
        Preconditions::checkNotNull(alignedStripe.chunks[i]);
        if (alignedStripe.chunks[i]->state == StripedBlockUtil::StripingChunk::REQUESTED) {
            if (!readChunk(targetBlocks[i], i)) {
                alignedStripe.missingChunksNum++;
            }
        }
    }
    checkMissingBlocks();
}

void StripeReader::readParityChunks(int num) {
    for (int i = dataBlkNum, j = 0; i < dataBlkNum + parityBlkNum && j < num; i++) {
        if (alignedStripe.chunks[i] == nullptr) {
            if (prepareParityChunk(i) && readChunk(targetBlocks[i], i)) {
                j++;
            } else {
                alignedStripe.missingChunksNum++;
            }
        }
    }
    checkMissingBlocks();
}

std::vector<shared_ptr<ReaderStrategy>> StripeReader::getReadStrategies(StripedBlockUtil::StripingChunk & chunk) {
    std::vector<shared_ptr<ReaderStrategy>> strategies;
    if (chunk.useByteBuffer()) {
        shared_ptr<ReaderStrategy> strategy = shared_ptr<ReaderStrategy>(new ReaderStrategy(*chunk.getByteBuffer()));
        strategies.push_back(strategy);
        return strategies;
    }

    strategies.resize(chunk.getChunkBuffer()->getSlices().size());
    for (int i = 0; i < strategies.size(); i++) {
        ByteBuffer * buffer = chunk.getChunkBuffer()->getSlice(i);
        strategies[i] = shared_ptr<ReaderStrategy>(new ReaderStrategy(*buffer));
    }
    return strategies;
}

int32_t StripeReader::readToBuffer(const LocatedBlock & curBlock,
                                   shared_ptr<BlockReader> blockReader,
                                   shared_ptr<ReaderStrategy> strategy,
                                   const DatanodeInfo & currentNode) {
    int32_t length = 0;
    try {
        while (length < strategy->targetLength) {
            int32_t ret = readFromBlock(curBlock, currentNode, blockReader,
                                        (char *)strategy->readBuf.getBuffer() + strategy->readBuf.position(),
                                        strategy->getReadBuf().remaining());
            // Set position for readBuf.
            strategy->readBuf.position(strategy->readBuf.position() + ret);
            if (ret < 0) {
                THROW(HdfsIOException, "Unexpected EOS from the reader");
            }
            length += ret;
        }
        return length;
    } catch (const ChecksumException & ce) {
        LOG(WARNING, "Found Checksum error for %s from %s", 
            curBlock.toString().c_str(), currentNode.getIpAddr().c_str());
        strategy->getReadBuf().clear();
        throw ce;
    } catch (const HdfsIOException & e) {
        LOG(WARNING, "Exception while reading for %s from %s",
            curBlock.toString().c_str(), currentNode.getIpAddr().c_str());
        strategy->getReadBuf().clear();
        throw e;
    }
}

int32_t StripeReader::readFromBlock(const LocatedBlock & curBlock, 
                                    const DatanodeInfo & currentNode,
                                    shared_ptr<BlockReader> blockReader,
                                    char * buf, int32_t size) {
    try {
        assert(blockReader);
        return blockReader->read(buf, size);
    } catch (const HdfsIOException & e) {
        /*
         * Failed to read from current block reader,
         * add the current datanode to invalid node list and try again.
         */
        std::string buffer;
        LOG(LOG_ERROR,
            "StripeReader: failed to read Block: %s from Datanode: %s, \n%s, "
            "retry read again from another Datanode.",
            curBlock.toString().c_str(), currentNode.getIpAddr().c_str(),
            GetExceptionDetail(e, buffer));
    } catch (const ChecksumException & e) {
        std::string buffer;
        LOG(LOG_ERROR,
            "StripeReader: failed to read Block: %s from Datanode: %s, \n%s, "
            "retry read again from another Datanode.",
            curBlock.toString().c_str(), currentNode.getIpAddr().c_str(),
            GetExceptionDetail(e, buffer));
    }

    LOG(INFO, "StripeReader: add invalid datanode %s to failed datanodes and try another "
              "datanode again for block %s.",
        currentNode.formatAddress().c_str(),  curBlock.toString().c_str());
    blockReader.reset();
    return -1;
}

bool StripeReader::readChunk(LocatedBlock& block, int chunkIndex) {
    StripedBlockUtil::StripingChunk * chunk = alignedStripe.chunks[chunkIndex];
    if (block.getBlockId() == 0) {
        chunk->state = StripedBlockUtil::StripingChunk::MISSING;
        return false;
    }

    if (readerInfos[chunkIndex] == nullptr) {
        if (!dfsStripedInputStream->createBlockReader(
            block, alignedStripe.getOffsetInBlock(), targetBlocks, readerInfos, chunkIndex)) {
            chunk->state = StripedBlockUtil::StripingChunk::MISSING;
            return false;
        }
    } else if(readerInfos[chunkIndex]->shouldSkip) {
        chunk->state = StripedBlockUtil::StripingChunk::MISSING;
        return false;
    }

    chunk->state = StripedBlockUtil::StripingChunk::PENDING;
    shared_ptr<BlockReader> reader = readerInfos[chunkIndex]->reader;
    DatanodeInfo datanode = readerInfos[chunkIndex]->datanode;
    long currentReaderOffset = readerInfos[chunkIndex]->blockReaderOffset;
    int64_t targetReaderOffset = alignedStripe.getOffsetInBlock();
    const shared_ptr<std::vector<shared_ptr<ReaderStrategy>>> strategies =
            shared_ptr<std::vector<shared_ptr<ReaderStrategy>>>(
                    new std::vector<shared_ptr<ReaderStrategy>>(getReadStrategies(*chunk)));

    std::future<StripedBlockUtil::BlockReadStats> future = threadPool.enqueue(
            [block,
             reader,
             datanode,
             currentReaderOffset,
             targetReaderOffset,
             strategies,
             this] {
        // reader can be null if getBlockReaderWithRetry failed or
        // the reader hit exception before
        if (reader == nullptr) {
            THROW(HdfsIOException, "The BlockReader is null. "
                  "The BlockReader creation failed or the reader hit exception.");
        }

        Preconditions::checkState(currentReaderOffset <= targetReaderOffset);
        if (currentReaderOffset < targetReaderOffset) {
            reader->skip(targetReaderOffset - currentReaderOffset);
        }

        int ret = 0;
        for (shared_ptr<ReaderStrategy> strategy: *strategies) {
            int bytesReead = readToBuffer(block, reader, strategy, datanode);
            ret += bytesReead;
        }

        return StripedBlockUtil::BlockReadStats(ret, false, 1);
    });

    futures.insert(std::pair<int, std::future<StripedBlockUtil::BlockReadStats>>{chunkIndex, std::move(future)});
    return true;
}

/**
 * read the whole stripe. do decoding if necessary
 */
void StripeReader::readStripe() {
    for (int i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] != nullptr &&
            alignedStripe.chunks[i]->state != StripedBlockUtil::StripingChunk::ALLZERO) {
            if (!readChunk(targetBlocks[i], i)) {
                alignedStripe.missingChunksNum++;
            }
        }
    }
    // There are missing block locations at this stage. Thus we need to read
    // the full stripe and one more parity block.
    if (alignedStripe.missingChunksNum > 0) {
        checkMissingBlocks();
        readDataForDecoding();
        // read parity chunks
        readParityChunks(alignedStripe.missingChunksNum);
    }

    // Input buffers for potential decode operation, which remains null until
    // first read failure
    while (!futures.empty()) {
        try {
            StripedBlockUtil::StripingChunkReadResult r(0, 0);
            StripedBlockUtil::getNextCompletedStripedRead(futures, r);

            StripedBlockUtil::StripingChunk * returnedChunk = alignedStripe.chunks[r.index];
            Preconditions::checkNotNull(returnedChunk);
            Preconditions::checkState(returnedChunk->state == StripedBlockUtil::StripingChunk::PENDING);

            if (r.state == StripedBlockUtil::StripingChunkReadResult::SUCCESSFUL) {
                returnedChunk->state = StripedBlockUtil::StripingChunk::FETCHED;
                alignedStripe.fetchedChunksNum++;
                updateState4SuccessRead(r);
                if (alignedStripe.fetchedChunksNum == dataBlkNum) {
                    clearFutures();
                    break;
                }
            } else {
                returnedChunk->state = StripedBlockUtil::StripingChunk::MISSING;
                dfsStripedInputStream->closeReader(readerInfos[r.index]);
                readerInfos[r.index] = nullptr;

                int missing = alignedStripe.missingChunksNum;
                alignedStripe.missingChunksNum++;
                checkMissingBlocks();

                readDataForDecoding();
                readParityChunks(alignedStripe.missingChunksNum - missing);
            }
        } catch (...) {
            std::string buffer;
            LOG(LOG_ERROR, "StripeReader: failed to read stripe.\n%s",
                GetExceptionDetail(current_exception(), buffer));
            clearFutures();
            // Don't decode if read interrupted
            rethrow_exception(current_exception());
        }
    }

    if (alignedStripe.missingChunksNum > 0) {
        decode();
    }
}

/**
 * Some fetched StripingChunk might be stored in original application
 * buffer instead of prepared decode input buffers. Some others are beyond
 * the range of the internal blocks and should correspond to all zero bytes.
 * When all pending requests have returned, this method should be called to
 * finalize decode input buffers.
 */
void StripeReader::finalizeDecodeInputs() {
    for (int i = 0; i < static_cast<int>(alignedStripe.chunks.size()); i++) {
        StripedBlockUtil::StripingChunk * chunk = alignedStripe.chunks[i];
        if (chunk != nullptr && chunk->state == StripedBlockUtil::StripingChunk::FETCHED) {
            if (chunk->useChunkBuffer()) {
                chunk->getChunkBuffer()->copyTo(decodeInputs[i]->getBuffer().get());
            } else {
                chunk->getByteBuffer()->flip();
            }
        } else if (chunk != nullptr &&
            chunk->state == StripedBlockUtil::StripingChunk::ALLZERO) {
                decodeInputs[i]->setAllZero(true);
        }
    }
}

/**
 * Decode based on the given input buffers and erasure coding policy.
 */
void StripeReader::decodeAndFillBuffer(bool fillBuffer) {
    // Step 1: prepare indices and output buffers for missing data units
    vector<int> decodeIndices = prepareErasedIndices();

    const int decodeChunkNum = decodeIndices.size();
    std::vector<shared_ptr<ECChunk>> outputs(decodeChunkNum);
    for (int i = 0; i < decodeChunkNum; i++) {
        outputs[i] = decodeInputs[decodeIndices[i]];
        decodeInputs[decodeIndices[i]] = nullptr;
    }

    // Step 2: decode into prepared output buffers
    decoder->decode(decodeInputs, decodeIndices, outputs);

    // Step 3: fill original application buffer with decoded data
    if (fillBuffer) {
        for (int i = 0; i < static_cast<int>(decodeIndices.size()); i++) {
            int missingBlkIdx = decodeIndices[i];
            StripedBlockUtil::StripingChunk *chunk = alignedStripe.chunks[missingBlkIdx];
            if (chunk->state == StripedBlockUtil::StripingChunk::MISSING && chunk->useChunkBuffer()) {
                chunk->getChunkBuffer()->copyFrom((outputs[i]->getBuffer().get()));
            }
        }
    }
}

/**
 * Prepare erased indices.
 */
std::vector<int> StripeReader::prepareErasedIndices() {
    std::vector<int> decodeIndices(parityBlkNum);
    int pos = 0;
    for (int i = 0; i < static_cast<int>(alignedStripe.chunks.size()); i++) {
        if (alignedStripe.chunks[i] != nullptr &&
            alignedStripe.chunks[i]->state == StripedBlockUtil::StripingChunk::MISSING) {
            decodeIndices[pos++] = i;
        }
    }

    std::vector<int> erasedIndices = CoderUtil::copyOf(decodeIndices, pos);
    return erasedIndices;
}

void StripeReader::clearFutures() {
    try {
        for (std::map<int, std::future<StripedBlockUtil::BlockReadStats>>::iterator it =
                futures.begin(); it != futures.end(); it++) {
            it->second.wait_for(30s);
        }
    } catch (...) {
        std::string buffer;
        LOG(WARNING, "StripeReader: Clear futures failed.\n%s",
            GetExceptionDetail(current_exception(), buffer));
    }
    futures.clear();
}

// BlockReaderInfo
StripeReader::BlockReaderInfo::BlockReaderInfo(
    shared_ptr<BlockReader> reader, DatanodeInfo node, long offset) :
    reader(reader), datanode(node), blockReaderOffset(offset) {
}

void StripeReader::BlockReaderInfo::setOffset(int64_t offset) {
    blockReaderOffset = offset;
}

void StripeReader::BlockReaderInfo::skip() {
    shouldSkip = true;
}

}
}
