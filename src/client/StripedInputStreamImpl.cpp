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
#include "Logger.h"
#include "FileSystemInter.h"
#include "RemoteBlockReader.h"
#include "StatefulStripeReader.h"
#include "ReadShortCircuitInfo.h"
#include "LocalBlockReader.h"
#include "Faultjector.h"

#include <inttypes.h>
#include <algorithm>


namespace Hdfs {

namespace Internal {

StripedInputStreamImpl::StripedInputStreamImpl(shared_ptr<LocatedBlocks> lbs) : 
    InputStreamImpl(lbs), ecPolicy(lbs->getEcPolicy()), curStripeBuf(nullptr),
    parityBuf(nullptr) {
    cellSize = ecPolicy->getCellSize();
    dataBlkNum = ecPolicy->getNumDataUnits();
    parityBlkNum = ecPolicy->getNumParityUnits();
    curStripeRange = shared_ptr<StripedBlockUtil::StripeRange>(
        new StripedBlockUtil::StripeRange(0, 0));
    groupSize = dataBlkNum + parityBlkNum;
    blockReaders.resize(groupSize);
    InputStreamImpl::endOfCurBlock = -1;
    ErasureCoderOptions ecOptions(dataBlkNum, parityBlkNum);
    decoder = shared_ptr<RawErasureDecoder>(new RawErasureDecoder(ecOptions));
}

StripedInputStreamImpl::~StripedInputStreamImpl() {
    for (int i = 0; i < (int)blockReaders.size(); ++i) {
        if (blockReaders[i] != nullptr) {
            delete blockReaders[i];
            blockReaders[i] = nullptr;
        }
    }

}

int64_t StripedInputStreamImpl::getOffsetInBlockGroup() {
    return cursor - curBlock->getOffset();
}

int64_t StripedInputStreamImpl::getOffsetInBlockGroup(int64_t pos) {
    return pos - curBlock->getOffset();
}

int32_t StripedInputStreamImpl::getStripedBufOffset(int64_t offsetInBlockGroup) {
    int64_t stripeLen = cellSize * dataBlkNum;
    // compute the position in the curStripeBuf based on "pos"
    return (int32_t) (offsetInBlockGroup % stripeLen);
  }

int32_t StripedInputStreamImpl::copyToTarget(char * buf, int32_t length) {
    int64_t offsetInBlk = getOffsetInBlockGroup();
    int bufOffset = getStripedBufOffset(offsetInBlk);
    curStripeBuf->position(bufOffset);
    int32_t result = std::min(length, (int32_t)curStripeBuf->remaining());
    curStripeBuf->copyTo(buf, result);
    return result;
}

void StripedInputStreamImpl::setCurBlock() {
    const LocatedBlock * lb = lbs->findBlock(cursor);
    if (!lb) {
        updateBlockInfos();
        lb = lbs->findBlock(cursor);
        if (!lb) {
            THROW(HdfsIOException, "StripedInputStreamImpl: cannot find block "
                "information at position: %" PRId64 " for file: %s",
                cursor, path.c_str());
        }
    }
    seekToBlock(*lb);
}

void StripedInputStreamImpl::resetCurStripeBuffer(bool force) {
    if (force && curStripeBuf == nullptr) {    
        curStripeBuf = new ByteBuffer(cellSize * dataBlkNum);
    }

    if (curStripeBuf != nullptr) {
        curStripeBuf->clear();
    }

    curStripeRange->setLength(0);
    curStripeRange->setOffset(0);
}

ByteBuffer * StripedInputStreamImpl::getParityBuffer() {
    if (parityBuf == nullptr) {
        parityBuf = new ByteBuffer(cellSize * parityBlkNum);
    }
    if (parityBuf != nullptr) {
        parityBuf->clear();
    }
    return parityBuf;
}

void StripedInputStreamImpl::closeReader(StripeReader::BlockReaderInfo * readerInfo) {
    if (readerInfo != nullptr &&
        readerInfo->reader != nullptr) {
        readerInfo->skip(); 
        delete readerInfo;
    }
}

void StripedInputStreamImpl::closeCurrentBlockReaders() {
    resetCurStripeBuffer(false);
    if (blockReaders.size() == 0) {
        return;
    }
    for (int i = 0; i < groupSize; i++) {
        closeReader(blockReaders[i]);
        blockReaders[i] = nullptr;
    }
    endOfCurBlock = -1;
}

bool StripedInputStreamImpl::isLocalNode(const DatanodeInfo * info) {
    static const unordered_set<std::string> LocalAddrSet = BuildLocalAddrSet();
    bool retval = LocalAddrSet.find(info->getIpAddr()) != LocalAddrSet.end();
    return retval;
}

const DatanodeInfo * StripedInputStreamImpl::choseBestNode(LocatedBlock & lb) {
    const std::vector<DatanodeInfo> & nodes = lb.getLocations();

    for (size_t i = 0; i < nodes.size(); ++i) {
        if (std::binary_search(failedNodes.begin(), failedNodes.end(), nodes[i])) {
            continue;
        }

        return &nodes[i];
    }

    return nullptr;
}

bool StripedInputStreamImpl::createBlockReader(LocatedBlock & block,
                                               long offsetInBlock,
                                               std::vector<LocatedBlock> & targetBlocks,
                                               std::vector<StripeReader::BlockReaderInfo *> & readerInfos,
                                               int chunkIndex) {
    bool lastReadFromLocal = false;
    exception_ptr lastException;
    shared_ptr<BlockReader> reader = nullptr;
    while (true) {
        DatanodeInfo * node = const_cast<DatanodeInfo *>(choseBestNode(block));
        if (node == nullptr) {
            return false;
        }

        try {
            if (!lastReadFromLocal && localRead &&
                !readFromUnderConstructedBlock && isLocalNode(node)) {
                lastReadFromLocal = true;

                shared_ptr<ReadShortCircuitInfo> info;
                ReadShortCircuitInfoBuilder builder(*node, auth, *conf);
                EncryptionKey ekey = filesystem->getEncryptionKeys();

                try {
                    info = builder.fetchOrCreate(block, block.getToken(), ekey);

                    if (!info) {
                        continue;
                    }

                    assert(info->isValid());
                    reader = shared_ptr<BlockReader>(new LocalBlockReader(
                        info, block, offsetInBlock, verify, *conf, localReaderBuffer));
                } catch (...) {
                    if (info) {
                        info->setValid(false);
                    }

                    throw;
                }
            } else {
                lastReadFromLocal = false;
                const char * clientName = filesystem->getClientName();
                // test bad node
                if (FaultInjector::get().testBadReader()) {
                    THROW(HdfsIOException, "bad RemoteBlockReader");
                }
                reader = shared_ptr<BlockReader>(new RemoteBlockReader(filesystem,
                    block, *node, *peerCache, offsetInBlock, block.getNumBytes() - offsetInBlock,
                    block.getToken(), clientName, verify, *conf));
            }

        } catch (const HdfsIOException & e) {
            lastException = current_exception();
            std::string buffer;

            if (lastReadFromLocal) {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\n"
                    "retry the same node but disable read shortcircuit feature",
                    block.toString().c_str(), path.c_str(),
                    node->formatAddress().c_str(), GetExceptionDetail(e, buffer));
            } else {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\nretry another node",
                    block.toString().c_str(), path.c_str(),
                    node->formatAddress().c_str(), GetExceptionDetail(e, buffer));
                failedNodes.push_back(*node);
                std::sort(failedNodes.begin(), failedNodes.end());
            }

            continue;
        }

        if (reader != nullptr) {
            readerInfos[chunkIndex] =
                new StripeReader::BlockReaderInfo(reader, *node, offsetInBlock);
            return true;
        }

        return false;
    }
}

void StripedInputStreamImpl::readOneStripe() {
    resetCurStripeBuffer(true);

    // 1. compute stripe range based on pos
    int64_t offsetInBlockGroup = getOffsetInBlockGroup();
    int64_t stripeLen = cellSize * dataBlkNum;
    int32_t stripeIndex = (int32_t) (offsetInBlockGroup / stripeLen);
    int32_t stripeBufOffset = (int32_t) (offsetInBlockGroup % stripeLen);
    int32_t stripeLimit = (int32_t) std::min(stripeLen, 
        curBlock->getNumBytes() - (stripeIndex * stripeLen));
    LOG(DEBUG1, "readOneStripe offsetInBlockGroup=%ld, stripeLen=%ld, "
        "stripeIndex=%d, stripeBufOffset=%d, stripeLimit=%d\n", 
        offsetInBlockGroup, stripeLen, stripeIndex, stripeBufOffset, stripeLimit);

    std::vector<LocatedBlock> blks;
    StripedBlockUtil::parseStripedBlockGroup(*curBlock, cellSize, dataBlkNum, parityBlkNum, blks);
    for (int32_t i = 0; i < (int32_t)curBlock->getIndices().size(); ++i) {
        int32_t idx = curBlock->getIndices()[i];
        LOG(DEBUG1, "block[%d] id=%ld, size=%ld, locs=%s, poolid=%s\n", idx, blks[idx].getBlockId(), 
            blks[idx].getNumBytes(), blks[idx].getLocations()[0].getIpAddr().c_str(), blks[idx].getPoolId().c_str());
    }

    // 2. aligne stripe
    int64_t stripeRangeLen = stripeLimit - stripeBufOffset;
    std::vector<StripedBlockUtil::AlignedStripe*> stripes;
    StripedBlockUtil::divideOneStripe(
        ecPolicy, cellSize, *curBlock, offsetInBlockGroup, 
        offsetInBlockGroup + stripeRangeLen - 1, curStripeBuf, stripes);

    // 3. read stripe
    for (int i = 0; i < (int)stripes.size(); ++i) {
        // Parse group to get chosen DN location
        shared_ptr<StripeReader> sreader =
                shared_ptr<StripeReader>(new StatefulStripeReader(*stripes[i], ecPolicy, blks,
                    blockReaders, nullptr, decoder, this, conf));
        sreader->readStripe();
    }

    // release stripes
    for (int i = 0; i < (int)stripes.size(); ++i) {
        if (stripes[i] != nullptr) {
            delete stripes[i];
            stripes[i] = nullptr;
        }
    }

    // 4. adjust stripe buffer
    curStripeBuf->position(stripeBufOffset);
    curStripeBuf->limit(stripeLimit);
    curStripeRange->setOffset(offsetInBlockGroup);
    curStripeRange->setLength(stripeRangeLen);
}

int32_t StripedInputStreamImpl::read(char * buf, int32_t size) {
    checkStatus();
    try {
        if (cursor < getFileLength()) {
            if (cursor > endOfCurBlock) {
                closeCurrentBlockReaders();
                setCurBlock();
            }

            int32_t realLen = std::min(size, (int32_t)(endOfCurBlock - cursor + 1));
            if (lbs->isLastBlockComplete()) {
                realLen = (int32_t)std::min((int64_t)realLen, (int64_t)(lbs->getFileLength() - cursor));
            }

            int result = 0;
            while (result < realLen) {
                if (!curStripeRange->include(getOffsetInBlockGroup())) {
                    readOneStripe();
                }
                int32_t ret = copyToTarget(buf + result, realLen - result);
                result += ret;
                cursor += ret;
            }
            return result;
        }
        return 0;
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void StripedInputStreamImpl::seekToBlock(const LocatedBlock & lb) {
    if (cursor >= lbs->getFileLength()) {
        assert(!lbs->isLastBlockComplete());
        readFromUnderConstructedBlock = true;
    } else {
        readFromUnderConstructedBlock = false;
    }
    assert(cursor >= lb.getOffset() && cursor < lb.getOffset() + lb.getNumBytes());
    curBlock = shared_ptr < LocatedBlock > (new LocatedBlock(lb));
    int64_t blockSize = curBlock->getNumBytes();
    assert(blockSize > 0);
    endOfCurBlock = curBlock->getOffset() + blockSize - 1;
    failedNodes.clear();
    blockReader.reset();
}

void StripedInputStreamImpl::seek(int64_t targetPos) {
    checkStatus();

    if (targetPos > getFileLength()) {
        THROW(HdfsEndOfStream,
            "StripedInputStreamImpl: seek over EOF, current position: %" PRId64 ", seek target: %" PRId64 ", in file: %s",
            cursor, targetPos, path.c_str());
    }

    if (targetPos < 0) {
        THROW(InvalidParameter, "StripedInputStreamImpl: Cannot seek to negative offset");
    }

    if (targetPos <= endOfCurBlock) {
        int64_t targetOffsetInBlk = getOffsetInBlockGroup(targetPos);
        if (curStripeRange->include(targetOffsetInBlk)) {
            int32_t bufOffset = getStripedBufOffset(targetOffsetInBlk);
            curStripeBuf->position(bufOffset);
            cursor = targetPos;
            return;
        }
    }

    cursor = targetPos;
    endOfCurBlock = -1;
}

void StripedInputStreamImpl::close() {
    InputStreamImpl::close();
    // release buffer for dataBlocks and parityBlocks
    if (curStripeBuf != nullptr) {
        delete curStripeBuf;
        curStripeBuf = nullptr;
    }
    if (parityBuf != nullptr) {
        delete parityBuf;
        parityBuf = nullptr;
    }
}

}
}
