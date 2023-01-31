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
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemInter.h"
#include "HWCrc32c.h"
#include "LeaseRenewer.h"
#include "Logger.h"
#include "OutputStream.h"
#include "StripedOutputStreamImpl.h"
#include "Packet.h"
#include "StripedBlockUtil.h"
#include "Preconditions.h"
#include "server/ExtendedBlock.h"

#include <cassert>
#include <inttypes.h>

namespace Hdfs {
namespace Internal {

StripedOutputStreamImpl::StripedOutputStreamImpl() : OutputStreamImpl() {
#ifdef MOCK
    stub = nullptr;
#endif
}

StripedOutputStreamImpl::StripedOutputStreamImpl(shared_ptr<ECPolicy> ecPolicy) :
    OutputStreamImpl(), ecPolicy(ecPolicy), blockGroupIndex(0) {
}

void StripedOutputStreamImpl::checkStatus() {
    if (closed) {
        THROW(HdfsIOException, "StripedOutputStreamImpl: stream is not opened.");
    }

    lock_guard < mutex > lock(mut);

    if (lastError != exception_ptr()) {
        rethrow_exception(lastError);
    }
}

void StripedOutputStreamImpl::setError(const exception_ptr & error) {
    try {
        lock_guard < mutex > lock(mut);
        lastError = error;
    } catch (...) {
    }
}

void StripedOutputStreamImpl::open(shared_ptr<FileSystemInter> fs, const char * path,
                                   std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> & pair,
                                   int flag, const Permission & permission, bool createParent, int replication,
                                   int64_t blockSize, int64_t fileId) {
    if (nullptr == path || 0 == strlen(path) || replication < 0 || blockSize < 0) {
        THROW(InvalidParameter, "Invalid parameter.");
    }

    if (!(flag == Create || flag == (Create | SyncBlock) || flag == Overwrite
            || flag == (Overwrite | SyncBlock) || flag == Append
            || flag == (Append | SyncBlock) || flag == (Create | Overwrite)
            || flag == (Create | Overwrite | SyncBlock)
            || flag == (Create | Append)
            || flag == (Create | Append | SyncBlock))) {
        THROW(InvalidParameter, "Invalid flag.");
    }

    try {
        openInternal(fs, path, pair, flag, permission, createParent, replication, blockSize, fileId);
        if (flag & Append) {
            shared_ptr<LocatedBlock> lastBlock = pair.first;
            prevBlockGroup4Append = lastBlock != nullptr ? lastBlock : nullptr;
        }
    } catch (...) {
        reset();
        throw;
    }
}

void StripedOutputStreamImpl::openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                                           std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> & pair,
                                           int flag, const Permission & permission, bool createParent,
                                           int replication, int64_t blockSize, int64_t fileId) {
    OutputStreamImpl::openInternal(fs, path, pair, flag, permission, createParent, replication, blockSize, fileId);

    do {
        try {
            //append
            if (flag & Append) {
                initAppend(pair);
                LeaseRenewer::GetLeaseRenewer().StartRenew(filesystem);
                break;
            }
        } catch (const FileNotFoundException & e) {
            if (!(flag & Create)) {
                throw;
            }
        }
        //create
        assert((flag & Create) || (flag & Overwrite));
        closed = false;
        computePacketChunkSize();
        LeaseRenewer::GetLeaseRenewer().StartRenew(filesystem);
    } while(0);                               

    // EC Policy
    checksumSize = sizeof(int32_t);
    lastSend = steady_clock::now();
    cellSize = ecPolicy->getCellSize();
    numAllBlocks = ecPolicy->getNumDataUnits() + ecPolicy->getNumParityUnits();
    numDataBlocks = ecPolicy->getNumDataUnits();
    ErasureCoderOptions coderOptions(numDataBlocks, numAllBlocks - numDataBlocks);
    encoder = shared_ptr<RawErasureEncoder> (new RawErasureEncoder(coderOptions));

    flushAllThreadPool = shared_ptr<ThreadPool>(new ThreadPool(numAllBlocks));
    coordinator = shared_ptr<Coordinator>(new Coordinator(numAllBlocks));
    cellBuffers = shared_ptr<CellBuffers>(new CellBuffers(this));

    streamers.resize(numAllBlocks);
    for (int i = 0; i < numAllBlocks; i++) {
        shared_ptr<StripedDataStreamer> streamer =
                shared_ptr<StripedDataStreamer>(
                        new StripedDataStreamer(i, coordinator));
        streamers[i] = streamer;
    }

    currentPackets.resize(numAllBlocks);
    setCurrentStreamer(0);
}

shared_ptr<StripedOutputStreamImpl::StripedDataStreamer>
        StripedOutputStreamImpl::setCurrentStreamer(int newIdx) {
    lock_guard < mutex > lock(mut);
    // backup currentPacket for current streamer
    if (currentStreamer != nullptr) {
        int oldIdx = getCurrentStreamer()->getIndex();
        if (oldIdx >= 0) {
            currentPackets[oldIdx] = currentPacket;
        }
    }

    currentStreamer = getStripedDataStreamer(newIdx);
    currentPacket = currentPackets[newIdx];
    currentPipeline = currentStreamer->getPipeline();

    return currentStreamer;
}

int StripedOutputStreamImpl::getCurrentIndex() {
    return getCurrentStreamer()->getIndex();
}

bool StripedOutputStreamImpl::shouldEndBlockGroup() {
    return currentBlockGroup != nullptr &&
           currentBlockGroup->getNumBytes() == blockSize * numDataBlocks;
}

void StripedOutputStreamImpl::replaceFailedStreamers() {
    assert(streamers.size() == numAllBlocks);
    int currentIndex = getCurrentIndex();
    assert(currentIndex == 0);
    for (int i = 0; i < numAllBlocks; i++) {
        auto oldStreamer = getStripedDataStreamer(i);
        if (!oldStreamer->isHealthy()) {
            LOG(INFO, "replacing previously failed streamer(index=%d)", oldStreamer->getIndex());
            auto streamer = shared_ptr<StripedDataStreamer>(new StripedDataStreamer(i, coordinator));
            streamers[i] = streamer;
            currentPackets[i] = nullptr;
            if (i == currentIndex) {
                currentStreamer = streamer;
                currentPacket = nullptr;
            }
        }
    }
}

void StripedOutputStreamImpl::closeAllStreamers() {
    // The write has failed, close all the streamers.
    for (int i = 0; i < static_cast<int>(streamers.size()); i++) {
        streamers[i]->close(true);
    }
}

void StripedOutputStreamImpl::allocateNewBlock() {
    failedStreamers.clear();

    // replace failed streamers
    std::vector<DatanodeInfo> excludeNodes;
    shared_ptr<LocatedBlock> prevBlockGroup = currentBlockGroup;
    if (prevBlockGroup4Append != nullptr) {
        prevBlockGroup = prevBlockGroup4Append;
        prevBlockGroup4Append = nullptr;
    }

    replaceFailedStreamers();

    shared_ptr<LocatedBlock> lb;
    try {
        lb = filesystem->addBlock(path, prevBlockGroup.get(), excludeNodes, fileId);
    } catch (...) {
        closeAllStreamers();
        THROW(HdfsIOException, "Cannot addBlock for EC.");
    }
    assert(lb->isStriped());
    // assign the new block to the current block group
    currentBlockGroup = lb;
    blockGroupIndex++;

    std::vector<LocatedBlock> blocks;
    StripedBlockUtil::parseStripedBlockGroup(*currentBlockGroup, cellSize, numDataBlocks,
                                             numAllBlocks - numDataBlocks, blocks);

    for (int i = 0; i < static_cast<int>(blocks.size()); i++) {
        coordinator->getFollowingBlocks()->push(i, blocks[i]);
    }
}

void StripedOutputStreamImpl::writeParity(int index, shared_ptr<ByteBuffer> buffer) {
    shared_ptr<StripedDataStreamer> current = setCurrentStreamer(index);
    int len = buffer->limit();

    if (current->isHealthy()) {
        try {
            int pos = 0;
            for (int i = 0; i < len; i += chunkSize) {
                int chunkLen = min(chunkSize, len - i);
                checksum->reset();
                checksum->update((const char *)buffer->getBuffer() + pos, chunkLen);
                appendChunkToPacketInternal((const char * )buffer->getBuffer() + pos, chunkLen);
                pos += chunkLen;
            }
        } catch(...) {
            int64_t oldBytes = (current->getPipeline() != nullptr) ? 
                                current->getPipeline()->getBytesSent() : 0;
            std::stringstream ss;
            ss << "oldBytes=" << oldBytes;
            ss << ", len=" << len;
            ss << ", i=" << current->getIndex();
            handleStreamerFailure(ss.str().c_str(), Hdfs::current_exception(), getCurrentStreamer());
        }
    }
}

bool StripedOutputStreamImpl::checkAnyParityStreamerIsHealthy() {
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
        if (streamers.at(i)->isHealthy()) {
            return true;
        }
    }
    return false;
}

void StripedOutputStreamImpl::encode(shared_ptr<RawErasureEncoder> encoder, int numData,
                                     std::vector<shared_ptr<ByteBuffer>> buffers) {
    std::vector<shared_ptr<ByteBuffer>> dataBuffers;
    std::vector<shared_ptr<ByteBuffer>> parityBuffers;

    dataBuffers.resize(numData);
    for (int i = 0; i < numData; i++) {
        dataBuffers[i] = buffers[i];
    }
    parityBuffers.resize(buffers.size() - numData);
    for (int j = numData; j < static_cast<int>(buffers.size()); j++) {
        parityBuffers[j - numData] = buffers[j];
    }

    encoder->encode(dataBuffers, parityBuffers);
}

void StripedOutputStreamImpl::writeParityCells() {
    std::vector<shared_ptr<ByteBuffer>> buffers = cellBuffers->getBuffers();
    // Skips encoding and writing parity cells if there are no healthy parity
    // data streamers
    if (!checkAnyParityStreamerIsHealthy()) {
        return;
    }
    //encode the data cells
    encode(encoder, numDataBlocks, buffers);
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
        writeParity(i, buffers[i]);
    }
    cellBuffers->clear();
}

void StripedOutputStreamImpl::flushAllInternals() {
    std::map<int, std::future<void>> flushAllFuturesMap;
    int current = getCurrentIndex();
    for (int i = 0; i < numAllBlocks; i++) {
        shared_ptr<StripedDataStreamer> s = setCurrentStreamer(i);
        if (s->isHealthy() && s->getPipeline() != nullptr) {
            try {
                // flush all data to Datanode
                std::future<void> future = flushAllThreadPool->enqueue(
                    [s] {
                        s->getPipeline()->waitForAcks(true);
                    }
                );
                flushAllFuturesMap.insert(
                    std::pair<int, std::future<void>>{i, std::move(future)});
            } catch (...) {
                std::stringstream ss;
                ss << "flushInternal, index=" << current;
                handleStreamerFailure(ss.str().c_str(), Hdfs::current_exception(), getCurrentStreamer());
            }
        }
    }
    setCurrentStreamer(current);
    for (auto & kv : flushAllFuturesMap) {
        try {
            std::future<void> & future = kv.second;
            future.get();
        } catch (...) {
            const shared_ptr<StripedDataStreamer> s = streamers[kv.first];
            std::stringstream ss;
            ss << "flushInternal future get failed, index=" << kv.first;
            handleStreamerFailure(ss.str().c_str(), Hdfs::current_exception(), s);
        }
    }
}

void StripedOutputStreamImpl::closePipeline() {
    lock_guard < mutex > lock(mut);

    if (!currentPipeline) {
        return;
    }

    if (getCurrentStreamer()->getBytesCurBlock() == blockSize) {
        setCurrentPacketToEmpty();
        currentPipeline->close(currentPacket);
        currentPacket.reset();
        currentPipeline.reset();
        getCurrentStreamer()->resetPipeline();
        getCurrentStreamer()->setBytesCurBlock(0);
    }
}

int StripedOutputStreamImpl::stripeDataSize() {
    return numDataBlocks * cellSize;
}

shared_ptr<StripedOutputStreamImpl::StripedDataStreamer> StripedOutputStreamImpl::getCurrentStreamer() {
    return currentStreamer;
}

shared_ptr<StripedOutputStreamImpl::StripedDataStreamer> StripedOutputStreamImpl::getStripedDataStreamer(int i) {
    return streamers.at(i);
}

void StripedOutputStreamImpl::setCurrentPacketToEmpty() {
    currentPacket = make_shared<Packet>(packetSize, chunksPerPacket, getCurrentStreamer()->getBytesCurBlock(),
                                        getCurrentStreamer()->getAndIncNextSeqNo(), checksumSize);
}

void StripedOutputStreamImpl::initAppend(std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus>> & lastBlockWithStatus) {
    FileStatus fileInfo;

    if (lastBlockWithStatus.second) {
        fileInfo = *lastBlockWithStatus.second;
    } else {
        fileInfo = filesystem->getFileStatus(path.c_str());
    }

    closed = false;
    blockSize = fileInfo.getBlockSize();
    cursor = fileInfo.getLength();

    computePacketChunkSize();
}

void StripedOutputStreamImpl::append(const char * buf, int64_t size) {
    LOG(DEBUG1, "append file %s size is %" PRId64 ", offset %" PRId64 " next pos %" PRId64, path.c_str(), size, cursor, size + cursor);

    if (nullptr == buf || size < 0) {
        THROW(InvalidParameter, "Invalid parameter.");
    }

    checkStatus();

    try {
        appendInternal(buf, size);
    } catch (...) {
        setError(current_exception());
        throw;
    }
}

void StripedOutputStreamImpl::appendInternal(const char * buf, int64_t size) {
    int64_t todo = size;

    while (todo > 0) {
        int batch = buffer.size() - position;
        batch = batch < todo ? batch : static_cast<int>(todo);

        /*
         * bypass buffer.
         */
        if (0 == position && todo >= static_cast<int64_t>(buffer.size())) {
            checksum->reset();
            checksum->update(buf + size - todo, batch);
            appendChunkToPacket(buf + size - todo, batch);
        } else {
            checksum->reset();
            checksum->update(buf + size - todo, batch);
            memcpy(buffer.data() + position, buf + size - todo, batch);
            position += batch;

            if (position == static_cast<int>(buffer.size())) {
                appendChunkToPacket(buffer.data(), buffer.size());
                position = 0;
            }
        }

        todo -= batch;
    }

    cursor += size;
}

void StripedOutputStreamImpl::appendChunkToPacketInternal(const char * buf, int size) {
    assert(nullptr != buf && size > 0);
    if (!currentPacket) {
        currentPacket = packets.getPacket(packetSize, chunksPerPacket,
                                          getCurrentStreamer()->getPipeline() != nullptr ?
                                          getCurrentStreamer()->getPipeline()->getBytesSent() : 0,
                                          getCurrentStreamer()->getAndIncNextSeqNo(), checksumSize);
    }

    currentPacket->addChecksum(checksum->getValue());
    checksum->reset();
    currentPacket->addData(buf, size);
    currentPacket->increaseNumChunks();
    getCurrentStreamer()->incBytesCurBlock(size);
    if (getCurrentStreamer()->getIndex() <= numDataBlocks) {
        bytesWritten += size;
    }

    // If packet is full, enqueue it for transmission
    if (currentPacket
        && (currentPacket->isFull() ||
            (getCurrentStreamer()->getBytesCurBlock() == blockSize))) {
        sendPacket(currentPacket);
    }
}

std::set<shared_ptr<StripedOutputStreamImpl::StripedDataStreamer>> 
StripedOutputStreamImpl::checkStreamers() {
    std::set<shared_ptr<StripedDataStreamer>> newFailed;
    for (auto s : streamers) {
        if (!s->isHealthy()) {
            auto it = find(failedStreamers.begin(), failedStreamers.end(), s);
            if (it == failedStreamers.end()) {
                newFailed.insert(s);
            }
        }
    }

    int failCount = failedStreamers.size() + newFailed.size();
    if (failCount > (numAllBlocks - numDataBlocks)) {
        closeAllStreamers();
        THROW(HdfsIOException, "Failed: the number of failed blocks = %d"
            " > the number of parity blocks = %d", 
            failCount, (numAllBlocks - numDataBlocks));
    }
    return newFailed;
}

void StripedOutputStreamImpl::handleStreamerFailure(const char* err, Hdfs::exception_ptr eptr, 
                                                    shared_ptr<StripedDataStreamer> streamer) {
    std::string ex;
    try {
        Hdfs::rethrow_exception(eptr);
    } catch (const std::exception & e) {
        ex = e.what();
    }
    LOG(WARNING, "Failed: %s, %s", err, ex.c_str());
    streamer->setInternalError(true);
    streamer->close(true);
    checkStreamers();
    currentPackets[streamer->getIndex()] = nullptr;

    currentPacket.reset();
    currentPipeline.reset();
    streamer->resetPipeline();
}

void StripedOutputStreamImpl::appendChunkToPacket(const char * buf, int size) {
    assert(nullptr != buf && size > 0);

    int index = getCurrentIndex();
    int pos = cellBuffers->addTo(index, buf, size);
    bool cellFull = pos == cellSize;

    if (currentBlockGroup == nullptr || shouldEndBlockGroup()) {
        // the incoming data should belong to a new block. Allocate a new block.
        allocateNewBlock();
    }

    currentBlockGroup->setNumBytes(currentBlockGroup->getNumBytes() + size);

    // note: the current streamer can be refreshed after allocating a new block
    if (getCurrentStreamer()->isHealthy()) {
        try {
            appendChunkToPacketInternal(buf, size);
        } catch (...) {
            std::stringstream ss;
            ss << "failed stream index=" << index;
            handleStreamerFailure(ss.str().c_str(), Hdfs::current_exception(), getCurrentStreamer());
        }
    }

    // Two extra steps are needed when a striping cell is full:
    // 1. Forward the current index pointer
    // 2. Generate parity packets if a full stripe of data cells are present
    if (cellFull) {
        int next = index + 1;
        //When all data cells in a stripe are ready, we need to encode
        //them and generate some parity cells. These cells will be
        //converted to packets and put to their DataStreamer's queue.
        if (next == numDataBlocks) {
            cellBuffers->flipDataBuffers();
            writeParityCells();
            next = 0;

            // if this is the end of the block group, end each internal block
            if (shouldEndBlockGroup()) {
                flushAllInternals();
                checkStreamerFailures(false);
                for (int i = 0; i < numAllBlocks; i++) {
                    shared_ptr<StripedDataStreamer> s = setCurrentStreamer(i);
                    if (s->isHealthy()) {
                        try {
                            closePipeline();
                        } catch (...) {}
                    }
                }
            } else {
                // check failure state for all the streamers. Bump GS if necessary
                checkStreamerFailures(true);
            }
        }
        setCurrentStreamer(next);
    }
}

void StripedOutputStreamImpl::sendPacket(shared_ptr<Packet> packet) {
    lock_guard < mutex > lock(mut);
    if (!currentPipeline) {
        setupPipeline();
    }

    currentPipeline->send(packet);
    currentPacket.reset();
    lastSend = steady_clock::now();
}

void StripedOutputStreamImpl::setupPipeline() {
    assert(currentPacket);

    LocatedBlock lb = getCurrentStreamer()->getFollowingBlock();
    getCurrentStreamer()->popFollowingBlock();
    currentPipeline = std::make_shared<StripedPipelineImpl>(path.c_str(), *conf, filesystem,
                                                            CHECKSUM_TYPE_CRC32C, conf->getDefaultChunkSize(),
                                                            replication, currentPacket->getOffsetInBlock(), packets,
                                                            std::make_shared<LocatedBlock>(std::move(lb)), fileId);

    getCurrentStreamer()->setPipeline(currentPipeline);
    lastSend = steady_clock::now();
}

/**
 * Flush all data in buffer and waiting for ack.
 * Will block until get all acks.
 */
void StripedOutputStreamImpl::flush() {
    LOG(DEBUG3, "StripedOutputStream does not support flush");
}

/**
 * @ref StripedOutputStream::sync
 */
void StripedOutputStreamImpl::sync() {
    LOG(DEBUG3, "StripedOutputStream does not support sync");
}

void StripedOutputStreamImpl::completeFile(bool throwError) {
    steady_clock::time_point start = steady_clock::now();

    while (true) {
        try {
            bool success;
            success = filesystem->complete(path, currentBlockGroup.get(), fileId);

            if (success) {
                return;
            }
        } catch (HdfsIOException & e) {
            if (throwError) {
                NESTED_THROW(HdfsIOException,
                             "StripedOutputStreamImpl: failed to complete file %s.",
                             path.c_str());
            } else {
                return;
            }
        }

        if (closeTimeout > 0) {
            steady_clock::time_point end = steady_clock::now();

            if (ToMilliSeconds(start, end) >= closeTimeout) {
                if (throwError) {
                    THROW(HdfsIOException,
                          "StripedOutputStreamImpl: timeout when complete file %s, timeout interval %d ms.",
                          path.c_str(), closeTimeout);
                } else {
                    return;
                }
            }
        }

        try {
            sleep_for(milliseconds(400));
        } catch (...) {
        }
    }
}

void StripedOutputStreamImpl::flushBuffer() {
    if (position != 0) {
        appendChunkToPacket(buffer.data(), position);
        bytesWritten += buffer.size();
        position = 0;
    }
}

bool StripedOutputStreamImpl::generateParityCellsForLastStripe() {
    long currentBlockGroupBytes = currentBlockGroup == nullptr ?
        0 : currentBlockGroup->getNumBytes();
    long lastStripeSize = currentBlockGroupBytes % stripeDataSize();
    if (lastStripeSize == 0) {
        return false;
    }

    long parityCellSize = lastStripeSize < cellSize ? lastStripeSize : cellSize;
    std::vector<shared_ptr<ByteBuffer>> buffers = cellBuffers->getBuffers();

    for (int i = 0; i < numAllBlocks; i++) {
        // Pad zero bytes to make all cells exactly the size of parityCellSize
        // If internal block is smaller than parity block, pad zero bytes.
        // Also pad zero bytes to all parity cells
        int position = buffers[i]->position();

        assert (position <= parityCellSize && "If an internal block is smaller than parity block, "
            "then its last cell should be small than last parity cell");
        for (int j = 0; j < parityCellSize - position; j++) {
            buffers[i]->put((uint8_t) 0);
        }
        buffers[i]->flip();
    }
    return true;
}

void StripedOutputStreamImpl::enqueueAllCurrentPackets() {
    int idx = getCurrentIndex();
    for (int i = 0; i < static_cast<int>(streamers.size()); i++) {
        shared_ptr<StripedDataStreamer> si = setCurrentStreamer(i);
        if (si->isHealthy() && currentPacket != nullptr) {
            try {
                sendPacket(currentPacket);
            } catch (...) {
                std::stringstream ss;
                ss << "enqueueAllCurrentPackets, i=" << i;
                handleStreamerFailure(ss.str().c_str(), Hdfs::current_exception(), getCurrentStreamer());
            }
        }
    }
    setCurrentStreamer(idx);
}

bool StripedOutputStreamImpl::isStreamerWriting(int index) {
    int64_t length = (currentBlockGroup == nullptr) ?
        0 : currentBlockGroup->getNumBytes();
    if (length == 0) {
      return false;
    }
    if (index >= numDataBlocks) {
      return true;
    }
    int numCells = static_cast<int>(((length - 1) / cellSize) + 1);
    return index < numCells;
}

ExtendedBlock StripedOutputStreamImpl::updateBlockForPipeline(
    std::set<shared_ptr<StripedDataStreamer>> healthyStreamers) {
    shared_ptr<LocatedBlock> updated = filesystem->updateBlockForPipeline(*currentBlockGroup);
    int64_t newGS = updated->getGenerationStamp();
    ExtendedBlock newBlock = static_cast<ExtendedBlock>(*currentBlockGroup);
    newBlock.setGenerationStamp(newGS);

    std::vector<LocatedBlock> updatedBlks;
    StripedBlockUtil::parseStripedBlockGroup(*updated, cellSize, numDataBlocks, 
                                             numAllBlocks - numDataBlocks, updatedBlks);
    for (int i = 0; i < numAllBlocks; i++) {
        auto si = getStripedDataStreamer(i);
        if (healthyStreamers.count(si) > 0) {
            si->getPipeline()->createBlockOutputStream(updatedBlks[i].getToken(), newGS, true);
        }
    }

    return newBlock;
}

std::vector<int64_t> StripedOutputStreamImpl::getBlockLengths() {
    std::vector<int64_t> blockLengths(numAllBlocks);
        for (int i = 0; i < numAllBlocks; i++) {
        auto streamer = getStripedDataStreamer(i);
        int64_t numBytes = -1;
        if (streamer->isHealthy() && streamer->getPipeline()) {
            numBytes = streamer->getPipeline()->getBytesSent();
        }
        blockLengths.push_back(numBytes);
    }
    return blockLengths;
}

int64_t StripedOutputStreamImpl::getAckedLength() {
    // Determine the number of full stripes that are sufficiently durable
    int64_t sentBytes = currentBlockGroup->getNumBytes();
    int64_t numFullStripes = sentBytes / numDataBlocks / cellSize;
    int64_t fullStripeLength = numFullStripes * numDataBlocks * cellSize;
    assert(fullStripeLength <= sentBytes);

    int64_t ackedLength = 0;

    // Determine the length contained by at least `numDataBlocks` blocks.
    // Since it's sorted, all the blocks after `offset` are at least as long,
    // and there are at least `numDataBlocks` at or after `offset`.
    std::vector<int64_t> blockLengths(getBlockLengths());
    std::sort(blockLengths.begin(), blockLengths.end());
    if (numFullStripes > 0) {
        int offset = blockLengths.size() - numDataBlocks;
        ackedLength = blockLengths[offset] * numDataBlocks;
    }

    // If the acked length is less than the expected full stripe length, then
    // we're missing a full stripe. Return the acked length.
    if (ackedLength < fullStripeLength) {
        return ackedLength;
    }
    // If the expected length is exactly a stripe boundary, then we're also done
    if (ackedLength == sentBytes) {
        return ackedLength;
    }

    /*
    Otherwise, we're potentially dealing with a partial stripe.
    The partial stripe is laid out as follows:

      0 or more full data cells, `cellSize` in length.
      0 or 1 partial data cells.
      0 or more empty data cells.
      `numParityBlocks` parity cells, the length of the longest data cell.

    If the partial stripe is sufficiently acked, we'll update the ackedLength.
    */

    // How many full and empty data cells do we expect?
    int numFullDataCells = static_cast<int>(
        ((sentBytes - fullStripeLength) / cellSize));
    int partialLength = static_cast<int>((sentBytes - fullStripeLength) % cellSize);
    int numPartialDataCells = partialLength == 0 ? 0 : 1;
    int numEmptyDataCells = numDataBlocks - numFullDataCells - numPartialDataCells;
    // Calculate the expected length of the parity blocks.
    int parityLength = numFullDataCells > 0 ? cellSize : partialLength;

    int64_t fullStripeBlockOffset = fullStripeLength / numDataBlocks;

    // Iterate through each type of streamers, checking the expected length.
    int64_t expectedBlockLengths[numAllBlocks];
    int idx = 0;
    // Full cells
    for (; idx < numFullDataCells; idx++) {
        expectedBlockLengths[idx] = fullStripeBlockOffset + cellSize;
    }
    // Partial cell
    for (; idx < numFullDataCells + numPartialDataCells; idx++) {
        expectedBlockLengths[idx] = fullStripeBlockOffset + partialLength;
    }
    // Empty cells
    for (; idx < numFullDataCells + numPartialDataCells + numEmptyDataCells;
         idx++) {
        expectedBlockLengths[idx] = fullStripeBlockOffset;
    }
    // Parity cells
    for (; idx < numAllBlocks; idx++) {
        expectedBlockLengths[idx] = fullStripeBlockOffset + parityLength;
    }

    // Check expected lengths against actual streamer lengths.
    // Update if we have sufficient durability.
    int numBlocksWithCorrectLength = 0;
    for (int i = 0; i < numAllBlocks; i++) {
        if (blockLengths[i] == expectedBlockLengths[i]) {
            numBlocksWithCorrectLength++;
        }
    }
    if (numBlocksWithCorrectLength >= numDataBlocks) {
        ackedLength = sentBytes;
    }

    return ackedLength;
}

void StripedOutputStreamImpl::updatePipeline(ExtendedBlock newBG) {
    std::vector<DatanodeInfo> newNodes(numAllBlocks);
    std::vector<std::string> newStorageIDs(numAllBlocks);
    const std::vector<DatanodeInfo>& nodes = currentBlockGroup->getLocations();
    const std::vector<std::string>& storageIDs = currentBlockGroup->getStorageIDs();
    for (int i = 0; i < numAllBlocks; i++) {
        auto streamer = getStripedDataStreamer(i);
        if (streamer->isHealthy()) {
            newNodes[i] = nodes[i];
            newStorageIDs[i] = storageIDs[i];
        } else {
            newStorageIDs[i] = "";
        }
    }

    int64_t sentBytes = currentBlockGroup->getNumBytes();
    int64_t ackedBytes = getAckedLength();
    assert(ackedBytes <= sentBytes);
    currentBlockGroup->setNumBytes(ackedBytes);
    newBG.setNumBytes(ackedBytes);
    filesystem->updatePipeline(*currentBlockGroup, newBG, newNodes, newStorageIDs);

    // update currentBlockGroup
    currentBlockGroup->setGenerationStamp(newBG.getGenerationStamp());
    currentBlockGroup->setNumBytes(sentBytes);   
}

void StripedOutputStreamImpl::checkStreamerFailures(bool needFlush) {
    std::set<shared_ptr<StripedDataStreamer>> newFailed = checkStreamers();
    if (newFailed.size() == 0) {
        return;
    }

    if (needFlush) {
        // for healthy streamers, wait till all of them have fetched the new block
        // and flushed out all the enqueued packets.
        flushAllInternals();
    }

    // recheck failed streamers again after the flush
    newFailed = checkStreamers();
    if (newFailed.size() > 0) {
        failedStreamers.insert(failedStreamers.end(), newFailed.begin(), newFailed.end());
        corruptBlockCountMap.insert(make_pair(blockGroupIndex, failedStreamers.size()));

        // mark all the healthy streamers
        std::set<shared_ptr<StripedDataStreamer>> healthySet;
        for (int i = 0; i < numAllBlocks; i++) {
            auto streamer = getStripedDataStreamer(i);
            if (streamer->isHealthy() && isStreamerWriting(i)) {
                healthySet.insert(streamer);
            }
        }

        // we have newly failed streamers, update block for pipeline
        ExtendedBlock newBG = updateBlockForPipeline(healthySet);
        newFailed.clear();

        // the updated block
        updatePipeline(newBG);
    }
}

void StripedOutputStreamImpl::close() {
    exception_ptr e;
    try {
        if (closed) {
            return;
        }

        // flush from all upper layers
        flushBuffer();
        // if the last stripe is incomplete, generate and write parity cells
        if (generateParityCellsForLastStripe()) {
            writeParityCells();
        }
        enqueueAllCurrentPackets();

        // flush all the data packets and wait for acks.
        flushAllInternals();
        // check failures.
        checkStreamerFailures(false);

        // end blocks.
        for (int i = 0; i < numAllBlocks; i++) {
            shared_ptr<StripedDataStreamer> s = setCurrentStreamer(i);
            if (s->isHealthy() && s->getPipeline() != nullptr) {
                try {
                    if (s->getBytesCurBlock() > 0) {
                        setCurrentPacketToEmpty();
                    }
                    // flush the last "close" packet to Datanode
                    currentPipeline->close(currentPacket);
                } catch (...) {
                    // TODO for both close and endBlock, we currently do not handle
                    // failures when sending the last packet. We actually do not need to
                    // bump GS for this kind of failure. Thus counting the total number
                    // of failures may be good enough.
                }
            }
        }
        completeFile(true);
    } catch (...) {
        e = current_exception();
    }

    LeaseRenewer::GetLeaseRenewer().StopRenew(filesystem);
    LOG(DEBUG1, "close file %s for write with length %" PRId64, path.c_str(), cursor);
    reset();

    if (e) {
        rethrow_exception(e);
    }
}

std::string StripedOutputStreamImpl::toString() {
    if (path.empty()) {
        return std::string("StripedOutputStream for path ") + path;
    } else {
        return std::string("StripedOutputStream (not opened)");
    }
}

// CellBuffers
StripedOutputStreamImpl::CellBuffers::CellBuffers(const StripedOutputStreamImpl *sosi) : sosi(sosi) {
    int cellSize = sosi->cellSize;
    int numAllBlocks = sosi->numAllBlocks;
    int chunkSize = sosi->chunkSize;
    if (cellSize % chunkSize != 0) {
        THROW(InvalidParameter,
              "StripedOutputStreamImpl: Invalid values: dfs.bytes-per-checksum = %d must divide cell size = %d",
              chunkSize, cellSize);
    }

    buffers.resize(numAllBlocks);
    for (int i = 0; i < numAllBlocks; i++) {
        buffers[i] = shared_ptr<ByteBuffer>(new ByteBuffer(cellSize));
    }
}

std::vector<shared_ptr<ByteBuffer>> StripedOutputStreamImpl::CellBuffers::getBuffers() {
    return buffers;
}

int StripedOutputStreamImpl::CellBuffers::addTo(int i, const char * b, int len) {
    shared_ptr<ByteBuffer> buf = buffers[i];
    int pos = buf->position() + len;
    Preconditions::checkState(pos <= sosi->cellSize);
    buf->putInt8_ts((int8_t *)b, len);
    return pos;
}

void StripedOutputStreamImpl::CellBuffers::clear() {
    for (int i = 0; i < sosi->numAllBlocks; i++) {
        buffers[i]->clear();
        buffers[i]->limit(sosi->cellSize);
    }
}

void StripedOutputStreamImpl::CellBuffers::flipDataBuffers() {
    for (int i = 0; i < sosi->numDataBlocks; i++) {
        buffers[i]->flip();
    }
}

// MultipleBlockingQueue
template<class T>
StripedOutputStreamImpl::MultipleBlockingQueue<T>::MultipleBlockingQueue() {
}

template<class T>
StripedOutputStreamImpl::MultipleBlockingQueue<T>::MultipleBlockingQueue(int numQueue) {
    for (int i = 0; i < numQueue; i++) {
        queues.push_back(shared_ptr<std::queue<T>>(new std::queue<T>()));
    }
}

template<class T>
void StripedOutputStreamImpl::MultipleBlockingQueue<T>::push(int i, T t) {
    queues[i]->push(t);
}

template<class T>
void StripedOutputStreamImpl::MultipleBlockingQueue<T>::pop(int i) {
    queues[i]->pop();
}

template<class T>
T& StripedOutputStreamImpl::MultipleBlockingQueue<T>::front(int i) {
    return queues[i]->front();
}

template<class T>
void StripedOutputStreamImpl::MultipleBlockingQueue<T>::clear() {
    for (int i = 0; i < queues.size; i++) {
        queues[i]->clear();
    }
}

// Coordinator
StripedOutputStreamImpl::Coordinator::Coordinator(){
}

StripedOutputStreamImpl::Coordinator::Coordinator(int numAllBlocks) {
    followingBlocks =
        shared_ptr<MultipleBlockingQueue<LocatedBlock>>(new MultipleBlockingQueue<LocatedBlock>(numAllBlocks));
    endBlocks =
        shared_ptr<MultipleBlockingQueue<LocatedBlock>>(new MultipleBlockingQueue<LocatedBlock>(numAllBlocks));
}

shared_ptr<StripedOutputStreamImpl::MultipleBlockingQueue<LocatedBlock>>
StripedOutputStreamImpl::Coordinator::getFollowingBlocks() {
    return followingBlocks;
}

}
}
