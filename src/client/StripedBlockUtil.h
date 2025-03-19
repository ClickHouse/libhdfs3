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
#ifndef _HDFS_LIBHDFS3_STRIPED_BLOCK_UTIL_H_
#define _HDFS_LIBHDFS3_STRIPED_BLOCK_UTIL_H_

#include <ostream>
#include <string>
#include <algorithm>
#include <climits>
#include <set>
#include <map>
#include <future>
#include "server/ExtendedBlock.h"
#include "server/LocatedBlock.h"
#include "ECPolicy.h"
#include "ByteBuffer.h"
#include "Logger.h"
#include "Preconditions.h"

namespace Hdfs {
namespace Internal {
class StripedBlockUtil {
public:
    class VerticalRange {
    public:
        /** start offset in the block group (inclusive). */
        long offsetInBlock;
        /** length of the stripe range. */
        long spanInBlock;

    public:
	    VerticalRange() :offsetInBlock(0), spanInBlock(0) {}
        VerticalRange(long offset, long length) {
            offsetInBlock = offset;
            spanInBlock = length;

            std::stringstream ss;
            ss << "offsetInBlock=" + std::to_string(offset)
               << " length=" + std::to_string(length)
               << " must be non-negative.";
            Preconditions::checkArgument(offset >= 0 && length >= 0, ss.str());
        }

	    void setOffsetInBlock(long offset) {
	        offsetInBlock = offset;
	    }

	    void setSpanInBlock(long span) {
	        spanInBlock = span;
	    }

            /** whether a position is in the range. */
            bool include(long pos) {
                return pos >= offsetInBlock && pos < offsetInBlock + spanInBlock;
            }

            ostream & operator <<(ostream & outputStream) {
                outputStream << "VerticalRange(offsetInBlock=" << offsetInBlock
                    << ", spanInBlock=" << spanInBlock << ")";
                return outputStream;
            }
    };

    class ChunkByteBuffer {
        std::vector<ByteBuffer*> slices;

	public:
        ChunkByteBuffer() {
        }

        ByteBuffer* getSlice(int i) const {
            return slices[i];
        }

        std::vector<ByteBuffer*> getSlices() const {
            return slices;
        }

        void addSlice(ByteBuffer* buffer, int offset, int len) {
            ByteBuffer* tmp = buffer->duplicate();
            tmp->position(buffer->position() + offset);
            tmp->limit(buffer->position() + offset + len);
            slices.push_back(tmp->slice());
        }

        /**
         *  Note: target will be ready-to-read state after the call.
         */
        void copyTo(ByteBuffer* target) {
            for (ByteBuffer* slice : slices) {
                slice->flip();
                target->put(slice);
            }
            target->clear();
        }

        void copyFrom(ByteBuffer * src) {
            ByteBuffer * tmp;
            int len;
            for (ByteBuffer* slice : slices) {
                len = slice->remaining();
                tmp = src->duplicate();
                tmp->limit(tmp->position() + len);
                slice->put(tmp);
                src->position(src->position() + len);
            }
        }
    };

    class StripeRange {
        /** start offset in the block group (inclusive). */
        long offsetInBlock;
        /** length of the stripe range. */
        long length;

    public:
        StripeRange(long offsetInBlock_, long length_) {
            offsetInBlock = offsetInBlock_;
            length = length_;

            std::stringstream ss;
            ss << "offsetInBlock=" + std::to_string(offsetInBlock)
                << " length=" + std::to_string(length)
                << " must be non-negative.";
            Preconditions::checkArgument(offsetInBlock >= 0 && length >= 0, ss.str());
        }

        bool include(long pos) {
            return pos >= offsetInBlock && pos < offsetInBlock + length;
        }

        long getLength() const {
            return length;
        }

        void setLength(long length_) {
            length = length_;
        }

        void setOffset(long offset_) {
            offsetInBlock = offset_;
        }

        ostream & operator <<(ostream & outputStream) {
            outputStream << "StripeRange(offsetInBlock=" << offsetInBlock
                << ", length=" << length;
            return outputStream;
        }
    };

    class StripingChunk {
    public:
        /** Chunk has been successfully fetched */
        static const int FETCHED = 0x01;
        /** Chunk has encountered failed when being fetched */
        static const int MISSING = 0x02;
        /** Chunk being fetched (fetching task is in-flight) */
        static const int PENDING = 0x04;
        /**
         * Chunk is requested either by application or for decoding, need to
         * schedule read task
         */
        static const int REQUESTED = 0X08;
        /**
         * Internal block is short and has no overlap with chunk. Chunk considered
         * all-zero bytes in codec calculations.
         */
        static const int ALLZERO = 0X0f;

        /**
         * If a chunk is completely in requested range, the state transition is:
         * REQUESTED (when AlignedStripe created) -> PENDING ->
         * {FETCHED | MISSING}
         * If a chunk is completely outside requested range (including parity
         * chunks), state transition is:
         * NULL (AlignedStripe created) -> REQUESTED (upon failure) ->
         * PENDING ...
         */
        int state = REQUESTED;

        ChunkByteBuffer * chunkBuffer;
        shared_ptr<ByteBuffer> byteBuffer;

    public:
        StripingChunk() {
            chunkBuffer = new ChunkByteBuffer();
            byteBuffer = nullptr;
        }

        StripingChunk(shared_ptr<ByteBuffer> buf) {
            chunkBuffer = nullptr;
            byteBuffer = buf;
        }

        StripingChunk(int state_) {
            chunkBuffer = nullptr;
            byteBuffer = nullptr;
            state = state_;
        }

        ~StripingChunk() {
            if (chunkBuffer != nullptr) {
                delete chunkBuffer;
                chunkBuffer = nullptr;
            }
        }

        bool useByteBuffer(){
            return byteBuffer != nullptr;
        }

        bool useChunkBuffer() {
            return chunkBuffer != nullptr;
        }

        ByteBuffer * getByteBuffer() const{
            return byteBuffer.get();
        }

        ChunkByteBuffer * getChunkBuffer() const {
            return chunkBuffer;
        }
    };

    class AlignedStripe {
    public:
        VerticalRange * range;
        /** status of each chunk in the stripe. */
        std::vector<StripingChunk*> chunks;
        int fetchedChunksNum = 0;
        int missingChunksNum = 0;

        AlignedStripe(long offsetInBlock, long length, int width) {
            Preconditions::checkArgument(offsetInBlock >= 0 && length >= 0,
                "Offset and length must be non-negative");
            range = new VerticalRange(offsetInBlock, length);
            chunks.resize(width);
        }

        ~AlignedStripe() {
            for (int i = 0; i < static_cast<int>(chunks.size()); ++i) {
                if (chunks[i] != nullptr) {
                    delete chunks[i];
                    chunks[i] = nullptr;
                }
            }

            if (range != nullptr) {
                delete range;
                range = nullptr;
            }
        }

        bool include(long pos) {
            return range->include(pos);
        }

        long getOffsetInBlock() const {
            return range->offsetInBlock;
        }

        long getSpanInBlock() const {
            return range->spanInBlock;
        }

        ostream & operator <<(ostream & outputStream) {
            outputStream << "AlignedStripe(Offset=" << range->offsetInBlock << ", length=" <<
                range->spanInBlock << ", fetchedChunksNum=" << fetchedChunksNum <<
                ", missingChunksNum=" << missingChunksNum << ")";
            return outputStream;
        }
    };

    class BlockReadStats {
    private:
        int bytesRead;
        bool shortCircuit;
        int networkDistance;

    public:
        BlockReadStats(int numBytesRead, bool shortCircuit_, int distance) {
            bytesRead = numBytesRead;
            shortCircuit = shortCircuit_;
            networkDistance = distance;
        }

        int getBytesRead() const {
            return bytesRead;
        }

        bool isShortCircuit() const {
            return shortCircuit;
        }

        int getNetworkDistance() const {
            return networkDistance;
        }

        ostream & operator <<(ostream & outputStream) {
            outputStream << "bytesRead=" << bytesRead << ", isShortCircuit=" << shortCircuit
                << ", networkDistance=" << networkDistance;
            return outputStream;
        }
    };

    class StripingCell {
    public:
        shared_ptr<ECPolicy> ecPolicy;
        /** Logical order in a block group, used when doing I/O to a block group. */
        long idxInBlkGroup;
        long idxInInternalBlk;
        int idxInStripe;
        /**
         * When a logical byte range is mapped to a set of cells, it might
         * partially overlap with the first and last cells. This field and the
         * size variable represent the start offset and size of the
         * overlap.
         */
        long offset;
        int size;

        StripingCell() = default;

        StripingCell(shared_ptr<ECPolicy> ecPolicy, int cellSize, long idxInBlkGroup,
            long cellOffset) {
            ecPolicy = ecPolicy;
            idxInBlkGroup = idxInBlkGroup;
            idxInInternalBlk = idxInBlkGroup / ecPolicy->getNumDataUnits();
            idxInStripe = static_cast<int>(idxInBlkGroup -
                idxInInternalBlk * ecPolicy->getNumDataUnits());
            offset = cellOffset;
            size = cellSize;
        }

        void init(shared_ptr<ECPolicy> ecPolicy, int cellSize, long idxInBlkGroup,
            long cellOffset) {
            ecPolicy = ecPolicy;
            idxInBlkGroup = idxInBlkGroup;
            idxInInternalBlk = idxInBlkGroup / ecPolicy->getNumDataUnits();
            idxInStripe = static_cast<int>(idxInBlkGroup -
                idxInInternalBlk * ecPolicy->getNumDataUnits());
            offset = cellOffset;
            size = cellSize;
        }

        int getIdxInStripe() const {
            return idxInStripe;
        }

        ostream & operator <<(ostream & outputStream) {
            outputStream << "StripingCell(idxInBlkGroup=" << idxInBlkGroup
                << ", idxInInternalBlk=" << idxInInternalBlk
                << ", idxInStrip=" << idxInStripe
                << ", offset=" << offset
                << ", size=" << size << ")";
            return outputStream;
        }
    };

    class StripingChunkReadResult {
    public:
        static const int SUCCESSFUL = 0x01;
        static const int FAILED = 0x02;
        static const int TIMEOUT = 0x04;
        static const int CANCELLED = 0x08;
        int index;
        int state;

    private:
        BlockReadStats * readStats;

    public:
        StripingChunkReadResult(int state_) {
            index = -1;
            state = state_;
            readStats = nullptr;

            Preconditions::checkArgument(state == TIMEOUT,
                "Only timeout result should return negative index.");
        }

        StripingChunkReadResult(int index, int state) {
            StripingChunkReadResult(index, state, nullptr);
        }

        StripingChunkReadResult(int index_, int state_, BlockReadStats * stats_) {
            index = index_;
            state = state_;
            readStats = stats_;

            Preconditions::checkArgument(state != TIMEOUT,
                "Timeout result should return negative index.");
        }

        BlockReadStats * getReadStats() const {
            return readStats;
        }

        void setIndex(int index_) {
            index = index_;
        }

        void setState(int state_) {
            state = state_;
        }

        void setReadStats(BlockReadStats * readStats_) {
            readStats = readStats_;
        }

        ostream & operator <<(ostream & outputStream) {
            outputStream << "(index=" << index << ", state =" << state
                << ", readStats =" << readStats << ")";
            return outputStream;
        }
    };

    static void checkBlocks(ExtendedBlock blockGroup, int i, ExtendedBlock blocki);
    static void constructInternalBlock(LocatedBlock & bg, int32_t idxInReturnedLocs, int32_t cellSize,
        int32_t dataBlkNum, int32_t idxInBlockGroup, LocatedBlock & lb);
    static void divideOneStripe(shared_ptr<ECPolicy> ecPolicy,
        int cellSize, LocatedBlock & blockGroup, long rangeStartInBlockGroup,
        long rangeEndInBlockGroup, ByteBuffer * buf, std::vector<AlignedStripe*> & stripes);

    static int64_t getInternalBlockLength(int64_t dataSize, int32_t cellSize,
        int32_t numDataBlocks, int32_t idxInBlockGroup);
    static void getNextCompletedStripedRead(
            std::map<int, std::future<StripedBlockUtil::BlockReadStats>> & futures,
            StripedBlockUtil::StripingChunkReadResult & r);

    static long offsetInBlkToOffsetInBG(int cellSize, int dataBlkNum,
        long offsetInBlk, int idxInBlockGroup);

    static void parseStripedBlockGroup(LocatedBlock & bg, int32_t cellSize,
        int32_t dataBlkNum, int32_t parityBlkNum, std::vector<LocatedBlock> & lbs);

private:
    /**
     * Cell indexing convention defined in StripingCell.
     */
    static void getRangesForInternalBlocks(shared_ptr<ECPolicy> ecPolicy, int cellSize,
        std::vector<StripingCell> & cells, std::vector<VerticalRange> & ranges);
    static void getStripingCellsOfByteRange(shared_ptr<ECPolicy> ecPolicy,
        int cellSize, LocatedBlock & blockGroup,
        long rangeStartInBlockGroup, long rangeEndInBlockGroup,
        std::vector<StripingCell> & cells);

    static int lastCellSize(int size, int cellSize, int numDataBlocks, int i) {
        if (i < numDataBlocks) {
            // parity block size (i.e. i >= numDataBlocks) is the same as
            // the first data block size (i.e. i = 0).
            size -= (i * cellSize);
            if (size < 0) {
                size = 0;
            }
        }
        return size > cellSize? cellSize: size;
    }

    static void mergeRangesForInternalBlocks(shared_ptr<ECPolicy> ecPolicy,
        std::vector<VerticalRange> & ranges, LocatedBlock & blockGroup,
        int cellSize, std::vector<AlignedStripe*> & stripes);

    static void prepareAllZeroChunks(LocatedBlock & blockGroup,
        std::vector<AlignedStripe*> & stripes, int cellSize, int dataBlkNum);

};

}
}

#endif /* _HDFS_LIBHDFS3_STRIPED_BLOCK_UTIL_H_ */
