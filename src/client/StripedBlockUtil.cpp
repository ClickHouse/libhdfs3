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
#include <algorithm>
#include "hdfs.h"
#include "StripedBlockUtil.h"
#include "ExceptionInternal.h"
#include "Exception.h"
#include "Preconditions.h"

using namespace Hdfs::Internal;

namespace Hdfs {
void StripedBlockUtil::checkBlocks(ExtendedBlock blockGroup, int i, ExtendedBlock blocki) {
    if (blocki.getPoolId() != blockGroup.getPoolId()) {
        string err = "Block pool IDs mismatched: block" + std::to_string(i) + "="
            + blocki.toString() + ", expected block group=" + blockGroup.toString();
	    throw err;
    }
    if (blocki.getBlockId() - i != blockGroup.getBlockId()) {
        throw "Block IDs mismatched: block" + std::to_string(i) + "="
            + blocki.toString() + ", expected block group=" + blockGroup.toString();
    }
    if (blocki.getGenerationStamp() != blockGroup.getGenerationStamp()) {
        throw "Generation stamps mismatched: block" + std::to_string(i) + "="
            + blocki.toString() + ", expected block group=" + blockGroup.toString();
    }
}

void StripedBlockUtil::constructInternalBlock(LocatedBlock & bg, int32_t idxInReturnedLocs,
    int32_t cellSize, int32_t dataBlkNum, int32_t idxInBlockGroup, LocatedBlock & lb) {
    lb.setBlockId(bg.getBlockId() + idxInBlockGroup);
    lb.setGenerationStamp(bg.getGenerationStamp());
    lb.setNumBytes(getInternalBlockLength(bg.getNumBytes(), cellSize, dataBlkNum, idxInBlockGroup));
    lb.setOffset(bg.getOffset());
    lb.setCorrupt(bg.isCorrupt());
    lb.setPoolId(bg.getPoolId());
    lb.setStriped(bg.isStriped());
    if (idxInReturnedLocs < static_cast<int32_t>(bg.getLocations().size())) {
        std::vector<DatanodeInfo> & nodes = lb.mutableLocations();
        nodes.push_back(bg.getLocations()[idxInReturnedLocs]);
        std::vector<std::string> & storageIDs = lb.mutableStorageIDs();
        storageIDs.push_back(bg.getStorageIDs()[idxInReturnedLocs]);
    } else {
        lb.setLocations(std::vector<DatanodeInfo>());
        lb.setStorageIDs(std::vector<std::string>());
    }
    std::vector<Token> & tokens = bg.mutableTokens();
    if (idxInReturnedLocs < static_cast<int32_t>(tokens.size())) {
        lb.setToken(tokens[idxInReturnedLocs]);
    }
}

void StripedBlockUtil::divideOneStripe(shared_ptr<ECPolicy> ecPolicy,
    int cellSize, LocatedBlock & blockGroup, long rangeStartInBlockGroup,
    long rangeEndInBlockGroup, ByteBuffer * buf, std::vector<AlignedStripe*> & stripes) {
    int dataBlkNum = ecPolicy->getNumDataUnits();
    // Step 1: map the byte range to StripingCells
    std::vector<StripingCell> cells;
    getStripingCellsOfByteRange(ecPolicy, cellSize, blockGroup,
        rangeStartInBlockGroup, rangeEndInBlockGroup, cells);

    // Step 2: get the unmerged ranges on each internal block
    std::vector<VerticalRange> ranges;
    getRangesForInternalBlocks(ecPolicy, cellSize, cells, ranges);

    // Step 3: merge into stripes
    mergeRangesForInternalBlocks(ecPolicy, ranges, blockGroup, cellSize, stripes);

    // Step 4: calculate each chunk's position in destination buffer. Since the
    // whole read range is within a single stripe, the logic is simpler here.
    int bufOffset =
        static_cast<int>(rangeStartInBlockGroup % (static_cast<int>(cellSize * dataBlkNum)));
    for (int i = 0; i < static_cast<int>(cells.size()); i++) {
      StripingCell & cell = cells[i];
      long cellStart = cell.idxInInternalBlk * cellSize + cell.offset;
      long cellEnd = cellStart + cell.size - 1;
      StripingChunk * chunk;
      for (int j = 0; j < static_cast<int>(stripes.size()); j++) {
        AlignedStripe * s = stripes[j];
        long stripeEnd = s->getOffsetInBlock() + s->getSpanInBlock() - 1;
        long overlapStart = std::max(cellStart, s->getOffsetInBlock());
        long overlapEnd = std::min(cellEnd, stripeEnd);
        int overLapLen = static_cast<int>(overlapEnd - overlapStart + 1);
        if (overLapLen > 0) {
            chunk = s->chunks[cell.idxInStripe];
            if (chunk == nullptr) {
                chunk = new StripingChunk();
                s->chunks[cell.idxInStripe] = chunk;
            }
            int pos = static_cast<int>(bufOffset + overlapStart - cellStart);
            chunk->getChunkBuffer()->addSlice(buf, pos, overLapLen);
        }
      }
      bufOffset += cell.size;
    }

    // Step 5: prepare ALLZERO blocks
    prepareAllZeroChunks(blockGroup, stripes, cellSize, dataBlkNum);
}

int64_t StripedBlockUtil::getInternalBlockLength(int64_t dataSize, int32_t cellSize, 
    int32_t numDataBlocks, int32_t idxInBlockGroup) {
    if (dataSize < 0 || cellSize <= 0 || numDataBlocks <= 0 || idxInBlockGroup < 0) {
        THROW(InvalidParameter, "invalid parameter.");
    }
    // Size of each stripe (only counting data blocks)
    int32_t stripeSize = cellSize * numDataBlocks;
    // If block group ends at stripe boundary, each internal block has an equal
    // share of the group
    int32_t lastStripeDataLen = static_cast<int32_t>(dataSize % stripeSize);
    if (lastStripeDataLen == 0) {
        return dataSize / numDataBlocks;
    }

    int32_t numStripes = static_cast<int32_t>((dataSize - 1) / stripeSize + 1);
    return (numStripes - 1) * cellSize
        + lastCellSize(lastStripeDataLen, cellSize, numDataBlocks, idxInBlockGroup);
}

void StripedBlockUtil::getNextCompletedStripedRead(
        std::map<int, std::future<StripedBlockUtil::BlockReadStats>> & futures,
        StripedBlockUtil::StripingChunkReadResult & r) {
    Preconditions::checkArgument(!futures.empty());
    std::future<BlockReadStats> & future = futures.begin()->second;
    int chunkIndex = futures.begin()->first;
    try {
        BlockReadStats stats = future.get();
        futures.erase(chunkIndex);
        r.setIndex(chunkIndex);
        r.setState(StripingChunkReadResult::SUCCESSFUL);
        r.setReadStats(&stats);
    } catch (...) {
        std::string buffer;
        LOG(DEBUG1, "Exception during striped read task.\n%s",
            GetExceptionDetail(current_exception(), buffer));
        futures.erase(chunkIndex);
        r.setIndex(chunkIndex);
        r.setState(StripingChunkReadResult::FAILED);
    }
}

long StripedBlockUtil::offsetInBlkToOffsetInBG(int cellSize, int dataBlkNum,
    long offsetInBlk, int idxInBlockGroup) {
    int64_t cellIdxInBlk = offsetInBlk / cellSize;
    return cellIdxInBlk * cellSize * dataBlkNum // n full stripes before offset
        + static_cast<int64_t>(idxInBlockGroup) * cellSize // m full cells before offset
        + offsetInBlk % cellSize; // partial cell
}

void StripedBlockUtil::parseStripedBlockGroup(LocatedBlock & bg, int32_t cellSize, 
    int32_t dataBlkNum, int32_t parityBlkNum, std::vector<LocatedBlock> & lbs) {
    assert(bg.isStriped());
    int32_t locatedBGSize = bg.getIndices().size();
    lbs.resize(dataBlkNum + parityBlkNum);
    for (int8_t i = 0; i < locatedBGSize; ++i) {
        int32_t idx = bg.getIndices()[i];
        if (idx < (dataBlkNum + parityBlkNum) && lbs[idx].getBlockId() == 0) {
            constructInternalBlock(bg, i, cellSize, dataBlkNum, idx, lbs[idx]);
        }
    }
}

void StripedBlockUtil::getRangesForInternalBlocks(shared_ptr<ECPolicy> ecPolicy, int cellSize,
    std::vector<StripingCell> & cells, std::vector<VerticalRange> & ranges) {
    int dataBlkNum = ecPolicy->getNumDataUnits();
    int parityBlkNum = ecPolicy->getNumParityUnits();
    ranges.resize(dataBlkNum + parityBlkNum);

    long earliestStart = LLONG_MAX;
    long latestEnd = -1;
    for (int i = 0; i < static_cast<int>(cells.size()); i++) {
        StripingCell & cell = cells[i];
        // iterate through all cells and update the list of StripeRanges
        if (ranges[cell.idxInStripe].spanInBlock == 0) {
            ranges[cell.idxInStripe].offsetInBlock = 
                cell.idxInInternalBlk * cellSize + cell.offset;
            ranges[cell.idxInStripe].spanInBlock = cell.size;
        } else {
            ranges[cell.idxInStripe].spanInBlock += cell.size;
        }
        VerticalRange & range = ranges[cell.idxInStripe];
        if (range.offsetInBlock < earliestStart) {
            earliestStart = range.offsetInBlock;
        }
        if (range.offsetInBlock + range.spanInBlock - 1 > latestEnd) {
            latestEnd = range.offsetInBlock + range.spanInBlock - 1;
        }
    }

    // Each parity block should be fetched at maximum range of all data blocks
    for (int i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
        ranges[i].offsetInBlock = earliestStart;
        ranges[i].spanInBlock = latestEnd - earliestStart + 1;
    }
}

void StripedBlockUtil::getStripingCellsOfByteRange(shared_ptr<ECPolicy> ecPolicy,
    int cellSize, LocatedBlock & blockGroup, long rangeStartInBlockGroup, 
    long rangeEndInBlockGroup, std::vector<StripingCell> & cells) {
    
    std::stringstream ss;
    ss << "start=" + std::to_string(rangeStartInBlockGroup)
       << " end=" + std::to_string(rangeEndInBlockGroup)
       << " blockSize=" + std::to_string(blockGroup.getNumBytes());
        
    Preconditions::checkArgument(
        rangeStartInBlockGroup <= rangeEndInBlockGroup &&
            rangeEndInBlockGroup < blockGroup.getNumBytes(), ss.str());
    long len = rangeEndInBlockGroup - rangeStartInBlockGroup + 1;
    int firstCellIdxInBG = static_cast<int>(rangeStartInBlockGroup / cellSize);
    int lastCellIdxInBG = static_cast<int>(rangeEndInBlockGroup / cellSize);
    int numCells = lastCellIdxInBG - firstCellIdxInBG + 1;
    cells.resize(numCells);

    int firstCellOffset = static_cast<int>(rangeStartInBlockGroup % cellSize);
    int firstCellSize =
        static_cast<int>(std::min(cellSize - (rangeStartInBlockGroup % cellSize), len));

    cells[0].init(ecPolicy, firstCellSize, firstCellIdxInBG, firstCellOffset);
    if (lastCellIdxInBG != firstCellIdxInBG) {
        int lastCellSize = static_cast<int>(rangeEndInBlockGroup % cellSize) + 1;
        cells[numCells - 1].init(ecPolicy, lastCellSize,
            lastCellIdxInBG, 0);
    }

    for (int i = 1; i < numCells - 1; i++) {
        cells[i].init(ecPolicy, cellSize, i + firstCellIdxInBG, 0);
    }
}

void StripedBlockUtil::mergeRangesForInternalBlocks(shared_ptr<ECPolicy> ecPolicy, 
    std::vector<VerticalRange> & ranges, LocatedBlock & blockGroup, 
    int cellSize, std::vector<AlignedStripe*> & stripes) {
    int dataBlkNum = ecPolicy->getNumDataUnits();
    int parityBlkNum = ecPolicy->getNumParityUnits();
    std::set<long> stripePoints;
    for (int i = 0; i < static_cast<int>(ranges.size()); i++) {
        VerticalRange & r = ranges[i];
        if (r.spanInBlock != 0) {
            stripePoints.insert(r.offsetInBlock);
            stripePoints.insert(r.offsetInBlock + r.spanInBlock);
        }
    }

    // Add block group last cell offset in stripePoints if it is fall in to read
    // offset range.
    int lastCellIdxInBG = static_cast<int>(blockGroup.getNumBytes() / cellSize);
    int idxInInternalBlk = lastCellIdxInBG / ecPolicy->getNumDataUnits();
    long lastCellEndOffset = (idxInInternalBlk * (long)cellSize)
        + (blockGroup.getNumBytes() % cellSize);
    if (*stripePoints.begin() < lastCellEndOffset
        && *stripePoints.rbegin() > lastCellEndOffset) {
        stripePoints.insert(lastCellEndOffset);
    }

    long prev = -1;
    std::set<long>::iterator it;
    for (it = stripePoints.begin(); it != stripePoints.end(); ++it) {
        long point = *it;
        if (prev >= 0) {
            stripes.push_back(new AlignedStripe(prev, point - prev,
                dataBlkNum + parityBlkNum));
        }
        prev = point;
    }
}

void StripedBlockUtil::prepareAllZeroChunks(LocatedBlock & blockGroup,
    std::vector<AlignedStripe*> & stripes, int cellSize, int dataBlkNum) {
    for (int i = 0; i < static_cast<int>(stripes.size()); i++) {
        AlignedStripe & s = *stripes[i];
        for (int i = 0; i < dataBlkNum; i++) {
            long internalBlkLen = getInternalBlockLength(blockGroup.getNumBytes(),
                cellSize, dataBlkNum, i);
            if (internalBlkLen <= s.getOffsetInBlock()) {
                Preconditions::checkState(s.chunks[i] == nullptr);
                s.chunks[i] = new StripingChunk(StripingChunk::ALLZERO);
            }
        }
    }
}

}
