/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
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
#include "Exception.h"
#include "ExceptionInternal.h"
#include "LocatedBlock.h"
#include "LocatedBlocks.h"

#include <algorithm>
#include <cassert>
#include <iostream>

namespace Hdfs {
namespace Internal {

const LocatedBlock * LocatedBlocksImpl::findBlock(int64_t position) {
    int32_t targetBlockIdx = 0;
    return findBlock(position, targetBlockIdx);
}

const LocatedBlock * LocatedBlocksImpl::findBlock(int64_t position, int32_t & targetBlockIdx) {
    if (position < fileLength) {
        LocatedBlock target(position);
        std::vector<LocatedBlock>::iterator bound;

        if (blocks.empty() || position < blocks.begin()->getOffset()) {
            targetBlockIdx = 0;
            return NULL;
        }

        /*
         * bound is first block which start offset is larger than
         * or equal to position
         */
        bound = std::lower_bound(blocks.begin(), blocks.end(), target,
                                 std::less<LocatedBlock>());
        assert(bound == blocks.end() || bound->getOffset() >= position);
        LocatedBlock * retval = NULL;

        targetBlockIdx = (int32_t)(bound - blocks.begin());
        if (bound == blocks.end()) {
            retval = &blocks.back();
        } else if (bound->getOffset() > position) {
            assert(bound != blocks.begin());
            --bound;
            retval = &(*bound);
        } else {
            retval = &(*bound);
        }

        if (position < retval->getOffset()
                || position >= retval->getOffset() + retval->getNumBytes()) {
            return NULL;
        }

        return retval;
    } else {
        targetBlockIdx = (int32_t)blocks.size();
        return lastBlock.get();
    }
}

void LocatedBlocksImpl::insertRange(int32_t blockIdx, std::vector<LocatedBlock> & newBlocks) {
    int32_t oldIdx = blockIdx;
    int32_t insStart = 0, insEnd = 0;
    for(int32_t newIdx = 0; newIdx < newBlocks.size() && oldIdx < blocks.size(); newIdx++) {
        int64_t newOff = newBlocks[newIdx].getOffset();
        int64_t oldOff = blocks[oldIdx].getOffset();
        if(newOff < oldOff) {
            insEnd++;
        } else if(newOff == oldOff) {
            // replace old cached block by the new one
            blocks[oldIdx] = newBlocks[newIdx];
            if(insStart < insEnd) { // insert new blocks
                addAll(blocks, oldIdx, newBlocks, insStart, insEnd);
                oldIdx += insEnd - insStart;
            }
            insStart = insEnd = newIdx+1;
            oldIdx++;
        } else {  // newOff > oldOff
            assert(false);
        }
    }
    insEnd = (int32_t)newBlocks.size();
    if(insStart < insEnd) { // insert new blocks
        addAll(blocks, oldIdx, newBlocks, insStart, insEnd);
    }
}

void LocatedBlocksImpl::addAll(std::vector<LocatedBlock> & oldBlocks, int32_t index, std::vector<LocatedBlock> & newBlocks,
            int32_t start, int32_t end) {
    int32_t oldSize = (int32_t)oldBlocks.size();
    int32_t shiftSize = end - start;
    oldBlocks.resize(oldSize + shiftSize);
    for (int32_t i = oldSize - 1; i >= index; --i) {
        oldBlocks[i + shiftSize] = oldBlocks[i];
    }
    for (int32_t i = 0; i < shiftSize; ++i) {
        oldBlocks[index + i] = newBlocks[i];
    }
}

}
}
