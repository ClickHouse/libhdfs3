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

#include "CoderUtil.h"

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {  

std::vector<int8_t> CoderUtil::emptyChunk(4096);

std::vector<int> CoderUtil::copyOf(std::vector<int> & original, int newLength) {
    if (newLength < 0) {
        throw "copyOf function's newLength should not be a negative number";
    }
    int orginal_length = original.size();
    int iter = std::min(newLength, orginal_length);
    
    std::vector<int> copy(newLength, 0);
    for (int i = 0; i < iter; i++) {
        copy[i] = original[i];
    }
    return copy;
}

std::vector<int8_t> CoderUtil::getEmptyChunk(int leastLength) {
    if ((int)emptyChunk.size() >= leastLength) {
        return emptyChunk; // In most time
    }

    std::vector<int8_t> newvec(leastLength);
    emptyChunk = newvec;

    return emptyChunk;
}

std::vector<int> CoderUtil::getValidIndexes(std::vector<std::shared_ptr<ByteBuffer>> & inputs) {
    std::vector<int> validIndexes(inputs.size());
    int idx = 0;
    for (int i = 0; i < (int)inputs.size(); i++) {
        if (inputs[i] != nullptr) {
            validIndexes[idx++] = i;
        }
    }

    return copyOf(validIndexes, idx);
}

/**
 * Find the valid input from all the inputs.
 * @param inputs input buffers to look for valid input
 * @return the first valid input
 */
std::shared_ptr<ByteBuffer> CoderUtil::findFirstValidInput(std::vector<std::shared_ptr<ByteBuffer>> & inputs) {
    for (std::shared_ptr<ByteBuffer> input : inputs) {
        if (input != nullptr) {
            return input;
        }
    }
    throw "Invalid inputs are found, all being null";
}

void CoderUtil::resetBuffer(std::shared_ptr<ByteBuffer> buffer, int len) {
    int pos = buffer->position();
    memset(buffer.get()->getBuffer() + pos, 0, len);
    buffer->position(pos);
}

void CoderUtil::resetOutputBuffers(std::vector<std::shared_ptr<ByteBuffer>> & buffers, int dataLen) {
    for (std::shared_ptr<ByteBuffer> tmp : buffers) {
        resetBuffer(tmp, dataLen);
    }
}

std::vector<std::shared_ptr<ByteBuffer>> CoderUtil::toBuffers(std::vector<std::shared_ptr<ECChunk>> & chunks) {
  
    std::vector<std::shared_ptr<ByteBuffer>> buffers(chunks.size());
    std::shared_ptr<ECChunk> chunk;
    
    for (int i = 0; i < (int)chunks.size(); i++) {
        chunk = chunks[i];
      
        if (!chunk) {
            buffers[i] = nullptr;
        } else {
            buffers[i] = chunk->getBuffer();
            if (chunk->isAllZero()) {
                CoderUtil::resetBuffer(buffers[i], buffers[i]->remaining());
            }
        }
    }
    return buffers;
}

}
}