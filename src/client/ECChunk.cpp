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

#include "ECChunk.h"

namespace Hdfs {
namespace Internal {

ECChunk::ECChunk(shared_ptr<ByteBuffer> buffer) : allZero(false) {
    chunkBuffer = buffer;
}

ECChunk::ECChunk(const shared_ptr<ByteBuffer> & buffer, int offset, int len) : allZero(false) {
    shared_ptr<ByteBuffer> tmp = shared_ptr<ByteBuffer>(buffer->duplicate());
    tmp->position(offset);
    tmp->limit(offset + len);
    chunkBuffer = shared_ptr<ByteBuffer>(tmp->slice());
}

shared_ptr<ByteBuffer> ECChunk::getBuffer() const {
    return chunkBuffer;
}

bool ECChunk::isAllZero() const {
    return allZero;
}

void ECChunk::setAllZero(bool _allZero) {
    allZero = _allZero;
}

/**
 * Convert to a bytes array, just for test usage.
 * @return bytes array
 */
std::vector<int8_t> ECChunk::toBytesArray() const {
    std::vector<int8_t> bytesArr(chunkBuffer->remaining());
    // Avoid affecting the original one
    chunkBuffer->mark();
    chunkBuffer->getBytes(&bytesArr[0], bytesArr.size());
    chunkBuffer->reset();

    return bytesArr;
}

}
}
