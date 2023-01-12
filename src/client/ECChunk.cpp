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

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

/**
 * Wrapping a bytes array
 * @param buffer buffer to be wrapped by the chunk
*/
ECChunk::ECChunk(std::vector<int8_t> buffer) {
    this->chunkBuffer = std::shared_ptr<ByteBuffer>(new ByteBuffer(&buffer[0], buffer.size()));
}


ECChunk::ECChunk(std::vector<int8_t> buffer, int offset, int len) {
    this->chunkBuffer = std::shared_ptr<ByteBuffer>(new ByteBuffer(&buffer[0], buffer.size()));
    this->chunkBuffer->position(offset);
    this->chunkBuffer->limit(offset + len);
}

ECChunk::ECChunk(std::shared_ptr<ByteBuffer> buffer) {
    this->chunkBuffer = buffer;
}


ECChunk::ECChunk(std::shared_ptr<ByteBuffer> buffer, int offset, int len) {
    std::shared_ptr<ByteBuffer> tmp = std::shared_ptr<ByteBuffer>(buffer->duplicate());
    tmp->position(offset);
    tmp->limit(offset + len);
    this->chunkBuffer = std::shared_ptr<ByteBuffer>(tmp->slice());
}

ECChunk::~ECChunk() {

}

std::shared_ptr<ByteBuffer> ECChunk::getBuffer() {
    return chunkBuffer;
}


bool ECChunk::isAllZero() {
    return allZero;
}

void ECChunk::setAllZero(bool allZero) {
    this->allZero = allZero;
}

/**
 * Convert to a bytes array, just for test usage.
 * @return bytes array
*/
std::vector<int8_t> ECChunk::toBytesArray() {
    std::vector<int8_t> bytesArr(chunkBuffer->remaining());
    // Avoid affecting the original one
    chunkBuffer->mark();
    chunkBuffer->getBytes(&bytesArr[0], bytesArr.size());
    chunkBuffer->reset();

    return bytesArr;
}

}
}