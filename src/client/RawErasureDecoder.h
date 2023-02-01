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

#ifndef _HDFS_LIBHDFS3_CLIENT_RAW_ERASURE_DECODER_H_
#define _HDFS_LIBHDFS3_CLIENT_RAW_ERASURE_DECODER_H_

#include "ByteBuffer.h"
#include "ErasureCoderOptions.h"
#include "ByteBufferDecodingState.h"
#include "ECChunk.h"

#include <vector>
#include <deque>
#include <memory>

namespace Hdfs {
namespace Internal {

class RawErasureDecoder {
public:
    RawErasureDecoder();
    explicit RawErasureDecoder(ErasureCoderOptions & coderOptions);
    ~RawErasureDecoder() = default;

    void doDecode(const shared_ptr<ByteBufferDecodingState> & decodingState);

    void decode(std::vector<shared_ptr<ECChunk>> & inputs,
                std::vector<int> & erasedIndexes,
                std::vector<shared_ptr<ECChunk>> & outputs);

    void decode(std::vector<shared_ptr<ByteBuffer>> & inputs,
                std::vector<int> & erasedIndexes,
                std::vector<shared_ptr<ByteBuffer>> & outputs);

    void prepareDecoding(const std::vector<shared_ptr<ByteBuffer>> & inputs, const vector<int> & erasedIndexes);

    void processErasures(const std::vector<int> & erasedIndexes);
    
    void generateDecodeMatrix(const std::vector<int> & erasedIndexes);

    int getNumDataUnits() const;
    int getNumAllUnits() const;

private:

    // relevant to schema and won't change during decode calls
    std::vector<int8_t> encodeMatrix;

    /**
     * Below are relevant to schema and erased indexes, thus may change during
     * decode calls.
     */
    std::vector<int8_t> decodeMatrix;
    std::vector<int8_t> invertMatrix;

    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    std::vector<int8_t> gfTables;
    std::vector<int> cachedErasedIndexes;
    std::vector<int> validIndexes;
    int numErasedDataUnits;
    std::deque<bool> erasureFlags;

    ErasureCoderOptions coderOptions;
};
    
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_RAW_ERASURE_DECODER_H_ */
