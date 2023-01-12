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

#include "gtest/gtest.h"

#include "client/RawErasureEncoder.h"
#include "client/RawErasureDecoder.h"

using namespace Hdfs;
using namespace Hdfs::Internal;
using namespace testing;

class TestRawErasureEncodeDecode: public ::testing::Test {
public:
    int chunkSize = 1024;
    int numDataUnits;
    int numParityUnits;
    bool startBufferWithZero = true;
    std::vector<int> erasedDataIndexes;
    std::vector<int> erasedParityIndexes;
public:
    TestRawErasureEncodeDecode() {
    }
    ~TestRawErasureEncodeDecode() {
    }

    void prepare(int numDataUnits, int numParityUnits, std::vector<int> & erasedDataIndexes, std::vector<int> & erasedParityIndexes) {
        srand(time(0));
        this->numDataUnits = numDataUnits;
        this->numParityUnits = numParityUnits;
        this->erasedDataIndexes = erasedDataIndexes;
        this->erasedParityIndexes = erasedParityIndexes;
    }

    int getRand(int min, int max) {
        return (rand() % (max - min + 1)) + min;
    }

    int8_t nextBytes() {
        return (int8_t) getRand(0, (1<<8) - 1);
    }

    /**
     * Fill len of dummy data in the buffer at the current position.
     */
    void fillDummyData(std::shared_ptr<ByteBuffer> & buffer, int len) {
        for (int i = 0; i < len; ++i) {
            buffer->put(nextBytes());
        }
    }

    /**
     * Allocate a buffer for output or writing. It can prepare for two kinds of
     * data buffers: one with position as 0, the other with position > 0
     */
    void allocateOutputBuffer(std::shared_ptr<ByteBuffer> & buffer, int bufferLen) {
        /**
         * When startBufferWithZero, will prepare a buffer as:---------------
         * otherwise, the buffer will be like:             ___TO--BE--WRITTEN___,
         * and in the beginning, dummy data are prefixed, to simulate a buffer of
         * position > 0.
         */
        int startOffset = startBufferWithZero ? 0 : 11; // 11 is arbitrary
        int allocLen = startOffset + bufferLen + startOffset;
        buffer = std::shared_ptr<ByteBuffer>(new ByteBuffer(allocLen));
        buffer->limit(startOffset + bufferLen);
        fillDummyData(buffer, startOffset);
        startBufferWithZero = ! startBufferWithZero;
    }

    /**
     * Generate data chunk by making random data.
     */
    void generateDataChunk(std::shared_ptr<ECChunk> & chunk) {
        std::shared_ptr<ByteBuffer> buffer;
        allocateOutputBuffer(buffer, chunkSize);
        int pos = buffer->position();
        for (int i = 0; i < chunkSize; ++i) {
            buffer->put(nextBytes());
        }
        buffer->flip();
        buffer->position(pos);

        chunk = std::shared_ptr<ECChunk>(new ECChunk(buffer));
    }

    void generateDataChunks(std::vector<std::shared_ptr<ECChunk>> & chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            generateDataChunk(chunks[i]);
        }
    }

    /**
     * Prepare data chunks for each data unit, by generating random data.
     */
    void prepareDataChunksForEncoding(std::vector<std::shared_ptr<ECChunk>> & chunks) {
        generateDataChunks(chunks);
    }

    /**
     * Allocate a chunk for output or writing.
     */
    void allocateOutputChunk(std::shared_ptr<ECChunk> & chunk) {
        std::shared_ptr<ByteBuffer> buffer;
        allocateOutputBuffer(buffer, chunkSize);

	    chunk = std::shared_ptr<ECChunk>(new ECChunk(buffer));
    }

    /**
     * Prepare parity chunks for encoding, each chunk for each parity unit.
     */
    void prepareParityChunksForEncoding(std::vector<std::shared_ptr<ECChunk>> & chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            allocateOutputChunk(chunks[i]);
        }
    }

    /**
     * Clone chunk along with copying the associated data. It respects how the
     * chunk buffer is allocated, direct or non-direct. It avoids affecting the
     * original chunk.
     */
    void cloneChunkWithData(std::shared_ptr<ECChunk> & chunk, std::shared_ptr<ECChunk> & result) {
        if (!chunk) {
            return;
        }

        std::shared_ptr<ByteBuffer> srcBuffer = chunk->getBuffer();

	    uint32_t length = srcBuffer->remaining();
        int8_t bytesArr[length];
        memset(bytesArr, 0, length);
        srcBuffer->mark();
        srcBuffer->getBytes(bytesArr, length);
        srcBuffer->reset();

        std::shared_ptr<ByteBuffer> destBuffer;
        allocateOutputBuffer(destBuffer, length);
        int pos = destBuffer->position();
        destBuffer->putBytes(bytesArr, length);
        destBuffer->flip();
        destBuffer->position(pos);

        result = std::shared_ptr<ECChunk>(new ECChunk(destBuffer));
    }

    /**
     * Clone chunks along with copying the associated data. It respects how the
     * chunk buffer is allocated, direct or non-direct. It avoids affecting the
     * original chunk buffers.
     */
    void cloneChunksWithData(std::vector<std::shared_ptr<ECChunk>> & chunks,
                             std::vector<std::shared_ptr<ECChunk>> & results) {
        for (int i = 0; i < chunks.size(); i++) {
            cloneChunkWithData(chunks[i], results[i]);
        }
    }

    /**
     * Erase some data chunks to test the recovering of them. As they're erased,
     * we don't need to read them and will not have the buffers at all, so just
     * set them as null.
     */
    void backupAndEraseChunks(std::vector<std::shared_ptr<ECChunk>> & dataChunks,
                              std::vector<std::shared_ptr<ECChunk>> & parityChunks,
                              std::vector<std::shared_ptr<ECChunk>> & toEraseChunks) {
        int idx = 0;
        for (int i = 0; i < erasedDataIndexes.size(); i++) {
            toEraseChunks[idx ++] = dataChunks[erasedDataIndexes[i]];
            dataChunks[erasedDataIndexes[i]] = nullptr;
        }

        for (int i = 0; i < erasedParityIndexes.size(); i++) {
            toEraseChunks[idx ++] = parityChunks[erasedParityIndexes[i]];
            parityChunks[erasedParityIndexes[i]] = nullptr;
        }
    }

    /**
     * Return input chunks for decoding, which is dataChunks + parityChunks.
     */
    void prepareInputChunksForDecoding(std::vector<std::shared_ptr<ECChunk>> & dataChunks,
                                       std::vector<std::shared_ptr<ECChunk>> & parityChunks,
                                       std::vector<std::shared_ptr<ECChunk>> & inputChunks) {
        int idx = 0;
        for (int i = 0; i < numDataUnits; i++) {
            inputChunks[idx ++] = dataChunks[i];
        }

        for (int i = 0; i < numParityUnits; i++) {
            inputChunks[idx ++] = parityChunks[i];
        }
    }

    void ensureOnlyLeastRequiredChunks(std::vector<std::shared_ptr<ECChunk>> & inputChunks) {
        int leastRequiredNum = numDataUnits;
        int erasedNum = erasedDataIndexes.size() + erasedParityIndexes.size();
        int goodNum = inputChunks.size() - erasedNum;
        int redundantNum = goodNum - leastRequiredNum;

        for (int i = 0; i < inputChunks.size() && redundantNum > 0; i++) {
            if (inputChunks[i]) {
                inputChunks[i] = nullptr; // Setting it null, not needing it actually
                redundantNum--;
            }
        }
    }

    /**
     * Prepare output chunks for decoding, each output chunk for each erased
     * chunk.
     */
    void prepareOutputChunksForDecoding(std::vector<std::shared_ptr<ECChunk>> & chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            allocateOutputChunk(chunks[i]);
        }
    }

    /**
     * Adjust and return erased indexes altogether, including erased data indexes
     * and parity indexes.
     */
    void getErasedIndexesForDecoding(std::vector<int> & erasedIndexesForDecoding) {
        int idx = 0;

        for (int i = 0; i < erasedDataIndexes.size(); i++) {
            erasedIndexesForDecoding[idx ++] = erasedDataIndexes[i];
        }

        for (int i = 0; i < erasedParityIndexes.size(); i++) {
            erasedIndexesForDecoding[idx ++] = erasedParityIndexes[i] + numDataUnits;
        }
    }

    /**
     * Convert an array of this chunks to an array of byte array.
     * Note the chunk buffers are not affected.
     */
    void toArrays(std::vector<std::shared_ptr<ECChunk>> & chunks, std::vector< std::vector<int8_t> > & bytesArr) {
        for (int i = 0; i < chunks.size(); i++) {
            if (chunks[i]) {
                bytesArr[i] = chunks[i]->toBytesArray();
            }
        }
    }

    bool deepEquals(std::vector<int8_t> & a1,
                    std::vector<int8_t> & a2) {
        int length = a1.size();
        if (a2.size() != length)
            return false;

        for (int i = 0; i < length; i++) {
            if (a1[i] != a2[i])
                return false;
        }
        return true;
    }

    bool deepEquals(std::vector< std::vector<int8_t> > & a1,
                    std::vector< std::vector<int8_t> > & a2) {
        int length = a1.size();
        if (a2.size() != length)
            return false;

        for (int i = 0; i < length; i++) {
            std::vector<int8_t> & e1 = a1[i];
            std::vector<int8_t> & e2 = a2[i];

            // Figure out whether the two elements are equal
            bool eq = deepEquals(e1, e2);

            if (!eq)
                return false;
        }
        return true;
    }

    /**
     * Compare and verify if erased chunks are equal to recovered chunks
     */
    void compareAndVerify(std::vector<std::shared_ptr<ECChunk>> & erasedChunks,
                          std::vector<std::shared_ptr<ECChunk>> & recoveredChunks) {
        std::vector< std::vector<int8_t> > erased(erasedChunks.size());
        toArrays(erasedChunks, erased);
        std::vector< std::vector<int8_t> > recovered(recoveredChunks.size());
        toArrays(recoveredChunks, recovered);
        bool result = deepEquals(erased, recovered);
        EXPECT_EQ(result, true);
    }
};

TEST_F(TestRawErasureEncodeDecode, TestEncodeDecode) {
    std::vector<int> erasedDataIndexesInUT, erasedParityIndexesInUT;
    erasedDataIndexesInUT.push_back(1);
    prepare(6, 3, erasedDataIndexesInUT, erasedParityIndexesInUT);

    ErasureCoderOptions coderOptions(numDataUnits, numParityUnits);
    RawErasureEncoder encoder(coderOptions);
    RawErasureDecoder decoder(coderOptions);
    
    // Generate data and encode
    std::vector<std::shared_ptr<ECChunk>> dataChunks(numDataUnits);
    prepareDataChunksForEncoding(dataChunks);

    std::vector<std::shared_ptr<ECChunk>> parityChunks(numParityUnits);
    prepareParityChunksForEncoding(parityChunks);

    // Backup all the source chunks for later recovering because some coders
    // may affect the source data.
    std::vector<std::shared_ptr<ECChunk>> clonedDataChunks(dataChunks.size());
    cloneChunksWithData(dataChunks, clonedDataChunks);

    encoder.encode(dataChunks, parityChunks);

    // Backup and erase some chunks
    std::vector<std::shared_ptr<ECChunk>> backupChunks(erasedDataIndexes.size() + erasedParityIndexes.size());
    backupAndEraseChunks(clonedDataChunks, parityChunks, backupChunks);

    // Decode
    std::vector<std::shared_ptr<ECChunk>> inputChunks(numDataUnits + numParityUnits);
    prepareInputChunksForDecoding(clonedDataChunks, parityChunks, inputChunks);

    // Remove unnecessary chunks, allowing only least required chunks to be read.
    ensureOnlyLeastRequiredChunks(inputChunks);

    std::vector<std::shared_ptr<ECChunk>> recoveredChunks(erasedDataIndexes.size() + erasedParityIndexes.size());
    prepareOutputChunksForDecoding(recoveredChunks);

    std::vector<int> erasedIndexesForDecoding(erasedDataIndexes.size() + erasedParityIndexes.size());
    getErasedIndexesForDecoding(erasedIndexesForDecoding);
    decoder.decode(inputChunks, erasedIndexesForDecoding, recoveredChunks);

    // Compare
    compareAndVerify(backupChunks, recoveredChunks);
}