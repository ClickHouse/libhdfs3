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

#ifndef LIBHDFS3_NATIVERSRAWDECODER_H
#define LIBHDFS3_NATIVERSRAWDECODER_H

#include "erasure_coder.h"
#include "AbstractNativeRawDecoder.h"

namespace Hdfs {
namespace Internal {

typedef struct _RSDecoder {
    IsalDecoder decoder;
    unsigned char* inputs[MMAX];
    unsigned char* outputs[MMAX];
} RSDecoder;

class NativeRSRawDecoder : public AbstractNativeRawDecoder {
public:
    NativeRSRawDecoder(ErasureCoderOptions & coderOptions);
    void release() override;
protected:
    void performDecodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs,
                           int inputOffsets[], int dataLen,
                           std::vector<int> & erased, std::vector<shared_ptr<ByteBuffer>> & outputs,
                           int outputOffsets[]) override;
private:
    void initImpl(int numDataUnits, int numParityUnits);
    void decodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs,
                    int inputOffsets[], int dataLen,
                    std::vector<int> & erased, std::vector<shared_ptr<ByteBuffer>> & outputs,
                    int outputOffsets[]);
    void destroyImpl();
};

}
}

#endif //LIBHDFS3_NATIVERSRAWDECODER_H
