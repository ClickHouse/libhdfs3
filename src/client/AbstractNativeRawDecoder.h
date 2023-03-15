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

#ifndef LIBHDFS3_ABSTRACTNATIVERAWDECODER_H
#define LIBHDFS3_ABSTRACTNATIVERAWDECODER_H

#include <shared_mutex>

#include "RawErasureDecoder.h"

namespace Hdfs {
namespace Internal {

class AbstractNativeRawDecoder : public RawErasureDecoder {
public:
    AbstractNativeRawDecoder(ErasureCoderOptions & coderOptions);

    void doDecode(const shared_ptr<ByteBufferDecodingState> & decodingState) override;

    void release() override;
protected:
    virtual void performDecodeImpl(std::vector<shared_ptr<ByteBuffer>> & inputs,
                                   int inputOffsets[], int dataLen,
                                   std::vector<int> & erased, std::vector<shared_ptr<ByteBuffer>> & outputs,
                                   int outputOffsets[]) = 0;
protected:
    mutable std::shared_mutex mutex;
    void * nativeCoder;
};

}
}

#endif //LIBHDFS3_ABSTRACTNATIVERAWDECODER_H
