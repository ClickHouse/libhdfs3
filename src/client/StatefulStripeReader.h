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
#ifndef _HDFS_LIBHDFS3_CLIENT_STATEFULSTRIPEDREADER_H_
#define _HDFS_LIBHDFS3_CLIENT_STATEFULSTRIPEDREADER_H_

#include "StripeReader.h"

namespace Hdfs {
namespace Internal {

class StatefulStripeReader : public StripeReader {
public:
    StatefulStripeReader(StripedBlockUtil::AlignedStripe & alignedStripe,
                         shared_ptr<ECPolicy> ecPolicy,
                         std::vector<LocatedBlock> & targetBlocks,
                         std::vector<StripeReader::BlockReaderInfo *> & readerInfos,
                         shared_ptr<CorruptedBlocks> corruptedBlocks,
                         shared_ptr<RawErasureDecoder> decoder,
                         StripedInputStreamImpl * dfsStripedInputStream,
                         shared_ptr<SessionConfig> conf);

    ~StatefulStripeReader();

public:
    /**
     * Prepare all the data chunks.
     */
    void prepareDecodeInputs() override;

    /**
     * Prepare the parity chunk and block reader if necessary.
     */
    bool prepareParityChunk(int index) override;

    /**
     * Decode to get the missing data.
     */
    void decode() override;

    /**
     * Default close do nothing.
     */
    void close() override;

private:
    std::mutex mtx;
};

}
}
#endif /* _HDFS_LIBHDFS3_CLIENT_STATEFULSTRIPEDREADER_H_ */
