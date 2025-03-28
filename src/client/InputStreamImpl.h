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
#ifndef _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_

#include "platform.h"

#include "BlockReader.h"
#include "ExceptionInternal.h"
#include "FileSystem.h"
#include "Hash.h"
#include "InputStreamInter.h"
#include "Memory.h"
#include "PeerCache.h"
#include "rpc/RpcAuth.h"
#include "server/Datanode.h"
#include "server/LocatedBlock.h"
#include "server/LocatedBlocks.h"
#include "SessionConfig.h"
#include "Unordered.h"
#include "LockVector.h"

#ifdef MOCK
#include "TestDatanodeStub.h"
#endif

namespace Hdfs {
namespace Internal {

/**
 * A input stream used read data from hdfs.
 */
class InputStreamImpl: public InputStreamInter {
public:
    InputStreamImpl();
    explicit InputStreamImpl(shared_ptr<LocatedBlocks> lbs); 
    ~InputStreamImpl();

    /**
     * Open a file to read
     * @param fs hdfs file system.
     * @param path the file to be read.
     * @param verifyChecksum verify the checksum.
     */
    void open(shared_ptr<FileSystemInter> fs, const char * path, bool verifyChecksum);

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    int32_t read(char * buf, int32_t size);

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @param position the position to seek.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    int32_t pread(char * buf, int32_t size, int64_t position);

    /**
     * To read data from hdfs, block until get the given size of bytes.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     */
    void readFully(char * buf, int64_t size);

    int64_t available();

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     */
    void seek(int64_t pos);

    /**
     * To get the current file point position.
     * @return the position of current file point.
     */
    int64_t tell();

    /**
     * Close the stream.
     */
    void close();

    /**
     * Convert to a printable string
     *
     * @return return a printable string
     */
    std::string toString();

protected:
    bool choseBestNode();
    bool isLocalNode();
    bool isLocalNode(DatanodeInfo & curNode);
    int32_t readInternal(char * buf, int32_t size);
    int32_t preadInternal(char * buf, int32_t size, int64_t position);
    std::vector<shared_ptr<LocatedBlock>> getBlockRange(int64_t offset, int64_t length);
    std::vector<shared_ptr<LocatedBlock>> getFinalizedBlockRange(int64_t offset, int64_t length);
    shared_ptr<LocatedBlock> fetchBlockAt(int64_t offset, int64_t length, bool useCache);
    virtual void fetchBlockByteRange(shared_ptr<LocatedBlock> curBlock, int64_t start, int64_t end, char * buf);
    void setupBlockReader(bool temporaryDisableLocalRead, shared_ptr<BlockReader> & blockReader,
                          shared_ptr<LocatedBlock> curBlock, int64_t start, int64_t end,
                          DatanodeInfo & curNode);
    bool choseBestNode(shared_ptr<LocatedBlock> curBlock, DatanodeInfo & curNode);
    const LocatedBlock * findBlockWithLock();
    int32_t readOneBlock(char * buf, int32_t size, bool shouldUpdateMetadataOnFailure);
    int64_t getFileLength();
    int64_t readBlockLength(const LocatedBlock & b);
    void checkStatus();
    void openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                      bool verifyChecksum);
    void readFullyInternal(char * buf, int64_t size);
    void seekInternal(int64_t pos);
    void seekToBlock(const LocatedBlock & lb);
    void setupBlockReader(bool temporaryDisableLocalRead);
    void updateBlockInfos();
    void updateBlockInfos(bool need);

    /**
     * Get block at the specified position.
     * Fetch it from the namenode if not cached.
     *
     * @param offset block corresponding to this offset in file is returned
     * @return located block
     */
    shared_ptr<LocatedBlock> getBlockAt(int64_t offset);

protected:
    bool closed;
    bool localRead;
    bool readFromUnderConstructedBlock;
    bool verify;
    DatanodeInfo curNode;
    exception_ptr lastError;
    FileStatus fileInfo;
    int maxGetBlockInfoRetry;
    int64_t cursor;
    int64_t endOfCurBlock;
    int64_t lastBlockBeingWrittenLength;
    int64_t prefetchSize;
    PeerCache *peerCache;
    RpcAuth auth;
    shared_ptr<BlockReader> blockReader;
    shared_ptr<FileSystemInter> filesystem;
    shared_ptr<LocatedBlock> curBlock;
    // state shared by stateful and positional read:
    // (protected by mutex on infoMutex)
    shared_ptr<LocatedBlocks> lbs;
    shared_ptr<SessionConfig> conf;
    std::string path;
    LockVector<DatanodeInfo> failedNodes;
    std::vector<char> localReaderBuffer;
    // mutex for state shared between read and pread
    std::recursive_mutex infoMutex;

#ifdef MOCK
private:
    Hdfs::Mock::TestDatanodeStub * stub;
#endif
};

unordered_set<std::string> BuildLocalAddrSet();

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_ */
