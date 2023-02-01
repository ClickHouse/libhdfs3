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
#ifndef _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAM_H_
#define _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAM_H_

#include "FileSystem.h"
#include "ECPolicy.h"

namespace Hdfs {

namespace Internal {
class OutputStreamInter;
}

/**
 * A output stream used to write data to hdfs.
 */
class OutputStream {
public:
    /**
     * Construct a new OutputStream.
     */
    OutputStream();
    OutputStream(Hdfs::Internal::shared_ptr<ECPolicy> ecPolicy);

    /**
     * Destroy a OutputStream instance.
     */
    ~OutputStream();

    /**
     * To create or append a file.
     * @param fs hdfs file system.
     * @param path the file path.
     * @param pair the result of create or append.
     * @param flag creation flag, can be Create, Append or Create|Overwrite.
     * @param permission create a new file with given permission.
     * @param createParent if the parent does not exist, create it.
     * @param replication create a file with given number of replication.
     * @param blockSize  create a file with given block size.
     * @param fileId  the file id.
     */
    void open(FileSystem & fs, const char * path,
              std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> & pair,
              int flag = Create, const Permission permission = Permission(0644),
              bool createParent = false, int replication = 0, int64_t blockSize = 0,
              int64_t fileId = 0);

    void open(FileSystem & fs, const char * path,
              int flag = Create, const Permission permission = Permission(0644),
              bool createParent = false, int replication = 0, int64_t blockSize = 0);

    /**
     * To append data to file.
     * @param buf the data used to append.
     * @param size the data size.
     */
    void append(const char * buf, int64_t size);

    /**
     * Flush all data in buffer and waiting for ack.
     * Will block until get all acks.
     */
    void flush();

    /**
     * return the current file length.
     * @return current file length.
     */
    int64_t tell();

    /**
     * the same as flush right now.
     */
    void sync();

    /**
     * close the stream.
     */
    void close();

private:
    Internal::OutputStreamInter * impl;

};

}

#endif /* _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAM_H_ */
