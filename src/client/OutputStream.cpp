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
#include "Atomic.h"
#include "FileSystemImpl.h"
#include "Memory.h"
#include "OutputStream.h"
#include "OutputStreamImpl.h"
#include "StripedOutputStreamImpl.h"

using namespace Hdfs::Internal;

namespace Hdfs {

OutputStream::OutputStream() {
    impl = new Internal::OutputStreamImpl;
}

OutputStream::OutputStream(ECPolicy * ecPolicy) {
    impl = new Internal::StripedOutputStreamImpl(ecPolicy);
}

OutputStream::~OutputStream() {
    delete impl;
}

void OutputStream::open(FileSystem & fs, const char * path,
                        std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> & pair,
                        int flag, const Permission permission, bool createParent, int replication,
                        int64_t blockSize, int64_t fileId) {
    if (!fs.impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->open(fs.impl->filesystem, path, pair, flag, permission, createParent, replication,
               blockSize, fileId);
}

void OutputStream::open(FileSystem & fs, const char * path,
                        int flag, const Permission permission, bool createParent, int replication,
                        int64_t blockSize) {
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> pair;
    open(fs, path, pair, flag, permission, createParent, replication, blockSize, 0);
}

/**
 * To append data to file.
 * @param buf the data used to append.
 * @param size the data size.
 */
void OutputStream::append(const char * buf, int64_t size) {
    impl->append(buf, size);
}

/**
 * Flush all data in buffer and waiting for ack.
 * Will block until get all acks.
 */
void OutputStream::flush() {
    impl->flush();
}

/**
 * return the current file length.
 * @return current file length.
 */
int64_t OutputStream::tell() {
    return impl->tell();
}

/**
 * the same as flush right now.
 */
void OutputStream::sync() {
    impl->sync();
}

/**
 * close the stream.
 */
void OutputStream::close() {
    impl->close();
}

}
