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
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemInter.h"
#include "InputStreamImpl.h"
#include "InputStreamInter.h"
#include "LocalBlockReader.h"
#include "Logger.h"
#include "RemoteBlockReader.h"
#include "server/Datanode.h"
#include "Thread.h"

#include <algorithm>
#include <ifaddrs.h>
#include <inttypes.h>
#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace Hdfs {
namespace Internal {

unordered_set<std::string> BuildLocalAddrSet() {
    unordered_set<std::string> set;
    struct ifaddrs * ifAddr = NULL;
    struct ifaddrs * pifAddr = NULL;
    struct sockaddr * addr;

    if (getifaddrs(&ifAddr)) {
        THROW(HdfsNetworkException,
              "InputStreamImpl: cannot get local network interface: %s",
              GetSystemErrorInfo(errno));
    }

    try {
        std::vector<char> host;
        const char * pHost;
        host.resize(INET6_ADDRSTRLEN + 1);

        for (pifAddr = ifAddr; pifAddr != NULL; pifAddr = pifAddr->ifa_next) {
            addr = pifAddr->ifa_addr;

            if (!addr) {
                continue;
            }

            memset(host.data(), 0, INET6_ADDRSTRLEN + 1);

            if (addr->sa_family == AF_INET) {
                pHost =
                    inet_ntop(addr->sa_family,
                              &(reinterpret_cast<struct sockaddr_in *>(addr))->sin_addr,
                              host.data(), INET6_ADDRSTRLEN);
            } else if (addr->sa_family == AF_INET6) {
                pHost =
                    inet_ntop(addr->sa_family,
                              &(reinterpret_cast<struct sockaddr_in6 *>(addr))->sin6_addr,
                              host.data(), INET6_ADDRSTRLEN);
            } else {
                continue;
            }

            if (NULL == pHost) {
                THROW(HdfsNetworkException,
                      "InputStreamImpl: cannot get convert network address to textual form: %s",
                      GetSystemErrorInfo(errno));
            }

            set.insert(pHost);
        }

        /*
         * add hostname.
         */
        long hostlen = sysconf(_SC_HOST_NAME_MAX);
        host.resize(hostlen + 1);

        if (gethostname(host.data(), host.size())) {
            THROW(HdfsNetworkException,
                  "InputStreamImpl: cannot get hostname: %s",
                  GetSystemErrorInfo(errno));
        }

        set.insert(host.data());
    } catch (...) {
        if (ifAddr != NULL) {
            freeifaddrs(ifAddr);
        }

        throw;
    }

    if (ifAddr != NULL) {
        freeifaddrs(ifAddr);
    }

    return set;
}

InputStreamImpl::InputStreamImpl() :
    closed(true), localRead(true), readFromUnderConstructedBlock(false), verify(
        true), maxGetBlockInfoRetry(3), cursor(0), endOfCurBlock(0), lastBlockBeingWrittenLength(
            0), prefetchSize(0), peerCache(NULL) {
#ifdef MOCK
    stub = NULL;
#endif
}

InputStreamImpl::InputStreamImpl(shared_ptr<LocatedBlocks> lbsPtr) {
    new (this)InputStreamImpl();
    lock_guard<std::recursive_mutex> lock(infoMutex);
    lbs = lbsPtr;
}

InputStreamImpl::~InputStreamImpl() {
}

void InputStreamImpl::checkStatus() {
    if (closed) {
        THROW(HdfsIOException, "InputStreamImpl: stream is not opened.");
    }

    if (lastError != exception_ptr()) {
        rethrow_exception(lastError);
    }
}


int64_t InputStreamImpl::readBlockLength(const LocatedBlock & b) {
    const std::vector<DatanodeInfo> & nodes = b.getLocations();
    int replicaNotFoundCount = nodes.size();

    for (size_t i = 0; i < nodes.size(); ++i) {
        try {
            int64_t n = 0;
            shared_ptr<Datanode> dn;
            RpcAuth a = auth;
            a.getUser().addToken(b.getToken());
#ifdef MOCK

            if (stub) {
                dn = stub->getDatanode();
            } else {
                dn = shared_ptr < Datanode > (new DatanodeImpl(nodes[i].getIpAddr().c_str(),
                                              nodes[i].getIpcPort(), *conf, a));
            }

#else
            dn = shared_ptr < Datanode > (new DatanodeImpl(nodes[i].getIpAddr().c_str(),
                                          nodes[i].getIpcPort(), *conf, a));
#endif
            n = dn->getReplicaVisibleLength(b);

            if (n >= 0) {
                return n;
            }
        } catch (const ReplicaNotFoundException & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block visible length for Block: %s file %s from Datanode: %s\n%s",
                b.toString().c_str(), path.c_str(), nodes[i].formatAddress().c_str(), GetExceptionDetail(e, buffer));
            LOG(INFO,
                "InputStreamImpl: retry get block visible length for Block: %s file %s from other datanode",
                b.toString().c_str(), path.c_str());
            --replicaNotFoundCount;
        } catch (const HdfsIOException & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block visible length for Block: %s file %s from Datanode: %s\n%s",
                b.toString().c_str(), path.c_str(), nodes[i].formatAddress().c_str(), GetExceptionDetail(e, buffer));
            LOG(INFO,
                "InputStreamImpl: retry get block visible length for Block: %s file %s from other datanode",
                b.toString().c_str(), path.c_str());
        }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
        return 0;
    }

    return -1;
}

/**
 * Getting blocks locations'information from namenode
 */
void InputStreamImpl::updateBlockInfos() {
    updateBlockInfos(true);
}

/**
 * Getting blocks locations'information from namenode
 * @param need Whether getBlockLocations needs to be called.
 */
void InputStreamImpl::updateBlockInfos(bool need) {
    lock_guard<std::recursive_mutex> lock(infoMutex);
    int retry = maxGetBlockInfoRetry;

    for (int i = 0; i < retry; ++i) {
        try {
            if (!lbs) {
                lbs = shared_ptr < LocatedBlocksImpl > (new LocatedBlocksImpl);
            }

            if (need) {
                filesystem->getBlockLocations(path, cursor, prefetchSize, *lbs);
            }

            // set true for retry scenario
            need = true;

            if (lbs->isLastBlockComplete()) {
                lastBlockBeingWrittenLength = 0;
            } else {
                shared_ptr<LocatedBlock> last = lbs->getLastBlock();

                if (!last) {
                    lastBlockBeingWrittenLength = 0;
                } else {
                    lastBlockBeingWrittenLength = readBlockLength(*last);

                    if (lastBlockBeingWrittenLength == -1) {
                        if (i + 1 >= retry) {
                            THROW(HdfsIOException,
                                  "InputStreamImpl: failed to get block visible length for Block: %s from all Datanode.",
                                  last->toString().c_str());
                        } else {
                            LOG(LOG_ERROR,
                                "InputStreamImpl: failed to get block visible length for Block: %s file %s from all Datanode.",
                                last->toString().c_str(), path.c_str());

                            try {
                                sleep_for(milliseconds(4000));
                            } catch (...) {
                            }

                            continue;
                        }
                    }

                    last->setNumBytes(lastBlockBeingWrittenLength);
                }
            }

            return;
        } catch (const HdfsRpcException & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block information for file %s, %s",
                path.c_str(), GetExceptionDetail(e, buffer));

            if (i + 1 >= retry) {
                throw;
            }
        }

        LOG(INFO,
            "InputStreamImpl: retry to get block information for file: %s, already tried %d time(s).",
            path.c_str(), i + 1);
    }
}

int64_t InputStreamImpl::getFileLength() {
    int64_t length = lbs->getFileLength();

    if (!lbs->isLastBlockComplete()) {
        length += lastBlockBeingWrittenLength;
    }

    return length;
}

void InputStreamImpl::seekToBlock(const LocatedBlock & lb) {
    if (cursor >= lbs->getFileLength()) {
        assert(!lbs->isLastBlockComplete());
        readFromUnderConstructedBlock = true;
    } else {
        readFromUnderConstructedBlock = false;
    }

    assert(cursor >= lb.getOffset()
           && cursor < lb.getOffset() + lb.getNumBytes());
    curBlock = shared_ptr < LocatedBlock > (new LocatedBlock(lb));
    int64_t blockSize = curBlock->getNumBytes();
    assert(blockSize > 0);
    endOfCurBlock = blockSize + curBlock->getOffset();
    failedNodes.clear();
    blockReader.reset();
}

bool InputStreamImpl::choseBestNode() {
    const std::vector<DatanodeInfo> & nodes = curBlock->getLocations();

    for (size_t i = 0; i < nodes.size(); ++i) {
        if (failedNodes.binary_search(nodes[i])) {
            continue;
        }

        curNode = nodes[i];
        return true;
    }

    return false;
}

bool InputStreamImpl::choseBestNode(shared_ptr<LocatedBlock> curBlock, DatanodeInfo & curNode) {
    const std::vector<DatanodeInfo> & nodes = curBlock->getLocations();

    for (size_t i = 0; i < nodes.size(); ++i) {
        if (failedNodes.binary_search(nodes[i])) {
            continue;
        }

        curNode = nodes[i];
        return true;
    }

    return false;
}

bool InputStreamImpl::isLocalNode() {
    static const unordered_set<std::string> LocalAddrSet = BuildLocalAddrSet();
    bool retval = LocalAddrSet.find(curNode.getIpAddr()) != LocalAddrSet.end();
    return retval;
}

bool InputStreamImpl::isLocalNode(DatanodeInfo & curNode) {
    static const unordered_set<std::string> LocalAddrSet = BuildLocalAddrSet();
    bool retval = LocalAddrSet.find(curNode.getIpAddr()) != LocalAddrSet.end();
    return retval;
}

void InputStreamImpl::setupBlockReader(bool temporaryDisableLocalRead) {
    bool lastReadFromLocal = false;
    exception_ptr lastException;

    while (true) {
        if (!choseBestNode()) {
            try {
                if (lastException) {
                    rethrow_exception(lastException);
                }
            } catch (...) {
                NESTED_THROW(HdfsIOException,
                             "InputStreamImpl: all nodes have been tried and no valid replica can be read for Block: %s.",
                             curBlock->toString().c_str());
            }

            THROW(HdfsIOException,
                  "InputStreamImpl: all nodes have been tried and no valid replica can be read for Block: %s.",
                  curBlock->toString().c_str());
        }

        try {
            int64_t offset, len;
            offset = cursor - curBlock->getOffset();
            assert(offset >= 0);
            len = curBlock->getNumBytes() - offset;
            assert(len > 0);

            if (!temporaryDisableLocalRead && !lastReadFromLocal &&
                !readFromUnderConstructedBlock && localRead && isLocalNode()) {
                lastReadFromLocal = true;

                shared_ptr<ReadShortCircuitInfo> info;
                ReadShortCircuitInfoBuilder builder(curNode, auth, *conf);

                try {
                    info = builder.fetchOrCreate(*curBlock, curBlock->getToken());

                    if (!info) {
                        continue;
                    }

                    assert(info->isValid());
                    blockReader = shared_ptr<BlockReader>(
                        new LocalBlockReader(info, *curBlock, offset, verify,
                                             *conf, localReaderBuffer));
                } catch (...) {
                    if (info) {
                        info->setValid(false);
                    }

                    throw;
                }
            } else {
                const char * clientName = filesystem->getClientName();
                lastReadFromLocal = false;
                blockReader = shared_ptr<BlockReader>(new RemoteBlockReader(
                    *curBlock, curNode, *peerCache, offset, len,
                    curBlock->getToken(), clientName, verify, *conf));
            }

            break;
        } catch (const HdfsIOException & e) {
            lastException = current_exception();
            std::string buffer;

            if (lastReadFromLocal) {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\n"
                    "retry the same node but disable read shortcircuit feature",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
                /*
                 * do not add node into failedNodes since we will retry the same node but
                 * disable local block reading
                 */
            } else {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\nretry another node",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
                failedNodes.push_back(curNode);
                failedNodes.sort();
            }
        }
    }
}

void InputStreamImpl::setupBlockReader(bool temporaryDisableLocalRead, shared_ptr<BlockReader> & blockReader,
                                       shared_ptr<LocatedBlock> curBlock, int64_t start, int64_t end,
                                       DatanodeInfo & curNode) {
    bool lastReadFromLocal = false;
    exception_ptr lastException;

    while (true) {
        if (!choseBestNode(curBlock, curNode)) {
            try {
                if (lastException) {
                    rethrow_exception(lastException);
                }
            } catch (...) {
                NESTED_THROW(HdfsIOException,
                             "InputStreamImpl: all nodes have been tried and no valid replica can be read for Block: %s.",
                             curBlock->toString().c_str());
            }

            THROW(HdfsIOException,
                  "InputStreamImpl: all nodes have been tried and no valid replica can be read for Block: %s.",
                  curBlock->toString().c_str());
        }

        try {
            int64_t offset, len;
            offset = start;
            assert(offset >= 0);
            len = end - start + 1;
            assert(len > 0);

            bool readFromUnderConstructedBlock = curBlock->isLastBlock();
            if (!temporaryDisableLocalRead && !lastReadFromLocal &&
                !readFromUnderConstructedBlock && localRead && isLocalNode(curNode)) {
                lastReadFromLocal = true;

                std::vector<char> localReaderBuffer;
                shared_ptr<ReadShortCircuitInfo> info;
                ReadShortCircuitInfoBuilder builder(curNode, auth, *conf);

                try {
                    info = builder.fetchOrCreate(*curBlock, curBlock->getToken());

                    if (!info) {
                        continue;
                    }

                    assert(info->isValid());
                    blockReader = shared_ptr<BlockReader>(
                            new LocalBlockReader(info, *curBlock, offset, verify,
                                                 *conf, localReaderBuffer));
                } catch (...) {
                    if (info) {
                        info->setValid(false);
                    }

                    throw;
                }
            } else {
                const char * clientName = filesystem->getClientName();
                lastReadFromLocal = false;
                blockReader = shared_ptr<BlockReader>(new RemoteBlockReader(
                        *curBlock, curNode, *peerCache, offset, len,
                        curBlock->getToken(), clientName, verify, *conf));
            }

            break;
        } catch (const HdfsIOException & e) {
            lastException = current_exception();
            std::string buffer;

            if (lastReadFromLocal) {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\n"
                    "retry the same node but disable read shortcircuit feature",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
                /*
                 * do not add node into failedNodes since we will retry the same node but
                 * disable local block reading
                 */
            } else {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\nretry another node",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
                failedNodes.push_back(curNode);
                failedNodes.sort();
            }
        }
    }
}

void InputStreamImpl::open(shared_ptr<FileSystemInter> fs, const char * path,
                           bool verifyChecksum) {
    if (NULL == path || 0 == strlen(path)) {
        THROW(InvalidParameter, "path is invalid.");
    }

    try {
        openInternal(fs, path, verifyChecksum);
    } catch (...) {
        close();
        throw;
    }
}

void InputStreamImpl::openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                                   bool verifyChecksum) {
    try {
        filesystem = fs;
        verify = verifyChecksum;
        this->path = fs->getStandardPath(path);
        LOG(DEBUG2, "%p, open file %s for read, verfyChecksum is %s", this, this->path.c_str(), (verifyChecksum ? "true" : "false"));
        conf = shared_ptr < SessionConfig > (new SessionConfig(fs->getConf()));
        this->auth = RpcAuth(fs->getUserInfo(), RpcAuth::ParseMethod(conf->getRpcAuthMethod()));
        prefetchSize = conf->getDefaultBlockSize() * conf->getPrefetchSize();
        localRead = conf->isReadFromLocal();
        maxGetBlockInfoRetry = conf->getMaxGetBlockInfoRetry();
        peerCache = &fs->getPeerCache();
        updateBlockInfos(!lbs);
        closed = false;
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const FileNotFoundException & e) {
        throw;
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException, "InputStreamImpl: cannot open file: %s.",
                     this->path.c_str());
    }
}

int32_t InputStreamImpl::read(char * buf, int32_t size) {
    checkStatus();

    try {
        int64_t prvious = cursor;
        int32_t done = readInternal(buf, size);
        LOG(DEBUG3, "%p read file %s size is %d, offset %" PRId64 " done %d, next pos %" PRId64, this, path.c_str(), size,
            prvious, done, cursor);
        return done;
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

int32_t InputStreamImpl::pread(char * buf, int32_t size, int64_t position) {
    checkStatus();

    try {
        int32_t done = preadInternal(buf, size, position);
        LOG(DEBUG3, "%p pread file %s size is %d, offset %" PRId64 " done %d" PRId64, this, path.c_str(), size,
            position, done);
        return done;
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

int32_t InputStreamImpl::readOneBlock(char * buf, int32_t size, bool shouldUpdateMetadataOnFailure) {
    bool temporaryDisableLocalRead = false;
    std::string buffer;

    while (true) {
        try {
            /*
             * Setup block reader here and handle failure.
             */
            if (!blockReader) {
                setupBlockReader(temporaryDisableLocalRead);
                temporaryDisableLocalRead = false;
            }
        } catch (const HdfsInvalidBlockToken & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s, \n%s, retry after updating block informations.",
                curBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
            return -1;
        } catch (const HdfsIOException & e) {
            /*
             * In setupBlockReader, we have tried all the replicas.
             * We now update block informations once, and try again.
             */
            if (shouldUpdateMetadataOnFailure) {
                LOG(LOG_ERROR,
                    "InputStreamImpl: failed to read Block: %s file %s, \n%s, retry after updating block informations.",
                    curBlock->toString().c_str(), path.c_str(),
                    GetExceptionDetail(e, buffer));
                return -1;
            } else {
                /*
                 * We have updated block informations and failed again.
                 */
                throw;
            }
        }

        /*
         * Block reader has been setup, read from block reader.
         */
        try {
            int32_t todo = size;
            todo = todo < endOfCurBlock - cursor ?
                   todo : static_cast<int32_t>(endOfCurBlock - cursor);
            assert(blockReader);
            todo = blockReader->read(buf, todo);
            cursor += todo;
            /*
             * Exit the loop and function from here if success.
             */
            return todo;
        } catch (const HdfsIOException & e) {
            /*
             * Failed to read from current block reader,
             * add the current datanode to invalid node list and try again.
             */
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));

            if (conf->doesNotRetryAnotherNode()) {
                throw;
            }
        } catch (const ChecksumException & e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
        }

        /*
         * Successfully create the block reader but failed to read.
         * Disable the local block reader and try the same node again.
         */
        if (!blockReader || dynamic_cast<LocalBlockReader *>(blockReader.get())) {
            temporaryDisableLocalRead = true;
        } else {
            /*
             * Remote block reader failed to read, try another node.
             */
            LOG(INFO, "IntputStreamImpl: Add invalid datanode %s to failed datanodes and try another datanode again for file %s.",
                curNode.formatAddress().c_str(), path.c_str());
            failedNodes.push_back(curNode);
            failedNodes.sort();
        }

        blockReader.reset();
    }
}

/**
 * To read data from hdfs.
 * @param buf the buffer used to filled.
 * @param size buffer size.
 * @return return the number of bytes filled in the buffer, it may less than size.
 */
int32_t InputStreamImpl::readInternal(char * buf, int32_t size) {
    int updateMetadataOnFailure = conf->getMaxReadBlockRetry();

    try {
        do {
            const LocatedBlock * lb = NULL;

            /*
             * Check if we have got the block information we need.
             */
            if (!lbs || cursor >= getFileLength()
                    || (cursor >= endOfCurBlock && !(lb = findBlockWithLock()))) {
                /*
                 * Get block information from namenode.
                 * Do RPC failover work in updateBlockInfos.
                 */
                updateBlockInfos();
            }

            /*
             * We already have the up-to-date block information,
             * Check if we reach the end of file.
             */
            if (cursor >= getFileLength()) {
                THROW_NO_STACK(HdfsEndOfStream,
                      "InputStreamImpl: read over EOF, current position: %" PRId64 ", read size: %d, from file: %s",
                      cursor, size, path.c_str());
            }

            /*
             * If we reach the end of block or the block information has just updated,
             * seek to the right block to read.
             */
            if (cursor >= endOfCurBlock) {
                lb = findBlockWithLock();

                if (!lb) {
                    THROW(HdfsIOException,
                          "InputStreamImpl: cannot find block information at position: %" PRId64 " for file: %s",
                          cursor, path.c_str());
                }

                /*
                 * Seek to the right block, setup all needed variable,
                 * but do not setup block reader, setup it latter.
                 */
                seekToBlock(*lb);
            }

            int32_t retval = readOneBlock(buf, size, updateMetadataOnFailure > 0);

            /*
             * Now we have tried all replicas and failed.
             * We will update metadata once and try again.
             */
            if (retval < 0) {
                {
                    lock_guard<std::recursive_mutex> lock(infoMutex);
                    lbs.reset();
                    /*
                     * update block infos right now after lbs is reset
                     * to ensure pread can read non-empty lbs.
                     */
                    updateBlockInfos();
                }
                endOfCurBlock = 0;
                --updateMetadataOnFailure;

                try {
                    sleep_for(seconds(1));
                } catch (...) {
                }

                continue;
            }

            return retval;
        } while (true);
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (const HdfsException & e) {
        /*
         * wrap the underlying error and rethrow.
         */
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot read file: %s, from position %" PRId64 ", size: %d.",
                     path.c_str(), cursor, size);
    }
}

/**
 * To read data from hdfs.
 * @param buf the buffer used to filled.
 * @param size buffer size.
 * @param position the position to seek.
 * @return return the number of bytes filled in the buffer, it may less than size.
 */
int32_t InputStreamImpl::preadInternal(char * buf, int32_t size, int64_t position) {
    int64_t cursor = position;
    try {
        int32_t realLen = size;
        std::vector<shared_ptr<LocatedBlock>> blockRange;
        {
            lock_guard<std::recursive_mutex> lock(infoMutex);
            int64_t filelen = getFileLength();
            if ((position < 0) || (position >= filelen)) {
                return -1;
            }
            if ((position + size) > filelen) {
                realLen = (int32_t) (filelen - position);
            }

            // determine the block and byte range within the block
            // corresponding to position and realLen
            blockRange = getBlockRange(position, (int64_t) realLen);
        }
        int32_t remaining = realLen;
        int32_t bytesHasRead = 0;
        for (shared_ptr<LocatedBlock> blk : blockRange) {
            int64_t targetStart = position - blk->getOffset();
            int32_t bytesToRead = std::min(remaining, (int32_t)(blk->getNumBytes() - targetStart));
            int64_t targetEnd = targetStart + bytesToRead - 1;

            fetchBlockByteRange(blk, targetStart, targetEnd, buf + bytesHasRead);

            bytesHasRead += bytesToRead;
            remaining -= bytesToRead;
            position += bytesToRead;
        }
        assert(remaining == 0);
        return realLen;
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (const HdfsException & e) {
        /*
         * wrap the underlying error and rethrow.
         */
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot pread file: %s, from position %" PRId64 ", size: %d.",
                     path.c_str(), cursor, size);
    }
}

/**
 * Get blocks in the specified range.
 * Fetch them from the namenode if not cached. This function
 * will not get a read request beyond the EOF.
 * @param offset starting offset in file
 * @param length length of data
 * @return consequent segment of located blocks
 * @throws IOException
 */
std::vector<shared_ptr<LocatedBlock>> InputStreamImpl::getBlockRange(int64_t offset, int64_t length) {
    lock_guard<std::recursive_mutex> lock(infoMutex);
    // getFileLength(): returns total file length
    // locatedBlocks.getFileLength(): returns length of completed blocks
    if (offset >= getFileLength()) {
        THROW(HdfsIOException, "Offset: %" PRId64 " exceeds file length: %" PRId64, offset, getFileLength());
    }
    std::vector<shared_ptr<LocatedBlock>> blocks;
    int64_t lengthOfCompleteBlk = lbs->getFileLength();
    bool readOffsetWithinCompleteBlk = offset < lengthOfCompleteBlk;
    bool readLengthPastCompleteBlk = offset + length > lengthOfCompleteBlk;

    if (readOffsetWithinCompleteBlk) {
        // get the blocks of finalized (completed) block range
        blocks = getFinalizedBlockRange(offset, std::min(length, lengthOfCompleteBlk - offset));
    }

    // get the blocks from incomplete block range
    if (readLengthPastCompleteBlk) {
        shared_ptr<LocatedBlock> lastBlock = shared_ptr<LocatedBlock>(new LocatedBlock);
        *lastBlock = *(lbs->getLastBlock());
        lastBlock->setLastBlock(true);
        blocks.push_back(lastBlock);
    }

    return blocks;
}

/**
 * Get blocks in the specified range.
 * Includes only the complete blocks.
 * Fetch them from the namenode if not cached.
 */
std::vector<shared_ptr<LocatedBlock>> InputStreamImpl::getFinalizedBlockRange(int64_t offset, int64_t length) {
    lock_guard<std::recursive_mutex> lock(infoMutex);
    assert(lbs);
    std::vector<shared_ptr<LocatedBlock>> blockRange;
    // search cached blocks first
    int64_t remaining = length;
    int64_t curOff = offset;
    while(remaining > 0) {
        shared_ptr<LocatedBlock> blk = fetchBlockAt(curOff, remaining, true);
        assert(curOff >= blk->getOffset());
        blockRange.push_back(blk);
        int64_t bytesRead = blk->getOffset() + blk->getNumBytes() - curOff;
        remaining -= bytesRead;
        curOff += bytesRead;
    }
    return blockRange;
}

shared_ptr<LocatedBlock> InputStreamImpl::fetchBlockAt(int64_t offset, int64_t length, bool useCache) {
    lock_guard<std::recursive_mutex> lock(infoMutex);
    int32_t targetBlockIdx;
    const LocatedBlock * lb = lbs->findBlock(offset, targetBlockIdx);
    if (!lb) { // block is not cached
        useCache = false;
    }
    if (!useCache) { // fetch blocks
        shared_ptr<LocatedBlocks> newBlocks = shared_ptr < LocatedBlocksImpl > (new LocatedBlocksImpl);
        if (length == 0) {
            filesystem->getBlockLocations(path, offset, prefetchSize, *newBlocks);
        } else {
            filesystem->getBlockLocations(path, offset, length, *newBlocks);
        }
        if (newBlocks->getBlocks().empty()) {
            THROW(HdfsIOException, "Could not find target position %" PRId64, offset);
        }
        lbs->insertRange(targetBlockIdx, newBlocks->getBlocks());
        lb = lbs->findBlock(offset);
        if (!lb) {
            LOG(LOG_ERROR, "fetchBlockAt failed, offset:%" PRId64 ", length:%" PRId64 ", targetBlockIdx:%d", offset,
                length, targetBlockIdx);
            THROW(HdfsIOException, "After insertRange, could not find target position %" PRId64, offset);
        }
    }
    shared_ptr<LocatedBlock> ret = shared_ptr<LocatedBlock>(new LocatedBlock);
    *ret = *lb;
    return ret;
}

void InputStreamImpl::fetchBlockByteRange(shared_ptr<LocatedBlock> curBlock, int64_t start, int64_t end, char * buf) {
    bool temporaryDisableLocalRead = false;
    std::string buffer;
    shared_ptr<BlockReader> blockReader;
    DatanodeInfo curNode;
    int32_t refetchToken = 1; // only need to get a new access token once

    while (true) {
        try {
            /*
             * Setup block reader here and handle failure.
             */
            if (!blockReader) {
                setupBlockReader(temporaryDisableLocalRead, blockReader, curBlock, start, end, curNode);
                temporaryDisableLocalRead = false;
            }
        } catch (const HdfsInvalidBlockToken & e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s, \n%s.",
                curBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
            if (refetchToken > 0 && !curBlock->isLastBlock()) {
                curBlock = fetchBlockAt(curBlock->getOffset(), end - start + 1, false);
                refetchToken -= 1;
                continue;
            }
            throw;
        } catch (const HdfsIOException & e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s, \n%s.",
                curBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
            throw;
        }

        /*
         * Block reader has been setup, read from block reader.
         */
        try {
            assert(blockReader);
            int nread = 0;
            int ret;
            int32_t len = (int32_t)(end - start + 1);
            int32_t remain = len;
            while (remain > 0) {
                ret = blockReader->read(buf + nread, remain);
                if (ret <= 0) {
                    break;
                }
                nread += ret;
                remain -= ret;
            }

            if (nread != len) {
                THROW(HdfsIOException, "truncated return from reader.read(): excpected %d got %d", len, nread);
            }
            /*
             * Exit the loop and function from here if success.
             */
            return;
        } catch (const HdfsIOException & e) {
            /*
             * Failed to read from current block reader,
             * add the current datanode to invalid node list and try again.
             */
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));

            if (conf->doesNotRetryAnotherNode()) {
                throw;
            }
        } catch (const ChecksumException & e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
        }

        /*
         * Successfully create the block reader but failed to read.
         * Disable the local block reader and try the same node again.
         */
        if (!blockReader || dynamic_cast<LocalBlockReader *>(blockReader.get())) {
            temporaryDisableLocalRead = true;
        } else {
            /*
             * Remote block reader failed to read, try another node.
             */
            LOG(INFO, "IntputStreamImpl: Add invalid datanode %s to failed datanodes and try another datanode again for file %s.",
                curNode.formatAddress().c_str(), path.c_str());
            failedNodes.push_back(curNode);
            failedNodes.sort();
        }

        blockReader.reset();
    }
}

const LocatedBlock * InputStreamImpl::findBlockWithLock() {
    lock_guard<std::recursive_mutex> lock(infoMutex);
    return lbs->findBlock(cursor);
}

/**
 * To read data from hdfs, block until get the given size of bytes.
 * @param buf the buffer used to filled.
 * @param size the number of bytes to be read.
 */
void InputStreamImpl::readFully(char * buf, int64_t size) {
    LOG(DEBUG3, "readFully file %s size is %" PRId64 ", offset %" PRId64, path.c_str(), size, cursor);
    checkStatus();

    try {
        return readFullyInternal(buf, size);
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void InputStreamImpl::readFullyInternal(char * buf, int64_t size) {
    int32_t done;
    int64_t pos = cursor, todo = size;

    try {
        while (todo > 0) {
            done = todo < std::numeric_limits<int32_t>::max() ?
                   static_cast<int32_t>(todo) :
                   std::numeric_limits<int32_t>::max();
            done = readInternal(buf + (size - todo), done);
            todo -= done;
        }
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        THROW_NO_STACK(HdfsEndOfStream,
              "InputStreamImpl: read over EOF, current position: %" PRId64 ", read size: %" PRId64 ", from file: %s",
              pos, size, path.c_str());
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot read fully from file: %s, from position %" PRId64 ", size: %" PRId64 ".",
                     path.c_str(), pos, size);
    }
}

int64_t InputStreamImpl::available() {
    checkStatus();

    try {
        if (blockReader) {
            return blockReader->available();
        }
    } catch (...) {
        lastError = current_exception();
        throw;
    }

    return 0;
}

/**
 * To move the file point to the given position.
 * @param size the given position.
 */
void InputStreamImpl::seek(int64_t pos) {
    LOG(DEBUG2, "%p seek file %s to %" PRId64 ", offset %" PRId64, this, path.c_str(), pos, cursor);
    checkStatus();

    try {
        seekInternal(pos);
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void InputStreamImpl::seekInternal(int64_t pos) {
    if (cursor == pos) {
        return;
    }

    if (!lbs || pos > getFileLength()) {
        updateBlockInfos();

        if (pos > getFileLength()) {
            THROW_NO_STACK(HdfsEndOfStream,
                  "InputStreamImpl: seek over EOF, current position: %" PRId64 ", seek target: %" PRId64 ", in file: %s",
                  cursor, pos, path.c_str());
        }
    }

    try {
        if (blockReader && pos > cursor && pos < endOfCurBlock && pos - cursor <= 128 * 1024) {
            blockReader->skip(pos - cursor);
            cursor = pos;
            return;
        }
    } catch (const HdfsIOException & e) {
        std::string buffer;
        LOG(LOG_ERROR, "InputStreamImpl: failed to skip %" PRId64 " bytes in current block reader for file %s\n%s",
            pos - cursor, path.c_str(), GetExceptionDetail(e, buffer));
        LOG(INFO, "InputStreamImpl: retry to seek to position %" PRId64 " for file %s", pos, path.c_str());
    } catch (const ChecksumException & e) {
        std::string buffer;
        LOG(LOG_ERROR, "InputStreamImpl: failed to skip %" PRId64 " bytes in current block reader for file %s\n%s",
            pos - cursor, path.c_str(), GetExceptionDetail(e, buffer));
        LOG(INFO, "InputStreamImpl: retry to seek to position %" PRId64 " for file %s", pos, path.c_str());
    }

    /**
     * the seek target exceed the current block or skip failed in current block reader.
     * reset current block reader and set the cursor to the target position to seek.
     */
    endOfCurBlock = 0;
    blockReader.reset();
    cursor = pos;
}

/**
 * To get the current file point position.
 * @return the position of current file point.
 */
int64_t InputStreamImpl::tell() {
    checkStatus();
    LOG(DEBUG2, "tell file %s at %" PRId64, path.c_str(), cursor);
    return cursor;
}

/**
 * Close the stream.
 */
void InputStreamImpl::close() {
    LOG(DEBUG2, "%p close file %s for read", this, path.c_str());
    closed = true;
    localRead = true;
    readFromUnderConstructedBlock = false;
    verify = true;
    filesystem.reset();
    cursor = 0;
    endOfCurBlock = 0;
    lastBlockBeingWrittenLength = 0;
    prefetchSize = 0;
    blockReader.reset();
    curBlock.reset();
    {
        lock_guard<std::recursive_mutex> lock(infoMutex);
        lbs.reset();
    }
    conf.reset();
    failedNodes.clear();
    path.clear();
    localReaderBuffer.resize(0);
    lastError = exception_ptr();
}

std::string InputStreamImpl::toString() {
    if (path.empty()) {
        return std::string("InputStream for path ") + path;
    } else {
        return std::string("InputStream (not opened)");
    }
}

shared_ptr<LocatedBlock> InputStreamImpl::getBlockAt(int64_t offset) {
    lock_guard<std::recursive_mutex> lock(infoMutex);
    assert(lbs != NULL);

    shared_ptr<LocatedBlock> blk;

    // check offset
    if (offset < 0 || offset >= getFileLength()) {
        THROW(HdfsIOException, "offset < 0 || offset >= getFileLength(), offset: %" PRId64 ", file length: %" PRId64,
              offset, getFileLength());
    } else if (offset >= lbs->getFileLength()) {
        // offset to the portion of the last block,
        // which is not known to the name-node yet;
        // getting the last block
        blk = lbs->getLastBlock();
        blk->setLastBlock(true);
    } else {
        // search cached blocks first
        blk = fetchBlockAt(offset, 0, true);
    }
    return blk;
}

}
}
