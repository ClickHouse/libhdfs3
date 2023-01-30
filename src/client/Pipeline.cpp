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
#include "DateTime.h"
#include "Pipeline.h"
#include "Logger.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "OutputStreamInter.h"
#include "FileSystemInter.h"
#include "datatransfer.pb.h"
#include "server/Datanode.h"
#include "DataReader.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace ::google::protobuf;
using namespace google::protobuf::io;
#include "Faultjector.h"

#include <inttypes.h>

namespace Hdfs {
namespace Internal {

PipelineImpl::PipelineImpl(const char * path, SessionConfig & conf,
                           shared_ptr<FileSystemInter> filesystem, int checksumType, int chunkSize,
                           int replication, int64_t bytesSent, PacketPool & packetPool,
                           shared_ptr<LocatedBlock> lastBlock, int64_t fileId) :
    config(conf), checksumType(checksumType), chunkSize(chunkSize), errorIndex(-1), replication(replication), bytesAcked(
    bytesSent), bytesSent(bytesSent), packetPool(packetPool), filesystem(filesystem), lastBlock(lastBlock), path(
    path), fileId(fileId) {
    canAddDatanode = conf.canAddDatanode();
    blockWriteRetry = conf.getBlockWriteRetry();
    connectTimeout = conf.getOutputConnTimeout();
    readTimeout = conf.getOutputReadTimeout();
    writeTimeout = conf.getOutputWriteTimeout();
    clientName = filesystem->getClientName();
}

PipelineImpl::PipelineImpl(bool append, const char * path, SessionConfig & conf,
                           shared_ptr<FileSystemInter> filesystem, int checksumType, int chunkSize,
                           int replication, int64_t bytesSent, PacketPool & packetPool,
                           shared_ptr<LocatedBlock> lastBlock, int64_t fileId) :
    config(conf), checksumType(checksumType), chunkSize(chunkSize), errorIndex(-1), replication(replication), bytesAcked(
        bytesSent), bytesSent(bytesSent), packetPool(packetPool), filesystem(filesystem), lastBlock(lastBlock), path(
            path), fileId(fileId) {
    canAddDatanode = conf.canAddDatanode();
    canAddDatanodeBest = conf.canAddDatanodeBest();
    blockWriteRetry = conf.getBlockWriteRetry();
    connectTimeout = conf.getOutputConnTimeout();
    readTimeout = conf.getOutputReadTimeout();
    writeTimeout = conf.getOutputWriteTimeout();
    clientName = filesystem->getClientName();

    if (append) {
        LOG(DEBUG2, "create pipeline for file %s to append to %s at position %" PRId64,
            path, lastBlock->toString().c_str(), lastBlock->getNumBytes());
        stage = PIPELINE_SETUP_APPEND;
        assert(lastBlock);
        nodes = lastBlock->getLocations();
        storageIDs = lastBlock->getStorageIDs();
        buildForAppendOrRecovery(false);
        stage = DATA_STREAMING;
    } else {
        LOG(DEBUG2, "create pipeline for file %s to write to a new block", path);
        stage = PIPELINE_SETUP_CREATE;
        buildForNewBlock();
        stage = DATA_STREAMING;
    }
}

int PipelineImpl::findNewDatanode(const std::vector<DatanodeInfo> & original) {
    if (nodes.size() != original.size() + 1) {
        THROW(HdfsIOException, "Failed to acquire a datanode for block %s from namenode.",
              lastBlock->toString().c_str());
    }

    for (size_t i = 0; i < nodes.size(); i++) {
        size_t j = 0;

        for (; j < original.size() && !(nodes[i] == original[j]); j++)
            ;

        if (j == original.size()) {
            return i;
        }
    }

    THROW(HdfsIOException, "Cannot add new datanode for block %s.", lastBlock->toString().c_str());
}

void PipelineImpl::transfer(const ExtendedBlock & blk, const DatanodeInfo & src,
                            const std::vector<DatanodeInfo> & targets, const Token & token) {
    shared_ptr<Socket> so(new TcpSocketImpl);
    shared_ptr<BufferedSocketReader> in(new BufferedSocketReaderImpl(*so));
    so->connect(src.getIpAddr().c_str(), src.getXferPort(), connectTimeout);
    EncryptionKey key = filesystem->getEncryptionKeys();
    DataTransferProtocolSender sender(*so, writeTimeout, src.formatAddress(), config.getEncryptedDatanode(),
        config.getSecureDatanode(), key, config.getCryptoBufferSize(), config.getDataProtection());
    sender.transferBlock(blk, token, clientName.c_str(), targets);
    char error_text[2048];
    sprintf(error_text, "from %s for block %s.", nodes[0].formatAddress().c_str(), lastBlock->toString().c_str());
    DataReader datareader(&sender, in, readTimeout);
    int size;
    std::vector<char> &buf = datareader.readResponse(error_text, size);
    BlockOpResponseProto resp;
    if (!resp.ParseFromArray(buf.data(), size)) {
        DataTransferEncryptorMessageProto resp2;
        if (resp2.ParseFromArray(buf.data(), size))
        {
            if (resp2.status() != DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS) {
                THROW(HdfsIOException, "Error doing transfer from %s for block %s.: %s",
              nodes[0].formatAddress().c_str(), lastBlock->toString().c_str(), resp2.message().c_str());
            }
        }
        THROW(HdfsIOException, "cannot parse datanode response from %s for block %s.",
              src.formatAddress().c_str(), lastBlock->toString().c_str());
    }

    if (Status::DT_PROTO_SUCCESS != resp.status()) {
        THROW(HdfsIOException, "Failed to transfer block to a new datanode %s for block %s.",
              targets[0].formatAddress().c_str(),
              lastBlock->toString().c_str());
    }
}

bool PipelineImpl::addDatanodeToPipeline(const std::vector<DatanodeInfo> & excludedNodes) {
    try {
        /*
         * get a new datanode
         */
        std::vector<DatanodeInfo> original = nodes;
        shared_ptr<LocatedBlock> lb;
        lb = filesystem->getAdditionalDatanode(path, *lastBlock, nodes, storageIDs,
                                               excludedNodes, 1);
        nodes = lb->getLocations();
        storageIDs = lb->getStorageIDs();

        /*
         * failed to add new datanode into pipeline.
         */
        if (original.size() == nodes.size()) {
            LOG(LOG_ERROR,
                "Failed to add new datanode into pipeline for block: %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
        } else {
            /*
             * find the new datanode
             */
            int d = findNewDatanode(original);
            /*
             * in case transfer block fail.
             */
            errorIndex = d;
            /*
             * transfer replica
             */
            DatanodeInfo & src = d == 0 ? nodes[1] : nodes[d - 1];
            std::vector<DatanodeInfo> targets;
            targets.push_back(nodes[d]);
            LOG(INFO, "Replicate block %s from %s to %s for file %s.", lastBlock->toString().c_str(),
                src.formatAddress().c_str(), targets[0].formatAddress().c_str(), path.c_str());
            try {
                transfer(*lastBlock, src, targets, lb->getToken());
                errorIndex = -1;
                return true;
            } catch (HdfsIOException &ex) {
                if (!config.getEncryptedDatanode() && config.getSecureDatanode()) {
                    config.setSecureDatanode(false);
                    filesystem->getConf().setSecureDatanode(false);
                    LOG(INFO, "Tried to use SASL connection but failed, falling back to non SASL");
                    transfer(*lastBlock, src, targets, lb->getToken());
                    errorIndex = -1;
                    return true;
                } else {
                    throw;
                }
            }
        }
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsFileSystemClosed & e) {
        throw;
    } catch (const SafeModeException & e) {
        throw;
    } catch (const HdfsException & e) {
        std::string buffer;
        LOG(LOG_ERROR,
            "Failed to add a new datanode into pipeline for block: %s file %s.\n%s",
            lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
    }

    return false;
}

void PipelineImpl::checkPipelineWithReplicas() {
    if (static_cast<int>(nodes.size()) < replication) {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        int size = nodes.size();

        for (int i = 0; i < size - 1; ++i) {
            ss << nodes[i].formatAddress() << ", ";
        }

        if (nodes.empty()) {
            ss << "Empty";
        } else {
            ss << nodes.back().formatAddress();
        }

        LOG(WARNING,
            "the number of nodes in pipeline is %d [%s], is less than the expected number of replica %d for block %s file %s",
            static_cast<int>(nodes.size()), ss.str().c_str(), replication,
            lastBlock->toString().c_str(), path.c_str());
    }
}

void PipelineImpl::buildForAppendOrRecovery(bool recovery) {
    int64_t gs = 0;
    int retry = blockWriteRetry;
    exception_ptr lastException;
    std::vector<DatanodeInfo> excludedNodes;
    std::vector<DatanodeInfo> empty;
    shared_ptr<LocatedBlock> lb;
    std::string buffer;
    DatanodeInfo removed;
    std::string storageID;
    bool useRemoved = false;

    do {
        /*
         * Remove bad datanode from list of datanodes.
         * If errorIndex was not set (i.e. appends), then do not remove
         * any datanodes
         */
        useRemoved = false;
        storageID = "";
        if (errorIndex >= 0) {
            assert(lastBlock);
            bool invalid = true;
            LOG(LOG_ERROR, "Pipeline: node %s had error. Trying to ping to test if valid.",
                nodes[errorIndex].formatAddress().c_str());
            try {
                RpcAuth a = RpcAuth(filesystem->getUserInfo(), RpcAuth::ParseMethod(config.getRpcAuthMethod()));
                shared_ptr<Datanode> dn = shared_ptr < Datanode > (new DatanodeImpl(nodes[errorIndex].getIpAddr().c_str(),
                                              nodes[errorIndex].getIpcPort(), config, a));
                dn->sendPing();
                invalid = false;
                LOG(INFO, "Pipeline: node %s was able to ping. Will continue to use.",
                    nodes[errorIndex].formatAddress().c_str());
                removed = nodes[errorIndex];
                if (errorIndex < (int)storageIDs.size())
                    storageID = storageIDs[errorIndex];
                useRemoved = true;
            }
            catch (...) {
            }
            if (invalid) {
                /*
                 * If node was pingable, don't exclude it, but do remove it for now. We will then
                 * be able to add it back later.
                */
                LOG(LOG_ERROR, "Pipeline: node %s is invalid and removed from pipeline when %s block %s for file %s, stage = %s.",
                    nodes[errorIndex].formatAddress().c_str(),
                    (recovery ? "recovery" : "append to"), lastBlock->toString().c_str(),
                    path.c_str(), StageToString(stage));
                excludedNodes.push_back(nodes[errorIndex]);
            }
            nodes.erase(nodes.begin() + errorIndex);

            if (!storageIDs.empty()) {
                storageIDs.erase(storageIDs.begin() + errorIndex);
            }

            if (nodes.empty() && invalid) {
                THROW(HdfsIOException,
                      "Build pipeline to %s block %s failed: all datanodes are bad.",
                      (recovery ? "recovery" : "append to"), lastBlock->toString().c_str());
            }

            errorIndex = -1;
        }

        try {
            gs = 0;

            /*
             * Check if the number of datanodes in pipeline satisfy the replication requirement,
             * add new datanode if not
             */
            if (stage != PIPELINE_SETUP_CREATE && stage != PIPELINE_CLOSE
                    && static_cast<int>(nodes.size()) < replication && canAddDatanode) {
                // Single data node case
                bool added = false;
                if (nodes.empty() && useRemoved) {
                    if (storageID.length()) {
                        LOG(INFO, "Pipeline: Adding back only datanode %s", removed.formatAddress().c_str());
                        nodes.push_back(removed);
                        lb = filesystem->updateBlockForPipeline(*lastBlock);
                        storageIDs.push_back(storageID);
                        lb->setPoolId(lastBlock->getPoolId());
                        lb->setBlockId(lastBlock->getBlockId());
                        lb->setLocations(nodes);
                        lb->setStorageIDs(storageIDs);
                        lb->setNumBytes(lastBlock->getNumBytes());
                        lb->setOffset(lastBlock->getOffset());
                        filesystem->updatePipeline(*lastBlock, *lb, nodes, storageIDs);
                        added = true;
                    }
                }
                if (!added && !addDatanodeToPipeline(excludedNodes)) {

                    // We may have remove nodes due to timeout, try again, but allow for
                    // excluded ones to be added back
                    if (!addDatanodeToPipeline(empty) && !canAddDatanodeBest) {
                        THROW(HdfsIOException,
                              "Failed to add new datanode into pipeline for block: %s file %s, "
                              "set \"output.replace-datanode-on-failure\" to \"false\" to disable this feature.",
                              lastBlock->toString().c_str(), path.c_str());
                    }
                }
            }
            if (nodes.empty()) {
                THROW(HdfsIOException,
                      "Build pipeline to %s block failed: all datanodes are bad.",
                      (recovery ? "recovery" : "append to"));
            }

            if (errorIndex >= 0) {
                continue;
            }

            checkPipelineWithReplicas();
            /*
             * Update generation stamp and access token
             */
            lb = filesystem->updateBlockForPipeline(*lastBlock);
            gs = lb->getGenerationStamp();
            /*
             * Try to build pipeline
             */
            createBlockOutputStream(lb->getToken(), gs, recovery);
            /*
             * everything is ok, reset errorIndex.
             */
            errorIndex = -1;
            lastException = exception_ptr();
            break; //break on success
        } catch (const HdfsInvalidBlockToken & e) {
            lastException = current_exception();
            recovery = true;
            LOG(LOG_ERROR,
                "Pipeline: Failed to build pipeline for block %s file %s, new generation stamp is %" PRId64 ",\n%s",
                lastBlock->toString().c_str(), path.c_str(), gs, GetExceptionDetail(e, buffer));
            LOG(INFO, "Try to recovery pipeline for block %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
        } catch (const HdfsTimeoutException & e) {
            lastException = current_exception();
            recovery = true;
            LOG(LOG_ERROR,
                "Pipeline: Failed to build pipeline for block %s file %s, new generation stamp is %" PRId64 ",\n%s",
                lastBlock->toString().c_str(), path.c_str(), gs, GetExceptionDetail(e, buffer));
            LOG(INFO, "Try to recovery pipeline for block %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
        } catch (const HdfsIOException & e) {
            lastException = current_exception();
            /*
             * Set recovery flag to true in case of failed to create a pipeline for appending.
             */
            recovery = true;
            LOG(LOG_ERROR,
                "Pipeline: Failed to build pipeline for block %s file %s, new generation stamp is %" PRId64 ",\n%s",
                lastBlock->toString().c_str(), path.c_str(), gs, GetExceptionDetail(e, buffer));
            LOG(INFO, "Try to recovery pipeline for block %s file %s.", lastBlock->toString().c_str(), path.c_str());
        }

        /*
         * we don't known what happened, no datanode is reported failure, reduce retry count in case infinite loop.
         * it may caused by rpc call throw HdfsIOException
         */
        if (errorIndex < 0) {
            --retry;
        }
    } while (retry > 0);

    if (lastException) {
        rethrow_exception(lastException);
    }

    /*
     * Update pipeline at the namenode, non-idempotent RPC call.
     */
    lb->setPoolId(lastBlock->getPoolId());
    lb->setBlockId(lastBlock->getBlockId());
    lb->setLocations(nodes);
    lb->setStorageIDs(storageIDs);
    lb->setNumBytes(lastBlock->getNumBytes());
    lb->setOffset(lastBlock->getOffset());
    filesystem->updatePipeline(*lastBlock, *lb, nodes, storageIDs);
    lastBlock = lb;
}

void PipelineImpl::locateNextBlock(
    const std::vector<DatanodeInfo> & excludedNodes) {
    milliseconds sleeptime(100);
    milliseconds fiveSeconds(5000);
    int retry = blockWriteRetry;

    while (true) {
        try {
            lastBlock = filesystem->addBlock(path, lastBlock.get(),
                                             excludedNodes, fileId);
            assert(lastBlock);
            return;
        } catch (const NotReplicatedYetException & e) {
            LOG(DEBUG1, "Got NotReplicatedYetException when try to addBlock for block %s, "
                "already retry %d times, max retry %d times", lastBlock->toString().c_str(),
                blockWriteRetry - retry, blockWriteRetry);

            if (retry--) {
                try {
                    sleep_for(sleeptime);
                } catch (...) {
                }

                sleeptime *= 2;
                sleeptime = sleeptime < fiveSeconds ? sleeptime : fiveSeconds;
            } else {
                throw;
            }
        }
    }
}

static std::string FormatExcludedNodes(
    const std::vector<DatanodeInfo> & excludedNodes) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << "[";
    int size = excludedNodes.size();

    for (int i = 0; i < size - 1; ++i) {
        ss << excludedNodes[i].formatAddress() << ", ";
    }

    if (excludedNodes.empty()) {
        ss << "Empty";
    } else {
        ss << excludedNodes.back().formatAddress();
    }

    ss << "]";
    return ss.str();
}

void PipelineImpl::buildForNewBlock() {
    int retryAllocNewBlock = 0, retry = blockWriteRetry;
    LocatedBlock lb;
    std::vector<DatanodeInfo> excludedNodes;
    shared_ptr<LocatedBlock> block = lastBlock;
    std::string buffer;

    do {
        errorIndex = -1;
        lastBlock = block;

        try {
            locateNextBlock(excludedNodes);
            lastBlock->setNumBytes(0);
            nodes = lastBlock->getLocations();
            storageIDs = lastBlock->getStorageIDs();
        } catch (const HdfsRpcException & e) {
            const char * lastBlockName = lastBlock ? lastBlock->toString().c_str() : "Null";
            LOG(LOG_ERROR,
                "Failed to allocate a new empty block for file %s, last block %s, excluded nodes %s.\n%s",
                path.c_str(), lastBlockName, FormatExcludedNodes(excludedNodes).c_str(), GetExceptionDetail(e, buffer));

            if (retryAllocNewBlock > blockWriteRetry) {
                throw;
            }

            LOG(INFO, "Retry to allocate a new empty block for file %s, last block %s, excluded nodes %s.",
                path.c_str(), lastBlockName, FormatExcludedNodes(excludedNodes).c_str());
            ++retryAllocNewBlock;
            continue;
        } catch (const HdfsException & e) {
            const char * lastBlockName = lastBlock ? lastBlock->toString().c_str() : "Null";
            LOG(LOG_ERROR,
                "Failed to allocate a new empty block for file %s, last block %s, excluded nodes %s.\n%s",
                path.c_str(), lastBlockName, FormatExcludedNodes(excludedNodes).c_str(), GetExceptionDetail(e, buffer));
            throw;
        }

        retryAllocNewBlock = 0;
        checkPipelineWithReplicas();

        if (nodes.empty()) {
            THROW(HdfsIOException,
                  "No datanode is available to create a pipeline for block %s file %s.",
                  lastBlock->toString().c_str(), path.c_str());
        }

        try {
            createBlockOutputStream(lastBlock->getToken(), 0, false);
            break;  //break on success
        } catch (const HdfsInvalidBlockToken & e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
        } catch (const HdfsTimeoutException & e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
        } catch (const HdfsIOException & e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
        }

        LOG(INFO, "Abandoning block: %s for file %s.", lastBlock->toString().c_str(), path.c_str());

        try {
            filesystem->abandonBlock(*lastBlock, path, fileId);
        } catch (const HdfsException & e) {
            LOG(LOG_ERROR,
                "Failed to abandon useless block %s for file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
            throw;
        }

        if (errorIndex >= 0) {
            LOG(INFO, "Excluding invalid datanode: %s for block %s for file %s",
                nodes[errorIndex].formatAddress().c_str(), lastBlock->toString().c_str(), path.c_str());
            excludedNodes.push_back(nodes[errorIndex]);
        } else {
            /*
             * we don't known what happened, no datanode is reported failure, reduce retry count in case of infinite loop.
             */
            --retry;
        }
    } while (retry);
}

/*
 * bad link node must be either empty or a "IP:PORT"
 */
void PipelineImpl::checkBadLinkFormat(const std::string & n) {
    std::string node = n;

    if (node.empty()) {
        return;
    }

    do {
        const char * host = node.data(), *port;
        size_t pos = node.find_last_of(":");

        if (pos == node.npos || pos + 1 == node.length()) {
            break;
        }

        node[pos] = 0;
        port = node.data() + pos + 1;
        struct addrinfo hints, *addrs;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = PF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
        int p;
        char * end;
        p = strtol(port, &end, 0);

        if (p >= 65536 || p <= 0 || end != port + strlen(port)) {
            break;
        }

        if (getaddrinfo(host, port, &hints, &addrs)) {
            break;
        }

        freeaddrinfo(addrs);
        return;
    } while (0);

    LOG(FATAL, "Cannot parser the firstBadLink string %s, it should be a bug or protocol incompatible.",
        n.c_str());
    THROW(HdfsException, "Cannot parser the firstBadLink string %s, it should be a bug or protocol incompatible.",
          n.c_str());
}

void PipelineImpl::createBlockOutputStream(const Token & token, int64_t gs, bool recovery) {
    std::string firstBadLink;
    exception_ptr lastError;
    bool needWrapException = true;

    try {
        sock = shared_ptr < Socket > (new TcpSocketImpl);
        reader = shared_ptr<BufferedSocketReader>(new BufferedSocketReaderImpl(*sock));
        sock->connect(nodes[0].getIpAddr().c_str(), nodes[0].getXferPort(),
                      connectTimeout);
        std::vector<DatanodeInfo> targets;

        for (size_t i = 1; i < nodes.size(); ++i) {
            targets.push_back(nodes[i]);
        }
        EncryptionKey key = filesystem->getEncryptionKeys();
        sender = shared_ptr<DataTransferProtocolSender>(new DataTransferProtocolSender(*sock, writeTimeout,
                                          nodes[0].formatAddress(),
                                          config.getEncryptedDatanode(),
                                          config.getSecureDatanode(),
                                          key, config.getCryptoBufferSize(),
                                          config.getDataProtection()));
        sender->writeBlock(*lastBlock, token, clientName.c_str(), targets,
                          (recovery ? (stage | 0x1) : stage), nodes.size(),
                          lastBlock->getNumBytes(), bytesSent, gs, checksumType, chunkSize);
        char error_text[2048];
        sprintf(error_text, "from %s for block %s.", nodes[0].formatAddress().c_str(), lastBlock->toString().c_str());
        DataReader datareader(sender.get(), reader, readTimeout);
        int size;
        std::vector<char> &buf = datareader.readResponse(error_text, size);
        BlockOpResponseProto resp;
        if (!resp.ParseFromArray(buf.data(), size)) {
            DataTransferEncryptorMessageProto resp2;
            if (resp2.ParseFromArray(buf.data(), size))
            {
                if (resp2.status() != DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS) {
                    THROW(HdfsIOException, "Error creating output stream from %s for block %s.: %s",
                  nodes[0].formatAddress().c_str(), lastBlock->toString().c_str(), resp2.message().c_str());
                }
            }
            THROW(HdfsIOException, "cannot parse datanode response from %s for block %s.",
                  nodes[0].formatAddress().c_str(), lastBlock->toString().c_str());
        }

        Status pipelineStatus = resp.status();
        firstBadLink = resp.firstbadlink();

        if (Status::DT_PROTO_SUCCESS != pipelineStatus) {
            needWrapException = false;

            if (Status::DT_PROTO_ERROR_ACCESS_TOKEN == pipelineStatus) {
                THROW(HdfsInvalidBlockToken,
                      "Got access token error for connect ack with firstBadLink as %s for block %s",
                      firstBadLink.c_str(), lastBlock->toString().c_str());
            } else {
                THROW(HdfsIOException, "Bad connect ack with firstBadLink as %s for block %s",
                      firstBadLink.c_str(), lastBlock->toString().c_str());
            }
        }

        return;
    } catch (HdfsIOException &ex) {
        if (!config.getEncryptedDatanode() && config.getSecureDatanode()) {
            config.setSecureDatanode(false);
            filesystem->getConf().setSecureDatanode(false);
            LOG(INFO, "Tried to use SASL connection but failed, falling back to non SASL");
            createBlockOutputStream(token, gs, recovery);
            return;
        } else {
            errorIndex = 0;
            lastError = current_exception();
        }
    } catch (...) {
        errorIndex = 0;
        lastError = current_exception();
    }

    checkBadLinkFormat(firstBadLink);

    if (!firstBadLink.empty()) {
        for (size_t i = 0; i < nodes.size(); ++i) {
            if (nodes[i].getXferAddr() == firstBadLink) {
                errorIndex = i;
                break;
            }
        }
    }

    assert(lastError);

    if (!needWrapException) {
        rethrow_exception(lastError);
    }

    try {
        rethrow_exception(lastError);
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "Cannot create block output stream for block %s, "
                     "recovery flag: %s, with last generate stamp %" PRId64 ".",
                     lastBlock->toString().c_str(), (recovery ? "true" : "false"), gs);
    }
}

void PipelineImpl::resend() {
    assert(stage != PIPELINE_CLOSE);

    for (size_t i = 0; i < packets.size(); ++i) {
        ConstPacketBuffer b = packets[i]->getBuffer();
        if (sender && sender->isWrapped()) {
            std::string indata;
            int size = b.getSize();
            indata.resize(size);
            memcpy(indata.data(), b.getBuffer(), size);
            std::string data = sender->wrap(indata);
            WriteBuffer buffer;
            if (sender->needsLength())
                buffer.writeBigEndian(static_cast<int32_t>(data.length()));
            char * b = buffer.alloc(data.length());
            memcpy(b, data.c_str(), data.length());
            sock->writeFully(buffer.getBuffer(0), buffer.getDataSize(0),
                         writeTimeout);
        }
        else {
            sock->writeFully(b.getBuffer(), b.getSize(),
                             writeTimeout);
        }
        int64_t tmp = packets[i]->getLastByteOffsetBlock();
        bytesSent = bytesSent > tmp ? bytesSent : tmp;
    }
}

void PipelineImpl::send(shared_ptr<Packet> packet) {
    ConstPacketBuffer buffer = packet->getBuffer();

    if (!packet->isHeartbeat()) {
        packets.push_back(packet);
    }

    /*
     * too many packets pending on the ack. wait in case of consuming to much memory.
     */
    if (static_cast<int>(packets.size()) > packetPool.getMaxSize()) {
        waitForAcks(false);
    }

    bool failover = false;

    do {
        try {
            if (failover) {
                resend();
            } else {
                assert(sock);
                // test bad node
                if (FaultInjector::get().testBadWriterAtKillPos(bytesSent)) {
                    LOG(INFO, "testBadWriterAtKillPos, bytesSent=%ld, bytesAcked=%ld",
                        bytesSent, bytesAcked);
                    THROW(HdfsIOException, "bad RemoteBlockWriter");
                }
                if (sender && sender->isWrapped()) {
                    std::string indata;
                    int size = buffer.getSize();
                    indata.resize(size);
                    memcpy(indata.data(), buffer.getBuffer(), size);
                    std::string data = sender->wrap(indata);
                    WriteBuffer buffer2;
                    if (sender->needsLength())
                        buffer2.writeBigEndian(static_cast<int32_t>(data.length()));
                    char * b = buffer2.alloc(data.length());
                    memcpy(b, data.c_str(), data.length());
                    sock->writeFully(buffer2.getBuffer(0), buffer2.getDataSize(0),
                                 writeTimeout);
                }
                else {
                    sock->writeFully(buffer.getBuffer(), buffer.getSize(),
                                     writeTimeout);
                }
                int64_t tmp = packet->getLastByteOffsetBlock();
                bytesSent = bytesSent > tmp ? bytesSent : tmp;
            }

            checkResponse(false);
            return;
        } catch (const HdfsIOException & e) {
            if (errorIndex < 0) {
                errorIndex = 0;
            }

            sock.reset();
        }

        if (lastBlock->isStriped()) {
            THROW(HdfsIOException, "ec block send failed");
        }

        buildForAppendOrRecovery(true);
        failover = true;

        if (stage == PIPELINE_CLOSE) {
            assert(packets.size() == 1 && packets[0]->isLastPacketInBlock());
            packets.clear();
            break;
        }
    } while (true);
}

void PipelineImpl::processAck(PipelineAck & ack) {
    assert(!ack.isInvalid());
    int64_t seqno = ack.getSeqno();

    if (HEART_BEAT_SEQNO == seqno) {
        return;
    }

    assert(!packets.empty());
    Packet & packet = *packets[0];

    if (ack.isSuccess()) {
        if (packet.getSeqno() != seqno) {
            THROW(HdfsIOException,
                  "processAck: pipeline ack expecting seqno %" PRId64 "  but received %" PRId64 " for block %s.",
                  packet.getSeqno(), seqno, lastBlock->toString().c_str());
        }

        int64_t tmp = packet.getLastByteOffsetBlock();
        bytesAcked = tmp > bytesAcked ? tmp : bytesAcked;
        assert(lastBlock);
        lastBlock->setNumBytes(bytesAcked);

        if (packet.isLastPacketInBlock()) {
            sock.reset();
        }

        packetPool.relesePacket(packets[0]);
        packets.pop_front();
    } else {
        for (int i = ack.getNumOfReplies() - 1; i >= 0; --i) {
            if (Status::DT_PROTO_SUCCESS != ack.getReply(i)) {
                errorIndex = i;
                /*
                 * handle block token expire as same as HdfsIOException.
                 */
                THROW(HdfsIOException,
                      "processAck: ack report error at node: %s for block %s.",
                      nodes[i].formatAddress().c_str(), lastBlock->toString().c_str());
            }
        }
    }
}

void PipelineImpl::processResponse() {
    PipelineAck ack;
    int size = 0;
    char error_text[2048];
    sprintf(error_text, "for block %s.", lastBlock->toString().c_str());
    DataReader datareader(sender.get(), reader, readTimeout);

    do {
        std::vector<char> &buf = datareader.readResponse(error_text, size);
        ack.reset();
        ack.readFrom(buf.data(), size);

        if (ack.isInvalid()) {
            THROW(HdfsIOException,
                  "processAllAcks: get an invalid DataStreamer packet ack for block %s",
                  lastBlock->toString().c_str());
        }

        processAck(ack);
    } while (datareader.getRest().size() > 0);
}

void PipelineImpl::checkResponse(bool wait) {
    int timeout = wait ? readTimeout : 0;
    bool readable = reader->poll(timeout);

    if (readable) {
        processResponse();
    } else if (wait) {
        THROW(HdfsIOException, "Timeout when reading response for block %s, datanode %s do not response.",
              lastBlock->toString().c_str(),
              nodes[0].formatAddress().c_str());
    }
}

void PipelineImpl::flush() {
    waitForAcks(true);
}

void PipelineImpl::waitForAcks(bool force) {
    lock_guard < mutex > lock(mut);
    bool failover = false;

    while (!packets.empty()) {
        /*
         * just wait for some acks in case of consuming too much memory.
         */
        if (!force && static_cast<int>(packets.size()) < packetPool.getMaxSize()) {
            return;
        }

        try {
            if (failover) {
                resend();
            }

            // test bad node
            if (FaultInjector::get().testBadWriterAtAckPos(bytesAcked)) {
                LOG(INFO, "testBadWriterAtAckPos, bytesSent=%ld, bytesAcked=%ld",
                    bytesSent, bytesAcked);
                THROW(HdfsIOException, "bad RemoteBlockWriter");
            }

            checkResponse(true);
            failover = false;
        } catch (const HdfsIOException & e) {
            if (errorIndex < 0) {
                errorIndex = 0;
            }

            std::string buffer;
            LOG(LOG_ERROR,
                "Failed to flush pipeline(index=%d) on datanode %s for block %s file %s.\n%s",
                errorIndex, nodes[errorIndex].formatAddress().c_str(), lastBlock->toString().c_str(),
                path.c_str(), GetExceptionDetail(e, buffer));
            LOG(INFO, "Rebuild pipeline to flush for block %s file %s.", lastBlock->toString().c_str(), path.c_str());
            sock.reset();
            failover = true;
        }

        if (failover) {
            if (lastBlock->isStriped()) {
                failover = false;
                THROW(HdfsIOException, "ec block wait ack failed");
            }
            buildForAppendOrRecovery(true);

            if (stage == PIPELINE_CLOSE) {
                assert(packets.size() == 1 && packets[0]->isLastPacketInBlock());
                packets.clear();
                break;
            }
        }
    }
}

int64_t PipelineImpl::getBytesSent() {
    return bytesSent;
}

bool PipelineImpl::isClosed() {
    return stage == PIPELINE_CLOSE;
}

shared_ptr<LocatedBlock> PipelineImpl::close(shared_ptr<Packet> lastPacket) {
    // test bad node
    if (FaultInjector::get().testPipelineClose()) {
        LOG(INFO, "testPipelineClose, bytesSent=%ld, bytesAcked=%ld",
            bytesSent, bytesAcked);
        THROW(HdfsIOException, "bad RemoteBlockWriter");
    }
    waitForAcks(true);
    lastPacket->setLastPacketInBlock(true);
    stage = PIPELINE_CLOSE;
    send(lastPacket);
    waitForAcks(true);
    sock.reset();
    lastBlock->setNumBytes(bytesAcked);
    LOG(DEBUG2, "close pipeline for file %s, block %s with length %" PRId64,
        path.c_str(), lastBlock->toString().c_str(),
        lastBlock->getNumBytes());
    return lastBlock;
}

StripedPipelineImpl::StripedPipelineImpl(const char * path, SessionConfig & conf,
                    shared_ptr<FileSystemInter> filesystem, int checksumType, int chunkSize,
                    int replication, int64_t bytesSent, PacketPool & packetPool,
                    shared_ptr<LocatedBlock> lastBlock, int64_t fileId) :
                    PipelineImpl(path, conf, filesystem, checksumType, chunkSize, replication,
                                 bytesSent, packetPool, lastBlock, fileId) {
    LOG(DEBUG2, "create pipeline for file %s to write to a new block", path);
    stage = PIPELINE_SETUP_CREATE;
    buildForNewBlock(lastBlock);
    stage = DATA_STREAMING;
}

void StripedPipelineImpl::buildForNewBlock(shared_ptr<LocatedBlock> block) {
    int retryAllocNewBlock = 0, retry = blockWriteRetry;
    std::string buffer;

    do {
        errorIndex = -1;
        lastBlock = block;

        try {
            lastBlock->setNumBytes(0);
            nodes = lastBlock->getLocations();
            storageIDs = lastBlock->getStorageIDs();
        } catch (const HdfsRpcException &e) {
            const char *lastBlockName = lastBlock ? lastBlock->toString().c_str() : "Null";
            LOG(LOG_ERROR,
                "Failed to allocate a new empty block for file %s, last block %s.\n%s",
                path.c_str(), lastBlockName, GetExceptionDetail(e, buffer));

            if (retryAllocNewBlock > blockWriteRetry) {
                throw;
            }

            LOG(INFO, "Retry to allocate a new empty block for file %s, last block %s.",
                path.c_str(), lastBlockName);
            ++retryAllocNewBlock;
            continue;
        } catch (const HdfsException &e) {
            const char *lastBlockName = lastBlock ? lastBlock->toString().c_str() : "Null";
            LOG(LOG_ERROR,
                "Failed to allocate a new empty block for file %s, last block %s.\n%s",
                path.c_str(), lastBlockName, GetExceptionDetail(e, buffer));
            throw;
        }

        retryAllocNewBlock = 0;
        if (nodes.empty()) {
            THROW(HdfsIOException,
                  "No datanode is available to create a pipeline for block %s file %s.",
                  lastBlock->toString().c_str(), path.c_str());
        }

        try {
            // test bad node
            if (FaultInjector::get().testCreateOutputStreamFailed()) {
                LOG(INFO, "testCreateOutputStreamFailed, bytesSent=%ld, bytesAcked=%ld",
                    bytesSent, bytesAcked);
                THROW(HdfsIOException, "bad RemoteBlockWriter");
            }
            createBlockOutputStream(lastBlock->getToken(), 0, false);
            break;  //break on success
        } catch (const HdfsInvalidBlockToken &e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
        } catch (const HdfsTimeoutException &e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
        } catch (const HdfsIOException &e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
        }

        LOG(INFO, "Abandoning block: %s for file %s.", lastBlock->toString().c_str(), path.c_str());

        try {
            filesystem->abandonBlock(*lastBlock, path, fileId);
        } catch (const HdfsException &e) {
            LOG(LOG_ERROR,
                "Failed to abandon useless block %s for file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e, buffer));
            throw;
        }

        if (errorIndex >= 0) {
            LOG(INFO, "Excluding invalid datanode: %s for block %s for file %s",
                nodes[errorIndex].formatAddress().c_str(), lastBlock->toString().c_str(), path.c_str());
        } else {
            /*
             * we don't known what happened, no datanode is reported failure, reduce retry count in case of infinite loop.
             */
            --retry;
        }

        if (lastBlock->isStriped() && errorIndex >= 0) {
            THROW(HdfsIOException, "build for ec newBlock failed");
        }
    } while (retry);
}

}
}
