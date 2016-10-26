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
#include "client/Token.h"
#include "datatransfer.pb.h"
#include "DataTransferProtocolSender.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "hdfs.pb.h"
#include "Security.pb.h"
#include "WriteBuffer.h"
#include "network/BufferedSocketReader.h"


using namespace google::protobuf;

namespace Hdfs {
namespace Internal {

static inline void Send(Socket & sock, DataTransferOp op, Message * msg,
                        int writeTimeout, SaslClient *saslClient) {
    WriteBuffer buffer;
    buffer.writeBigEndian(static_cast<int16_t>(DATA_TRANSFER_VERSION));
    buffer.write(static_cast<char>(op));
    int msgSize = msg->ByteSize();
    buffer.writeVarint32(msgSize);
    char * b = buffer.alloc(msgSize);

    if (!msg->SerializeToArray(b, msgSize)) {
        THROW(HdfsIOException,
              "DataTransferProtocolSender cannot serialize header to send buffer.");
    }
    std::string rawdata;
    if (saslClient) {
        std::string data = saslClient->encode(buffer.getBuffer(0), buffer.getDataSize(0));
        WriteBuffer buffer2;
        if (saslClient->needsLength())
            buffer2.writeBigEndian(static_cast<int32_t>(data.length()));
        char * b = buffer2.alloc(data.length());
        memcpy(b, data.c_str(), data.length());
        int size = buffer2.getDataSize(0);
        rawdata.resize(size);
        memcpy(rawdata.data(), buffer2.getBuffer(0), size);
    }
    else {
        int size = buffer.getDataSize(0);
        rawdata.resize(size);
        memcpy(rawdata.data(), buffer.getBuffer(0), size);
    }
    sock.writeFully(rawdata.c_str(), rawdata.length(), writeTimeout);
}

static inline void BuildBaseHeader(const ExtendedBlock & block,
                                   const Token & accessToken, BaseHeaderProto * header) {
    ExtendedBlockProto * eb = header->mutable_block();
    TokenProto * token = header->mutable_token();
    eb->set_blockid(block.getBlockId());
    eb->set_generationstamp(block.getGenerationStamp());
    eb->set_numbytes(block.getNumBytes());
    eb->set_poolid(block.getPoolId());
    token->set_identifier(accessToken.getIdentifier());
    token->set_password(accessToken.getPassword());
    token->set_kind(accessToken.getKind());
    token->set_service(accessToken.getService());
}

static inline void BuildClientHeader(const ExtendedBlock & block,
                                     const Token & accessToken, const char * clientName,
                                     ClientOperationHeaderProto * header) {
    header->set_clientname(clientName);
    BuildBaseHeader(block, accessToken, header->mutable_baseheader());
}

static inline void BuildNodeInfo(const DatanodeInfo & node,
                                 DatanodeInfoProto * info) {
    DatanodeIDProto * id = info->mutable_id();
    id->set_hostname(node.getHostName());
    id->set_infoport(node.getInfoPort());
    id->set_ipaddr(node.getIpAddr());
    id->set_ipcport(node.getIpcPort());
    id->set_datanodeuuid(node.getDatanodeId());
    id->set_xferport(node.getXferPort());
    info->set_location(node.getLocation());
}

static inline void BuildNodesInfo(const std::vector<DatanodeInfo> & nodes,
                                  RepeatedPtrField<DatanodeInfoProto> * infos) {
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        BuildNodeInfo(nodes[i], infos->Add());
    }
}

DataTransferProtocolSender::DataTransferProtocolSender(Socket & sock,
        int writeTimeout, const std::string & datanodeAddr, bool secure, bool token,
        EncryptionKey& key, int32_t cryptoBufferSize, int32_t protection) :
    sock(sock), writeTimeout(writeTimeout), datanode(datanodeAddr), isSecure(secure),
    isToken(token), saslComplete(false), saslClient(NULL), theKey(key), cryptoBufferSize(cryptoBufferSize),
     protection(protection){
}

DataTransferProtocolSender::~DataTransferProtocolSender() {
    if (saslClient)
        delete saslClient;
}

void DataTransferProtocolSender::readBlock(const ExtendedBlock & blk,
        const Token & blockToken, const char * clientName,
        int64_t blockOffset, int64_t length) {
    try {
        OpReadBlockProto op;
        op.set_len(length);
        op.set_offset(blockOffset);
        BuildClientHeader(blk, blockToken, clientName, op.mutable_header());
        if (isSecure || isToken)
            setupSasl(blk, blockToken);
        Send(sock, READ_BLOCK, &op, writeTimeout, saslClient);
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        NESTED_THROW(HdfsEndOfStream,
                     "DataTransferProtocolSender cannot send write request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsIOException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send write request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send read request to datanode %s.",
                     datanode.c_str());
    }
}

void DataTransferProtocolSender::writeBlock(const ExtendedBlock & blk,
        const Token & blockToken, const char * clientName,
        const std::vector<DatanodeInfo> & targets, int stage, int pipelineSize,
        int64_t minBytesRcvd, int64_t maxBytesRcvd,
        int64_t latestGenerationStamp, int checksumType, int bytesPerChecksum) {
    try {
        OpWriteBlockProto op;
        op.set_latestgenerationstamp(latestGenerationStamp);
        op.set_minbytesrcvd(minBytesRcvd);
        op.set_maxbytesrcvd(maxBytesRcvd);
        op.set_pipelinesize(targets.size());
        op.set_stage((OpWriteBlockProto_BlockConstructionStage) stage);
        BuildClientHeader(blk, blockToken, clientName, op.mutable_header());
        ChecksumProto * ck = op.mutable_requestedchecksum();
        ck->set_bytesperchecksum(bytesPerChecksum);
        ck->set_type((ChecksumTypeProto) checksumType);
        BuildNodesInfo(targets, op.mutable_targets());
        if (isSecure || isToken)
            setupSasl(blk, blockToken);
        Send(sock, WRITE_BLOCK, &op, writeTimeout, saslClient);
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        NESTED_THROW(HdfsEndOfStream,
                     "DataTransferProtocolSender cannot send write request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsIOException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send write request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send write request to datanode %s.",
                     datanode.c_str());
    }
}

void DataTransferProtocolSender::transferBlock(const ExtendedBlock & blk,
        const Token & blockToken, const char * clientName,
        const std::vector<DatanodeInfo> & targets) {
    try {
        OpTransferBlockProto op;
        BuildClientHeader(blk, blockToken, clientName, op.mutable_header());
        BuildNodesInfo(targets, op.mutable_targets());
        if (isSecure || isToken)
            setupSasl(blk, blockToken);
        Send(sock, TRANSFER_BLOCK, &op, writeTimeout, saslClient);
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        NESTED_THROW(HdfsEndOfStream,
                     "DataTransferProtocolSender cannot send transfer request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsIOException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send transfer request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send transfer request to datanode %s.",
                     datanode.c_str());
    }
}

void DataTransferProtocolSender::blockChecksum(const ExtendedBlock & blk,
        const Token & blockToken) {
    try {
        //TODO
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send checksum request to datanode %s.",
                     datanode.c_str());
    }
}

void DataTransferProtocolSender::requestShortCircuitFds(const ExtendedBlock blk,
                                                        const Token& blockToken,
                                                        uint32_t maxVersion) {
    try {
        OpRequestShortCircuitAccessProto op;
        BuildBaseHeader(blk, blockToken, op.mutable_header());
        op.set_maxversion(maxVersion);
        if (isSecure || isToken)
            setupSasl(blk, blockToken);

        Send(sock, REQUEST_SHORT_CIRCUIT_FDS, &op, writeTimeout, saslClient);
    } catch (const HdfsCanceled& e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        NESTED_THROW(HdfsEndOfStream,
                     "DataTransferProtocolSender cannot send short-circuit fds request to datanode %s.",
                     datanode.c_str());
     } catch (const HdfsIOException & e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send short-circuit fds request to datanode %s.",
                     datanode.c_str());
    } catch (const HdfsException& e) {
        NESTED_THROW(HdfsIOException,
                     "DataTransferProtocolSender cannot send request "
                     "short-circuit fds request "
                     "to datanode %s.",
                     datanode.c_str());
    }
}

void sendSaslMessage(Socket & sock, DataTransferEncryptorMessageProto_DataTransferEncryptorStatus status,
    std::string payload, std::string message, int writeTimeout, bool secure) {
    DataTransferEncryptorMessageProto msg;

    msg.set_status(status);
    msg.set_payload(payload.c_str());
    msg.set_message(message);

    if (secure) {
        CipherOptionProto* added = msg.add_cipheroption();
        added->set_suite(CipherSuiteProto::AES_CTR_NOPADDING);
    }
    WriteBuffer buffer;
    int msgSize = msg.ByteSize();
    buffer.writeVarint32(msgSize);
    char * b = buffer.alloc(msgSize);

    if (!msg.SerializeToArray(b, msgSize)) {
        THROW(HdfsIOException,
              "DataTransferProtocolSender cannot serialize SASL message to send buffer.");
    }

    sock.writeFully(buffer.getBuffer(0), buffer.getDataSize(0), writeTimeout);


}

void readSaslMessage(Socket & sock, int readTimeout, DataTransferEncryptorMessageProto &msg,
                        std::string &datanode) {
    std::vector<char> buffer(128);
    std::vector<char> body(128);
    uint32_t headerSize = 0;
    /*
     * read response header
     */
    BufferedSocketReaderImpl in(sock);

    headerSize = in.readVarint32(readTimeout);
    buffer.resize(headerSize);
    in.readFully(&buffer[0], headerSize, readTimeout);

    if (!msg.ParseFromArray(buffer.data(), headerSize)) {
        THROW(HdfsRpcException,
              "DataNode to \"%s\" got protocol mismatch: cannot parse response header.",
              datanode.c_str())
    }
    if (msg.status() != DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS) {
        THROW(HdfsRpcException,
              "DataNode to \"%s\" got protocol mismatch: got error reading SASL response: %s.",
              datanode.c_str(), msg.message().c_str())
    }
}

bool DataTransferProtocolSender::isWrapped() {
    if (saslClient && (saslClient->isPrivate() || saslClient->isIntegrity()))
        return true;
    return false;
}

bool DataTransferProtocolSender::needsLength() {
    if (saslClient)
        return saslClient->needsLength();
    return true;
}

std::string DataTransferProtocolSender::unwrap(std::string& data) {
    std::string rawdata = saslClient->decode(data.c_str(), data.length());
    return rawdata;
}

std::string DataTransferProtocolSender::wrap(std::string& data) {
    std::string rawdata = saslClient->encode(data.c_str(), data.length());
    return rawdata;
}

std::string DataTransferProtocolSender::unwrap(const char *input, size_t input_len) {
    std::string rawdata = saslClient->decode(input, input_len);
    return rawdata;
}

std::string DataTransferProtocolSender::wrap(const char *input, size_t input_len) {
    std::string rawdata = saslClient->encode(input, input_len);
    return rawdata;
}

extern std::string Base64Encode(const std::string & in);

void DataTransferProtocolSender::setupSasl(const ExtendedBlock blk, const Token& blockToken) {
    WriteBuffer buffer;
    buffer.writeBigEndian((int)0xDEADBEEF);

    sock.writeFully(buffer.getBuffer(0), buffer.getDataSize(0), writeTimeout);
    std::string payload;
    payload.resize(1);
    payload[0] = 0;

    sendSaslMessage(sock, DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS,
        payload, "", writeTimeout, false);
    DataTransferEncryptorMessageProto msg;
    readSaslMessage(sock, writeTimeout*10, msg, datanode);
    if (saslClient)
        delete saslClient;

    RpcSaslProto_SaslAuth auth;
    auth.set_method("TOKEN");
    auth.set_mechanism("DIGEST-MD5");
    std::string temp;
    Token ourToken;
    ourToken.setIdentifier(blockToken.getIdentifier());
    ourToken.setKind(blockToken.getKind());
    ourToken.setPassword(blockToken.getPassword());
    ourToken.setService(blockToken.getService());

    if (isSecure && theKey.getNonce().length() == 0)
        isSecure = false;

    if (isSecure) {
        char temp[100];
        std::string nonce = theKey.getNonce();
        std::string user;
        sprintf(temp, "%d", theKey.getKeyId());
        user = temp;
        user += " ";
        user += theKey.getBlockPoolId();
        user += " ";
        user += Base64Encode(nonce);
        ourToken.setIdentifier(user);
        ourToken.setPassword(theKey.getEncryptionKey());
    }

    temp = "0";
    auth.set_serverid(temp);
    temp = "hdfs";
    auth.set_protocol(temp);
    saslClient = new SaslClient(auth, ourToken, "", isSecure, protection);
    std::string token = saslClient->evaluateChallenge(msg.payload());
    sendSaslMessage(sock, DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS,
        token, "", writeTimeout, isSecure);
    readSaslMessage(sock, writeTimeout*10, msg, datanode);



    token = saslClient->evaluateChallenge(msg.payload());
    if (token.length() != 0) {
        THROW(HdfsRpcException,
              "DataNode to \"%s\" got protocol mismatch: got error evaluating challenge.",
              datanode.c_str())
    }

    if (msg.cipheroption().size()) {
        CipherOptionProto cipher = msg.cipheroption().Get(0);
        std::string inKey = cipher.inkey();
        std::string inIv = cipher.iniv();
        std::string outKey = cipher.outkey();
        std::string outIv = cipher.outiv();

        inKey = saslClient->decode(inKey.c_str(), inKey.length());
        outKey = saslClient->decode(outKey.c_str(), outKey.length());

        AESClient *aes = new AESClient(inKey, inIv, outKey, outIv, cryptoBufferSize);
        saslClient->setAes(aes);
    }
    saslComplete = true;

}
}
}

