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
#ifndef _HDFS_LIBHDFS3_SERVER_ENCRYPTIONKEY_H_
#define _HDFS_LIBHDFS3_SERVER_ENCRYPTIONKEY_H_

#include <string>
#include <sstream>

namespace Hdfs {
namespace Internal {

/**
 * Identifies an encryption Key
 */
class EncryptionKey {
public:
    EncryptionKey() :
        keyId(0), expiryDate(0) {
    }

    int32_t getKeyId() const {
        return keyId;
    }

    void setKeyId(int32_t keyId) {
        this->keyId = keyId;
    }

    int64_t getExpiryDate() const {
        return expiryDate;
    }

    void setExpiryDate(int64_t expiryDate) {
        this->expiryDate = expiryDate;
    }

    const std::string & getBlockPoolId() const {
        return blockPoolId;
    }

    void setBlockPoolId(const std::string & blockPoolId) {
        this->blockPoolId = blockPoolId;
    }

    const std::string & getNonce() const {
        return nonce;
    }

    void setNonce(const std::string & nonce) {
        this->nonce = nonce;
    }

    const std::string & getEncryptionKey() const {
        return encryptionKey;
    }

    void setEncryptionKey(const std::string & encryptionKey) {
        this->encryptionKey = encryptionKey;
    }

    const std::string & getEncryptionAlgorithm() const {
        return encryptionAlgorithm;
    }

    void setEncryptionAlgorithm(const std::string & encryptionAlgorithm) {
        this->encryptionAlgorithm = encryptionAlgorithm;
    }


private:
    int32_t keyId;
    int64_t expiryDate;
    std::string blockPoolId;
    std::string nonce;
    std::string encryptionKey;
    std::string encryptionAlgorithm;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_ENCRYPTIONKEY_H_ */
