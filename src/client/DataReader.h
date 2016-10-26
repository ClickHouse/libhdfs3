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
#ifndef _HDFS_LIBHDFS3_SERVER_DATAREADER_H_
#define _HDFS_LIBHDFS3_SERVER_DATAREADER_H_

#include <string>
#include <vector>
namespace Hdfs {
namespace Internal {

/**
 * Helps read data responses from the server
 */
class DataReader {
public:
    DataReader(DataTransferProtocol *sender,
            shared_ptr<BufferedSocketReader> reader, int readTimeout);
    std::vector<char>& readResponse(const char* text, int &outsize);
    std::vector<char>& readPacketHeader(const char* text, int size, int &outsize);
    std::string& getRest() {
        return rest;
    }

    void setRest(const char* data, int size);
    void reduceRest(int size);
    void getMissing(int size);

private:
    std::string raw;
    std::string decrypted;
    std::string rest;
    std::vector<char> buf;
    DataTransferProtocol *sender;
    shared_ptr<BufferedSocketReader> reader;
    int readTimeout;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_DATAREADER_H_ */
