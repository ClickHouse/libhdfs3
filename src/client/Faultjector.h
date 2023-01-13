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
#ifndef _HDFS_LIBHDFS3_CLIENT_FAULTJECTOR_H_
#define _HDFS_LIBHDFS3_CLIENT_FAULTJECTOR_H_

#include <mutex>

namespace Hdfs {

namespace Internal {
class FaultInjector {
public:
    ~FaultInjector() {};
    FaultInjector(const FaultInjector &) = delete;
    FaultInjector & operator=(const FaultInjector &) = delete;

    static FaultInjector & get() {
        static FaultInjector instance;
        return instance;
    }

private:
    FaultInjector() {};

private:
    int32_t badNodesForRead = 0;
    int64_t writeKillPos = 0;
    int64_t ackKillPos = 0;
    bool testWriteKill = false;
    bool testAckKill = false;
    bool closePipeline = false;
    bool createOutputStreamFailed = false;
    std::mutex mtx;

public:
    void setBadNodesForRead(int32_t val) {
        badNodesForRead = val;
    }

    bool testBadReader() {
        if (badNodesForRead > 0) {
            badNodesForRead--;
            return true;
        }
        return false;
    }

    void setWriteKillPos(int64_t pos) {
        writeKillPos = pos;
        testWriteKill = true;
    }

    bool testBadWriterAtKillPos(int64_t pos) {
        if (testWriteKill && pos >= writeKillPos) {
            testWriteKill = false;
            return true;
        }
        return false;
    }

    void setAckKillPos(int64_t pos) {
        ackKillPos = pos;
        testAckKill = true;
    }

    bool testBadWriterAtAckPos(int64_t pos) {
        std::lock_guard<std::mutex> lock(mtx);
        if (testAckKill && pos >= ackKillPos) {
            testAckKill = false;
            return true;
        }
        return false;
    }

    void setPipelineClose(bool val) {
        closePipeline = val;
    }

    bool testPipelineClose() {
        if (closePipeline) {
            closePipeline = false;
            return true;
        }
        return false;
    }

    void setCreateOutputStreamFailed(bool val) {
        createOutputStreamFailed = val;
    }

    bool testCreateOutputStreamFailed() {
        if (createOutputStreamFailed) {
            createOutputStreamFailed = false;
            return true;
        }
        return false;
    }

};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_FAULTJECTOR_H_ */