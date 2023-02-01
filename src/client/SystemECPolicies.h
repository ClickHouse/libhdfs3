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
#ifndef _HDFS_LIBHFDS3_CLIENT_SYSTEM_EC_POLICIES_H_
#define _HDFS_LIBHFDS3_CLIENT_SYSTEM_EC_POLICIES_H_

#include <map>
#include <vector>
#include <shared_mutex>
#include <boost/noncopyable.hpp>

#include "Memory.h"
#include "ECPolicy.h"

enum PolicyId {
    REPLICATION = 0,
    RS_6_3 = 1,
    RS_3_2 = 2,
    RS_6_3_LEGACY = 3,
    XOR_2_1 = 4,
    RS_10_4 = 5
};

namespace Hdfs {

namespace Internal {

class SystemECPolicies: public boost::noncopyable {
public:
    static SystemECPolicies & getInstance() {
        static SystemECPolicies instance;
        return instance;
    }

    /**
     * get system ec policy by id.
     * @param id The ec policy id.
     */
    shared_ptr<ECPolicy> getById(int8_t id);

    /**
     * add a new ec policy.
     * @param id The ec policy id.
     * @param ecPolicy The new ec policy.
     */
    void addEcPolicy(int8_t id, shared_ptr<ECPolicy> ecPolicy);

private:
    SystemECPolicies();
    ~SystemECPolicies() = default;

private:
    static std::shared_mutex mutex;

    // 1MB
    const int32_t cellsize = 1024 * 1024;
    
    std::vector<shared_ptr<ECPolicy>> sysPolicies;
    std::map<int8_t, shared_ptr<ECPolicy>> maps;
};

}
}

#endif /* _HDFS_LIBHFDS3_CLIENT_SYSTEM_EC_POLICIES_H_ */
