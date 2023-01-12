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
#include <Memory.h>
#include <shared_mutex>

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

class SystemECPolicies {
public:
    ~SystemECPolicies();
    SystemECPolicies(const SystemECPolicies &) = delete;
    SystemECPolicies & operator=(const SystemECPolicies &) = delete;

    static SystemECPolicies & getInstance() {
        static SystemECPolicies instance;
        return instance;
    }

    /**
     * get system ec policy by id.
     * @param id The ec policy id.
     */
    ECPolicy * getById(int8_t id);

    /**
     * add a new ec policy.
     * @param id The ec policy id.
     * @param ecPolicy The new ec policy.
     */
    void addEcPolicy(int8_t id, ECPolicy * ecPolicy);

private:
    SystemECPolicies();

private:
    static std::shared_mutex mutex;

    // 1MB
    const int32_t cellsize = 1024 * 1024;
    
    // system ec policy
    ECPolicy * replicationPolicy;
    ECPolicy * sysPolicy1;
    ECPolicy * sysPolicy2;
    ECPolicy * sysPolicy3;
    ECPolicy * sysPolicy4;
    ECPolicy * sysPolicy5;
    
    std::vector<ECPolicy *> sysPolicies;
    std::map<int8_t, ECPolicy *> maps;
};

}
}

#endif /* _HDFS_LIBHFDS3_CLIENT_SYSTEM_EC_POLICIES_H_ */
