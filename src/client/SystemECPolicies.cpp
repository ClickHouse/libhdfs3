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

#include "Logger.h"
#include "SystemECPolicies.h"

namespace Hdfs {

namespace Internal {

std::shared_mutex SystemECPolicies::mutex;

SystemECPolicies::SystemECPolicies() {
    replicationPolicy = new ECPolicy(REPLICATION, cellsize, 1, 2, "REPLICATION");
    sysPolicy1 = new ECPolicy(RS_6_3, cellsize, 6, 3, "RS");
    sysPolicy2 = new ECPolicy(RS_3_2, cellsize, 3, 2, "RS");
    sysPolicy3 = new ECPolicy(RS_6_3_LEGACY, cellsize, 6, 3, "RS-LEGACY");
    sysPolicy4 = new ECPolicy(XOR_2_1, cellsize, 2, 1, "XOR");
    sysPolicy5 = new ECPolicy(RS_10_4, cellsize, 10, 4, "RS");
    sysPolicies = {replicationPolicy, sysPolicy1, sysPolicy2, 
                   sysPolicy3, sysPolicy4, sysPolicy5 };
    if (maps.empty()) {
        for (int i = 0; i < (int)sysPolicies.size(); ++i) {
            LOG(DEBUG1, "ecpolicy name=%s\n", sysPolicies[i]->getName());
            maps.insert(std::make_pair(sysPolicies[i]->getId(), sysPolicies[i]));
        }
    }
}

SystemECPolicies::~SystemECPolicies() {
    for (int i = 0; i < (int)sysPolicies.size(); ++i) {
        LOG(DEBUG1, "ecpolicy name=%s\n", sysPolicies[i]->getName());
        delete sysPolicies[i];
    }
}

ECPolicy * SystemECPolicies::getById(int8_t id) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (maps.count(id) > 0) {
        return maps[id];
    }
    return nullptr;
}

void SystemECPolicies::addEcPolicy(int8_t id, ECPolicy * ecPolicy) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    sysPolicies.push_back(ecPolicy);
    maps.insert(std::make_pair(id, ecPolicy));
}

}
}
