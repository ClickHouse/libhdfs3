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
#ifndef _HDFS_LIBHFDS3_CLIENT_EC_POLICY_H_
#define _HDFS_LIBHFDS3_CLIENT_EC_POLICY_H_

#include <cctype>
#include <sstream>

namespace Hdfs {

namespace Internal {

class ECPolicy {
public:
    ECPolicy() = default;
    ECPolicy(int8_t id, int32_t cellSize, int32_t dataUnits,
        int32_t parityUnits, const char * codecName) : 
        id(id), cellSize(cellSize), dataUnits(dataUnits), 
        parityUnits(parityUnits), codecName(codecName) {
        name = codecName + 
               std::string("-") + std::to_string(dataUnits) + 
               std::string("-") + std::to_string(parityUnits) + 
               std::string("-") + std::to_string(cellSize / 1024) + "k";
    }

    ~ECPolicy() = default;
    
    const char * getName() const {
        return name.c_str();
    }

    void setName(const char * name_) {
        name = name_;
    }

    int32_t getCellSize() const {
        return cellSize;
    }

    void setCellSize(int32_t cellSize_) {
        cellSize = cellSize_;
    }

    const char * getCodecName() const {
        return codecName.c_str();
    }

    void setCodecName(const char * codecName_) {
        codecName = codecName_;
    }

    int32_t getNumDataUnits() const {
        return dataUnits;
    }

    void setNumDataUnits(int32_t dataUnits_) {
        dataUnits = dataUnits_;
    }

    int32_t getNumParityUnits() const {
        return parityUnits;
    }

    void setNumParityUnits(int32_t parityUnits_) {
        parityUnits = parityUnits_;
    }

    int8_t getId() const {
        return id;
    }

    void setId(int8_t id_) {
        id = id_;
    }

private:
    int8_t id;
    int32_t cellSize;
    int32_t dataUnits;
    int32_t parityUnits;
    std::string name;
    std::string codecName;
};

}
}

#endif /* _HDFS_LIBHFDS3_CLIENT_EC_POLICY_H_ */
