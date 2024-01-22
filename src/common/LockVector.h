/********************************************************************
 * 2024 -
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
#ifndef _HDFS_LIBHDFS3_COMMON_LOCKVECTOR_H_
#define _HDFS_LIBHDFS3_COMMON_LOCKVECTOR_H_

#include <mutex>
#include <vector>

namespace Hdfs {
namespace Internal {

template<typename T>
class LockVector{
    std::mutex mlock;
    std::vector<T> mvec;

public:
    void push_back(const T& value) {
        std::lock_guard<std::mutex> lock(mlock);
        mvec.push_back(value);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mlock);
        mvec.clear();
    }

    void sort() {
        std::lock_guard<std::mutex> lock(mlock);
        std::sort(mvec.begin(), mvec.end());
    }

    bool binary_search(const T &value) {
        std::lock_guard<std::mutex> lock(mlock);
        return std::binary_search(mvec.begin(), mvec.end(), value);
    }

    const std::vector<T>& get(){
        return mvec;
    }

    void set(std::vector<T> &vec) {
        mvec = vec;
    }

};

}
}
#endif //_HDFS_LIBHDFS3_COMMON_LOCKVECTOR_H_
