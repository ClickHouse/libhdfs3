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

#ifndef _HDFS_LIBHDFS3_RS_UTIL_H_
#define _HDFS_LIBHDFS3_RS_UTIL_H_

#include "ByteBuffer.h"
#include "GaloisField.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace Hdfs {
namespace Internal {

class RSUtil {
public:
    RSUtil();

    static void encodeData(std::vector<int8_t> & gfTables,
                           std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                           std::vector<std::shared_ptr<ByteBuffer>> & outputs);

    static void initTables(int k, int rows, std::vector<int8_t> & codingMatrix,
                           int matrixOffset, std::vector<int8_t> & gfTables);
    static void genCauchyMatrix(std::vector<int8_t> & a, int m, int k);

    static std::shared_ptr<GaloisField> getGF() {
        return GaloisField::getInstance();
    }

public:
    static const int PRIMITIVE_ROOT = 2;   

};

}
}

#endif /* _HDFS_LIBHDFS3_RS_UTIL_H_ */