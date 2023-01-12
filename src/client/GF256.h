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

#ifndef _HDFS_LIBHDFS3_GF256_H_
#define _HDFS_LIBHDFS3_GF256_H_

#include <iostream>
#include <string>
#include <vector>

namespace Hdfs {
namespace Internal {

class GF256 {
    class GfMulTab {
    public:
        GfMulTab() {
            innerGfMulTab = std::vector<std::vector<int8_t>>(256);
            for (int i = 0; i < 256; i++) {
                innerGfMulTab[i].resize(256);
                for (int j = 0; j < 256; j++) {
                    innerGfMulTab[i][j] = GF256::gfMul((int8_t) i, (int8_t) j);
                }
            }
        }
        std::vector<std::vector<int8_t>> & getInnerGfMulTab() {
            return innerGfMulTab;
        }
    private:
        std::vector<std::vector<int8_t>> innerGfMulTab;
    };

public:
    GF256(/* args */);
    static int8_t gfInv(int8_t a);
    static void gfInvertMatrix(std::vector<int8_t> & inMatrix, std::vector<int8_t> & outMatrix, int n);
    static int8_t gfMul(int8_t a, int8_t b);
    static void gfVectMulInit(int8_t c, std::vector<int8_t> & tbl, int offset);
    static void initTheGfMulTab(std::vector<std::vector<int8_t>> & theGfMulTab);

    static std::vector<std::vector<int8_t>> & getInstance();
    static std::vector<int8_t> GF_BASE;
    static std::vector<int8_t> GF_LOG_BASE;

};

}
}

#endif /* _HDFS_LIBHDFS3_GF256_H_ */
