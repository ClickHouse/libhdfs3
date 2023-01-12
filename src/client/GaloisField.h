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

#ifndef _HDFS_LIBHDFS3_GALOIS_FIELD_H_
#define _HDFS_LIBHDFS3_GALOIS_FIELD_H_

#include "ByteBuffer.h"

#include <vector>
#include <memory>
#include <map>
#include <algorithm>
#include <utility>
#include <mutex>


namespace Hdfs {
namespace Internal {

class GaloisField {
public:
    static std::shared_ptr<GaloisField> getInstance();
    static std::shared_ptr<GaloisField> getInstance(int fieldSize, int primitivePolynomial);

    GaloisField();
    GaloisField(int fieldSize, int primitivePolynomial);

    int add(int x, int y);
    std::vector<int> add(std::vector<int> & p, std::vector<int> & q);
    int getFieldSize() const;
    int getPrimitivePolynomial() const;
    int multiply(int x, int y);
    std::vector<int> multiply(std::vector<int> & p, std::vector<int> & q);
    int power(int x, int n);
    void solveVandermondeSystem(std::vector<int> & x, std::vector< std::vector<int8_t> > & y, std::vector<int> & outputOffsets, int len, int dataLen);
    void solveVandermondeSystem(std::vector<int> & x, std::vector<int> & y);
    void solveVandermondeSystem(std::vector<int> & x, std::vector<int> & y, int len);
    void substitute(std::vector< std::vector<int8_t> > & p, std::vector<int8_t> &q, int x);
    void substitute(std::vector< std::vector<int8_t> > &p, std::vector<int> &offsets, int len, std::vector<int8_t> &q, int offset, int x);
    void substitute(std::vector<ByteBuffer> &p, int len, ByteBuffer &q, int x);
    int substitute(std::vector<int> &p, int x);

private:
    // Field size 256 is good for byte based system
    static const int DEFAULT_FIELD_SIZE = 256;
    // primitive polynomial 1 + X^2 + X^3 + X^4 + X^8 (substitute 2)
    static const int DEFAULT_PRIMITIVE_POLYNOMIAL = 285;
    static std::map<int, std::shared_ptr<GaloisField>> instances;
    static std::mutex singleton_mtx;

    std::vector<int> logTable;
    std::vector<int> powTable;
    std::vector < std::vector<int> > mulTable;
    std::vector < std::vector<int> > divTable;
    int fieldSize;
    int primitivePeriod;
    int primitivePolynomial;
    
};


}
}

#endif /* _HDFS_LIBHDFS3_GALOIS_FIELD_H_ */
