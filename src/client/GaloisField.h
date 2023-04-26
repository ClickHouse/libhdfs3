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
#include "Memory.h"

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
    static shared_ptr<GaloisField> getInstance();
    static shared_ptr<GaloisField> getInstance(int fieldSize, int primitivePolynomial);

    int getFieldSize() const;

private:
    GaloisField() = default;
    explicit GaloisField(int _fieldSize);

    // Field size 256 is good for byte based system
    static const int DEFAULT_FIELD_SIZE = 256;
    // primitive polynomial 1 + X^2 + X^3 + X^4 + X^8 (substitute 2)
    static const int DEFAULT_PRIMITIVE_POLYNOMIAL = 285;
    static std::map<int, shared_ptr<GaloisField>> instances;
    static std::mutex singleton_mtx;

    int fieldSize;
};


}
}

#endif /* _HDFS_LIBHDFS3_GALOIS_FIELD_H_ */
