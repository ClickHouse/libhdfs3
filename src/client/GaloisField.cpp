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

#include "GaloisField.h"

namespace Hdfs {
namespace Internal {

std::mutex GaloisField::singleton_mtx;
std::map<int, shared_ptr<GaloisField>> GaloisField::instances;

GaloisField::GaloisField(int _fieldSize) {
    fieldSize = _fieldSize;
}

int GaloisField::getFieldSize() const {
    return fieldSize;
}

/**
 * Get the object performs Galois field arithmetic with default setting
 */
shared_ptr<GaloisField> GaloisField::getInstance() {
    return getInstance(DEFAULT_FIELD_SIZE, DEFAULT_PRIMITIVE_POLYNOMIAL);
}

/**
 * Get the object performs Galois field arithmetics
 *
 * @param fieldSize           size of the field
 * @param primitivePolynomial a primitive polynomial corresponds to the size
 */
shared_ptr<GaloisField> GaloisField::getInstance(int fieldSize, int primitivePolynomial) {
    int key = static_cast<int>(((fieldSize << 16) & 0xFFFF0000) + (primitivePolynomial & 0x0000FFFF));
    std::lock_guard<std::mutex> lk(singleton_mtx);
    std::map<int, shared_ptr<GaloisField>>::iterator it;
    it = instances.find(key);
    if (it != instances.end()) {
        return it->second;
    } else {
        auto res = shared_ptr<GaloisField>(new GaloisField(fieldSize));
        instances.insert(std::pair<int, shared_ptr<GaloisField>>(key, res));
        return res;
    }
}

}
}
