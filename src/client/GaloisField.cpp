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

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

std::mutex GaloisField::singleton_mtx;
std::map<int, std::shared_ptr<GaloisField>> GaloisField::instances;

GaloisField::GaloisField(int fieldSize, int primitivePolynomial) {
    this->fieldSize = fieldSize;
    this->primitivePeriod = fieldSize - 1;
    this->primitivePolynomial = primitivePolynomial;
    std::vector<int> logTable(fieldSize);
    std::vector<int> powTable(fieldSize);
    std::vector<std::vector<int>> mulTable(fieldSize);
    std::vector<std::vector<int>> divTable(fieldSize);
    
    int value = 1;
    for (int pow = 0; pow < fieldSize - 1; pow++) {
        powTable[pow] = value;
        logTable[value] = pow;
        value = value * 2;
        if (value >= fieldSize) {
            value = value ^ primitivePolynomial;
        }
    }
    // building multiplication table
    for (int i = 0; i < fieldSize; i++) {
        mulTable[i].resize(fieldSize);
        for (int j = 0; j < fieldSize; j++) {
            if (i == 0 || j == 0) {
                mulTable[i][j] = 0;
                continue;
            }
            int z = logTable[i] + logTable[j];
            z = z >= primitivePeriod ? z - primitivePeriod : z;
            z = powTable[z];
            mulTable[i][j] = z;
        }
    }
    // building division table
    for (int i = 0; i < fieldSize; i++) {
        divTable[i].resize(fieldSize);
        for (int j = 1; j < fieldSize; j++) {
            if (i == 0) {
                divTable[i][j] = 0;
                continue;
            }
            int z = logTable[i] - logTable[j];
            z = z < 0 ? z + primitivePeriod : z;
            z = powTable[z];
            divTable[i][j] = z;
        }
    }
}

int GaloisField::add(int x, int y) {
  return x ^ y;
}

std::vector<int> GaloisField::add(std::vector<int> & p, std::vector<int> & q) {
    int len = std::max(p.size(), q.size());
    std::vector<int> result(len);
    for (int i = 0; i < len; i++) {
        if (i < (int)p.size() && i < (int)q.size()) {
            result[i] = add(p[i], q[i]);
        } else if (i < (int)p.size()) {
            result[i] = p[i];
        } else {
            result[i] = q[i];
        }
    }
    return result;
}

int GaloisField::getFieldSize() const {
    return fieldSize;
}

/**
 * Get the object performs Galois field arithmetic with default setting
 */
std::shared_ptr<GaloisField> GaloisField::getInstance() {
    return getInstance(DEFAULT_FIELD_SIZE, DEFAULT_PRIMITIVE_POLYNOMIAL);
}

/**
   * Get the object performs Galois field arithmetics
   *
   * @param fieldSize           size of the field
   * @param primitivePolynomial a primitive polynomial corresponds to the size
*/
std::shared_ptr<GaloisField> GaloisField::getInstance(int fieldSize, int primitivePolynomial) {
    int key = ((fieldSize << 16) & 0xFFFF0000) + (primitivePolynomial & 0x0000FFFF);
    std::lock_guard<std::mutex> lk(singleton_mtx);
    std::map<int, std::shared_ptr<GaloisField>>::iterator it;
    it = instances.find(key);
    if (it != instances.end()) {
        return it->second;
    } else {
        auto res = std::make_shared<GaloisField>(fieldSize, primitivePolynomial);
        instances.insert(std::pair<int, std::shared_ptr<GaloisField>>(key, res));
        return res;
    }
}

/**
 * Return the primitive polynomial in GF(2)
 *
 * @return primitive polynomial as a integer
 */
int GaloisField::getPrimitivePolynomial() const {
    return primitivePolynomial;
}

/**
 * Compute the multiplication of two fields
 *
 * @param x input field
 * @param y input field
 * @return result of multiplication
 */
int GaloisField::multiply(int x, int y) {
    return mulTable[x][y];
}

/**
 * Compute the multiplication of two polynomials. The index in the array
 * corresponds to the power of the entry. For example p[0] is the constant
 * term of the polynomial p.
 *
 * @param p input polynomial
 * @param q input polynomial
 * @return polynomial represents p*q
 */
std::vector<int> GaloisField::multiply(std::vector<int> & p, std::vector<int> & q) {
    int len = p.size() + q.size() - 1;
    std::vector<int> result(len);
    for (int i = 0; i < len; i++) {
        result[i] = 0;
    }
    for (int i = 0; i < (int)p.size(); i++) {
        for (int j = 0; j < (int)q.size(); j++) {
            result[i + j] = add(result[i + j], multiply(p[i], q[j]));
        }
    }
    return result;
}

/**
 * Compute power n of a field
 *
 * @param x input field
 * @param n power
 * @return x^n
 */
int GaloisField::power(int x, int n) {
    if (n == 0) {
        return 1;
    }
    if (x == 0) {
        return 0;
    }
    x = logTable[x] * n;
    if (x < primitivePeriod) {
        return powTable[x];
    }
    x = x % primitivePeriod;
    return powTable[x];
}

/**
 * A "bulk" version to the solving of Vandermonde System
 */
void GaloisField::solveVandermondeSystem(std::vector<int> &x, std::vector< std::vector<int8_t> > &y,
                                         std::vector<int> &outputOffsets, int len, int dataLen) {
    int idx1, idx2;
    for (int i = 0; i < len - 1; i++) {
        for (int j = len - 1; j > i; j--) {
            for (idx2 = outputOffsets[j-1], idx1 = outputOffsets[j];
                idx1 < outputOffsets[j] + dataLen; idx1++, idx2++) {
                y[j][idx1] = (int8_t) (y[j][idx1] ^ mulTable[x[i]][y[j - 1][idx2] & 0x000000FF]);
            }
        }
    }
    for (int i = len - 1; i >= 0; i--) {
        for (int j = i + 1; j < len; j++) {
            for (idx1 = outputOffsets[j]; idx1 < outputOffsets[j] + dataLen; idx1++) {
                y[j][idx1] = (int8_t) (divTable[y[j][idx1] & 0x000000FF][x[j] ^ x[j - i - 1]]);
            }
        }
        for (int j = i; j < len - 1; j++) {
            for (idx2 = outputOffsets[j+1], idx1 = outputOffsets[j];
                idx1 < outputOffsets[j] + dataLen; idx1++, idx2++) {
                y[j][idx1] = (int8_t) (y[j][idx1] ^ y[j + 1][idx2]);
            }
        }
    }
}

/**
 * Given a Vandermonde matrix V[i][j]=x[j]^i and std::vector y, solve for z such
 * that Vz=y. The output z will be placed in y.
 *
 * @param x the std::vector which describe the Vandermonde matrix
 * @param y right-hand side of the Vandermonde system equation. will be
 *          replaced the output in this std::vector
 */
void GaloisField::solveVandermondeSystem(std::vector<int> &x, std::vector<int> &y) {
    solveVandermondeSystem(x, y, x.size());
}

/**
 * Given a Vandermonde matrix V[i][j]=x[j]^i and std::vector y, solve for z such
 * that Vz=y. The output z will be placed in y.
 *
 * @param x   the std::vector which describe the Vandermonde matrix
 * @param y   right-hand side of the Vandermonde system equation. will be
 *            replaced the output in this std::vector
 * @param len consider x and y only from 0...len-1
 */
void GaloisField::solveVandermondeSystem(std::vector<int> &x, std::vector<int> &y, int len) {
    for (int i = 0; i < len - 1; i++) {
        for (int j = len - 1; j > i; j--) {
            y[j] = y[j] ^ mulTable[x[i]][y[j - 1]];
        }
    }
    for (int i = len - 1; i >= 0; i--) {
        for (int j = i + 1; j < len; j++) {
            y[j] = divTable[y[j]][x[j] ^ x[j - i - 1]];
        }
        for (int j = i; j < len - 1; j++) {
            y[j] = y[j] ^ y[j + 1];
        }
    }
}

/**
 * A "bulk" version of the substitute.
 * Tends to be 2X faster than the "int" substitute in a loop.
 *
 * @param p input polynomial
 * @param q store the return result
 * @param x input field
 */
void GaloisField::substitute(std::vector< std::vector<int8_t> > &p, std::vector<int8_t> &q, int x) {
    int y = 1;
    for (int i = 0; i < (int)p.size(); i++) {
        std::vector<int8_t> pi = p[i];
        for (int j = 0; j < (int)pi.size(); j++) {
            int pij = pi[j] & 0x000000FF;
            q[j] = (int8_t)(q[j] ^ mulTable[pij][y]);
        }
        y = mulTable[x][y];
    }
}

void GaloisField::substitute(std::vector< std::vector<int8_t> > &p, std::vector<int> &offsets, int len,
                             std::vector<int8_t> &q, int offset, int x) {
    int y = 1, iIdx, oIdx;
    for (int i = 0; i < (int)p.size(); i++) {
        std::vector<int8_t> * pi = &p[i];
        for (iIdx = offsets[i], oIdx = offset; iIdx < offsets[i] + len; iIdx++, oIdx++) {
            int pij = pi != NULL ? (*pi)[iIdx] & 0x000000FF : 0;
            q[oIdx] = (int8_t) (q[oIdx] ^ mulTable[pij][y]);
        }
        y = mulTable[x][y];
    }
}

void GaloisField::substitute(std::vector<ByteBuffer> &p, int len, ByteBuffer &q, int x) {
    int y = 1, iIdx, oIdx;
    for (int i = 0; i < (int)p.size(); i++) {
        ByteBuffer* pi = &p[i];
        int pos = pi != NULL ? pi->position() : 0;
        int limit = pi != NULL ? pi->limit() : len;
        for (oIdx = q.position(), iIdx = pos; iIdx < limit; iIdx++, oIdx++) {
            int pij = pi != NULL ? pi->get(iIdx) & 0x000000FF : 0;
            q.put((int8_t)(q.get(oIdx) ^ mulTable[pij][y]), oIdx);
        }
        y = mulTable[x][y];
    }
}

/**
 * Substitute x into polynomial p(x).
 *
 * @param p input polynomial
 * @param x input field
 * @return p(x)
 */
int GaloisField::substitute(std::vector<int> &p, int x) {
    int result = 0;
    int y = 1;
    for (int i = 0; i < (int)p.size(); i++) {
        result = result ^ mulTable[p[i]][y];
        y = mulTable[x][y];
    }
    return result;
}


}
}