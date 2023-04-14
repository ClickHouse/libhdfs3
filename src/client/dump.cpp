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

#include <string>

#include "erasure_coder.h"
#include "Logger.h"

namespace Hdfs {
namespace Internal {

std::string dumpCodingMatrix(unsigned char * buf, int n1, int n2) {
    std::string res = "[";
    int i, j;
    for (i = 0; i < n1; i++) {
        res += "[";
        for (j = 0; j < n2; j++) {
            if (j > 0) res += " ";
            res += std::to_string(0xff & buf[j + (i * n2)]);
        }
        res += "]";
    }
    res += "]";
    return res;
}

void dumpEncoder(IsalEncoder * pCoder) {
    int numDataUnits = pCoder->coder.numDataUnits;
    int numParityUnits = pCoder->coder.numParityUnits;
    int numAllUnits = pCoder->coder.numAllUnits;

    std::string encodeMatrixStr = dumpCodingMatrix((unsigned char *) pCoder->encodeMatrix,
                                                   numDataUnits, numAllUnits).c_str();

    LOG(INFO, "Encoding (numAllUnits = %d, numParityUnits = %d, numDataUnits = %d), EncodeMatrix = %s",
        numAllUnits, numParityUnits, numDataUnits, encodeMatrixStr.c_str());
}

void dumpDecoder(IsalDecoder * pCoder) {
    int i, j;
    int numDataUnits = pCoder->coder.numDataUnits;
    int numAllUnits = pCoder->coder.numAllUnits;

    std::string erasedIndexesStr = "[";
    for (j = 0; j < pCoder->numErased; j++) {
        if (j > 0) erasedIndexesStr += " ";
        erasedIndexesStr += std::to_string(pCoder->erasedIndexes[j]);
    }
    erasedIndexesStr += "]";

    std::string decodeIndexesStr = "[";
    for (i = 0; i < numDataUnits; i++) {
        if (i > 0) decodeIndexesStr += " ";
        decodeIndexesStr += std::to_string(pCoder->decodeIndex[i]);
    }
    decodeIndexesStr += "]";

    std::string EncodeMatrixStr = dumpCodingMatrix((unsigned char *) pCoder->encodeMatrix,
                                                   numDataUnits, numAllUnits);

    std::string InvertMatrixStr = dumpCodingMatrix((unsigned char *) pCoder->invertMatrix,
                                                   numDataUnits, numAllUnits);

    std::string DecodeMatrixStr = dumpCodingMatrix((unsigned char *) pCoder->decodeMatrix,
                                                   numDataUnits, numAllUnits);

    LOG(INFO, "Recovering (numAllUnits = %d, numDataUnits = %d, numErased = %d), ErasedIndexes = %s, "
              "DecodeIndexes = %s, EncodeMatrix = %s, InvertMatrix = %s, DecodeMatrix = %s",
        numAllUnits, numDataUnits, pCoder->numErased, erasedIndexesStr.c_str(), decodeIndexesStr.c_str(),
        EncodeMatrixStr.c_str(), InvertMatrixStr.c_str(), DecodeMatrixStr.c_str());
}

}
}
