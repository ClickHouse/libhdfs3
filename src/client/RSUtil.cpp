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

#include "GF256.h"
#include "RSUtil.h"

#include <algorithm>

namespace Hdfs {
namespace Internal {

void RSUtil::initTables(int k, int rows, const std::vector<int8_t> & codingMatrix,
                        int matrixOffset, std::vector<int8_t> & gfTables) {
    int i, j;

    int offset = 0, idx = matrixOffset;
    for (i = 0; i < rows; i++) {
        for (j = 0; j < k; j++) {
            GF256::gfVectMulInit(codingMatrix[idx++], gfTables, offset);
            offset += 32;
        }
    }
}    

/**
 * Ported from Intel ISA-L library.
 */
void RSUtil::genCauchyMatrix(std::vector<int8_t> & a, int m, int k) {
    // Identity matrix in high position
    for (int i = 0; i < k; i++) {
        a[k * i + i] = 1;
    }

    // For the rest choose 1/(i + j) | i != j
    int pos = k * k;
    for (int i = k; i < m; i++) {
        for (int j = 0; j < k; j++) {
            a[pos++] = GF256::gfInv(static_cast<int8_t>(i ^ j));
        }
    } 
}

void RSUtil::encodeData(const std::vector<int8_t> & gfTables,
                        const std::vector<shared_ptr<ByteBuffer>> & inputs,
                        const std::vector<shared_ptr<ByteBuffer>> & outputs) {
    int numInputs = static_cast<int>(inputs.size());
    int numOutputs = static_cast<int>(outputs.size());
    int dataLen = static_cast<int>(inputs[0]->remaining());
    int l, i, j, k, iPos, oPos;
    unsigned long long t1, t2, t3, t4, * iPosInULL, * oPosInULL;
    shared_ptr<ByteBuffer> input, output;
    int8_t s;
    const int times = dataLen / 8;
    const int extra = dataLen - dataLen % 8;
    const std::vector< std::vector<int8_t> > & gf256_multable = GF256::getInstance();
    for (l = 0; l < numOutputs; l++) {
        output = outputs[l];

        for (j = 0; j < numInputs; j++) {
            input = inputs[j];
            iPos = static_cast<int>(input->position());
            oPos = static_cast<int>(output->position());
            int mark = static_cast<int>(output->position());

            s = gfTables[j * 32 + l * numInputs * 32 + 1];
            const std::vector<int8_t> & tableLine = gf256_multable[s & 0xff];

            for (i = 0; i < times; i++, iPos += 8, oPos += 8) {
                iPosInULL = (unsigned long long *)(&input->getBuffer()[iPos]);
                oPosInULL = (unsigned long long *)(&output->getBuffer()[oPos]);
                t1 = *iPosInULL;
                t2 = *oPosInULL;
                t3 = 0;
                t4 = 0;
                t3 |= static_cast<unsigned long long>(static_cast<unsigned char>(t2 ^ tableLine[0xff & t1]));

                for (k = 1; k < 8; k++) {
                    t1 >>= 8;
                    t2 >>= 8;
                    t4 += 8;
                    t3 |= static_cast<unsigned long long>(static_cast<unsigned char>(t2 ^ tableLine[0xff & t1])) << t4;
                }
                *oPosInULL = t3;
            }

            for (i = extra; i < dataLen; i++, iPos++, oPos++) {
                output->putInt8_t(output->getInt8_t(oPos) ^
                    tableLine[0xff & input->getInt8_t(iPos)], oPos);
            }
            output->position(mark);
        }
    }
}

}
}
