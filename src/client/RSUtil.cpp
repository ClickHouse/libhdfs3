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
#include <iostream>

using namespace Hdfs;
using namespace Hdfs::Internal;
namespace Hdfs {
namespace Internal {

void RSUtil::initTables(int k, int rows, std::vector<int8_t> & codingMatrix,
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
            a[pos++] = GF256::gfInv((int8_t) (i ^ j));
        }
    } 
}

void RSUtil::encodeData(std::vector<int8_t> & gfTables,
                        std::vector<std::shared_ptr<ByteBuffer>> & inputs,
                        std::vector<std::shared_ptr<ByteBuffer>> & outputs) {
    int numInputs = inputs.size();
    int numOutputs = outputs.size();
    int dataLen = inputs[0]->remaining();
    int l, i, j, iPos, oPos;
    ByteBuffer *input, *output;
    int8_t s;
    const int times = dataLen / 8;
    const int extra = dataLen - dataLen % 8;
    std::vector<int8_t> tableLine;
    std::vector< std::vector<int8_t> > & gf256_multable = GF256::getInstance();
    for (l = 0; l < numOutputs; l++) {
        output = outputs[l].get();

        for (j = 0; j < numInputs; j++) {
            input = inputs[j].get();
            iPos = input->position();
            oPos = output->position();
            int mark = output->position();

            s = gfTables[j * 32 + l * numInputs * 32 + 1];
            std::vector<int8_t> & tableLine = gf256_multable[s & 0xff];

            for (i = 0; i < times; i++, iPos += 8, oPos += 8) {
                output->putInt8_t(output->getInt8_t(oPos + 0) ^
                    tableLine[0xff & input->getInt8_t(iPos + 0)], oPos + 0);
                output->putInt8_t(output->getInt8_t(oPos + 1) ^
                    tableLine[0xff & input->getInt8_t(iPos + 1)], oPos + 1);
                output->putInt8_t(output->getInt8_t(oPos + 2) ^
                    tableLine[0xff & input->getInt8_t(iPos + 2)], oPos + 2);
                output->putInt8_t(output->getInt8_t(oPos + 3) ^
                    tableLine[0xff & input->getInt8_t(iPos + 3)], oPos + 3);
                output->putInt8_t(output->getInt8_t(oPos + 4) ^
                    tableLine[0xff & input->getInt8_t(iPos + 4)], oPos + 4);
                output->putInt8_t(output->getInt8_t(oPos + 5) ^
                    tableLine[0xff & input->getInt8_t(iPos + 5)], oPos + 5);
                output->putInt8_t(output->getInt8_t(oPos + 6) ^
                    tableLine[0xff & input->getInt8_t(iPos + 6)], oPos + 6);
                output->putInt8_t(output->getInt8_t(oPos + 7) ^
                    tableLine[0xff & input->getInt8_t(iPos + 7)], oPos + 7);
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