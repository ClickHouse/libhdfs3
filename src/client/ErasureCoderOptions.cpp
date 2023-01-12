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

#include "ErasureCoderOptions.h"

using namespace Hdfs;
using namespace Hdfs::Internal;
using namespace std;

namespace Hdfs {
namespace Internal {

ErasureCoderOptions::ErasureCoderOptions(int numDataUnits, int numParityUnits) :
        AllowChangeInputs(false), AllowVerboseDump(false) {
    this->numDataUnits = numDataUnits;
    this->numParityUnits = numParityUnits;
    this->numAllUnits = numDataUnits + numParityUnits;
}

ErasureCoderOptions::ErasureCoderOptions(int numDataUnits, int numParityUnits,
                                         bool allowChangeInputs, bool allowVerboseDump) {
    this->numDataUnits = numDataUnits;
    this->numParityUnits = numParityUnits;
    this->numAllUnits = numDataUnits + numParityUnits;
    this->AllowChangeInputs = allowChangeInputs;
    this->AllowVerboseDump = allowVerboseDump;
}

/**
 * The number of data input units for the coding. A unit can be a byte,
 * chunk or buffer or even a block.
 * @return count of data input units
 */
int ErasureCoderOptions::getNumDataUnits() {
    return numDataUnits;
}

/**
 * The number of parity output units for the coding. A unit can be a byte,
 * chunk, buffer or even a block.
 * @return count of parity output units
 */
int ErasureCoderOptions::getNumParityUnits() {
    return numParityUnits;
}


/**
 * The number of all the involved units in the coding.
 * @return count of all the data units and parity units
 */
int ErasureCoderOptions::getNumAllUnits() {
    return numAllUnits;
}

/**
 * Allow changing input buffer content (not positions). Maybe better
 * performance if not allowed.
 * @return true if allowing input content to be changed, false otherwise
 */
bool ErasureCoderOptions::allowChangeInputs() {
    return this->AllowChangeInputs;
}

/**
 * Allow dump verbose debug info or not.
 * @return true if verbose debug info is desired, false otherwise
 */
bool ErasureCoderOptions::allowVerboseDump() {
    return this->AllowVerboseDump;
}

}
}