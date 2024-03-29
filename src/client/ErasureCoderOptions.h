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

#ifndef _HDFS_LIBHDFS3_ERASURE_CODER_OPTIONS_H_
#define _HDFS_LIBHDFS3_ERASURE_CODER_OPTIONS_H_

#include <iostream>
#include <string>
#include <vector>

namespace Hdfs {
namespace Internal {

class ErasureCoderOptions {
public:
    ErasureCoderOptions(int _numDataUnits, int _numParityUnits);
    ErasureCoderOptions(int _numDataUnits, int _numParityUnits, bool _allowVerboseDump);
    int getNumDataUnits() const;
    int getNumParityUnits() const;
    int getNumAllUnits() const;
    bool isAllowVerboseDump() const;

public:
    int numDataUnits;
    int numParityUnits;
    int numAllUnits;
    bool allowVerboseDump;
};

}
}

#endif /* _HDFS_LIBHDFS3_ERASURE_CODER_OPTIONS_H_ */
