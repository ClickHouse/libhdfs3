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

#include <iostream>
#include "Preconditions.h"

using namespace Hdfs;
using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

Preconditions::Preconditions() {
}

/**
 * Ensures the truth of an expression involving the state of the calling
 * instance, but not involving any parameters to the calling method.
 *
 * @param expression a boolean expression
 */
void Preconditions::checkState(bool expression) {
    if (!expression){
        THROW(InvalidParameter, "illegal state exception.");
    }   
}

/**
 * Ensures the truth of an expression involving one or more parameters to the
 * calling method.
 *
 * @param expression a boolean expression
 * @throws IllegalArgumentException if {@code expression} is false
 */
void Preconditions::checkArgument(bool expression) {
    if (!expression) {
        THROW(InvalidParameter, "invalid parameter exception.");
    }
}

void Preconditions::checkArgument(bool expression, string errorMessage) {
    if (!expression){
        THROW(InvalidParameter, errorMessage.c_str());
    }
}

void Preconditions::checkNotNull(void* reference) {
    if (reference == nullptr) {
        THROW(InvalidParameter, "null pointer exception.");
    }
}

}
}
