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

#include "RawErasureCoderFactory.h"
#include "NativeRSRawEncoder.h"
#include "NativeRSRawDecoder.h"
#include "Logger.h"

namespace Hdfs {
namespace Internal {

bool RawErasureCoderFactory::buildSupportsIsal() {
#ifdef HADOOP_ISAL_LIBRARY
    return true;
#else
    return false;
#endif
}

shared_ptr<RawErasureEncoder> RawErasureCoderFactory::createEncoder(ErasureCoderOptions & coderOptions) {
    if (buildSupportsIsal()) {
	LOG(DEBUG1, "support isa-l");
        return shared_ptr<NativeRSRawEncoder>(new NativeRSRawEncoder(coderOptions));
    } else {
	LOG(DEBUG1, "not support isa-l");
        return shared_ptr<RawErasureEncoder>(new RawErasureEncoder(coderOptions));
    }
}

shared_ptr<RawErasureDecoder> RawErasureCoderFactory::createDecoder(ErasureCoderOptions & coderOptions) {
    if (buildSupportsIsal()) {
	LOG(DEBUG1, "support isa-l");
        return shared_ptr<NativeRSRawDecoder>(new NativeRSRawDecoder(coderOptions));
    } else {
	LOG(DEBUG1, "not support isa-l");
        return shared_ptr<RawErasureDecoder>(new RawErasureDecoder(coderOptions));
    }
}

}
}