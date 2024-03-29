/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
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
#include "Logger.h"
#include "Packet.h"
#include "PacketPool.h"

namespace Hdfs {
namespace Internal {

PacketPool::PacketPool(int size) :
    maxSize(size) {
}

shared_ptr<Packet> PacketPool::getPacket(int pktSize, int chunksPerPkt,
        int64_t offsetInBlock, int64_t seqno, int checksumSize) {
    lock_guard < mutex > lock(mut);
    if (packets.empty()) {
        return shared_ptr<Packet>(
                   new Packet(pktSize, chunksPerPkt, offsetInBlock, seqno,
                              checksumSize));
    } else {
        shared_ptr<Packet> retval = packets.front();
        packets.pop_front();
        retval->reset(pktSize, chunksPerPkt, offsetInBlock, seqno,
                      checksumSize);
        return retval;
    }
}

void PacketPool::releasePacket(shared_ptr<Packet> packet) {
    lock_guard < mutex > lock(mut);
    if (static_cast<int>(packets.size()) >= maxSize) {
        return;
    }

    packets.push_back(packet);
}

}
}
