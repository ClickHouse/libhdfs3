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
#include "DateTime.h"
#include "Pipeline.h"
#include "Logger.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "datatransfer.pb.h"
#include "DataReader.h"


#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace ::google::protobuf;
using namespace google::protobuf::io;

namespace Hdfs {
namespace Internal {


int fillData(BufferedSocketReader *reader, std::string &raw, bool &error, DataTransferProtocol *sender=NULL) {
    int offset=0;
    int numRetries=0;
    raw.resize(65536);
    int polltime = 100;
    error = false;
    if (sender) {
        std::string temp;
        temp.resize(1);

        // we need to read at most 5 bytes
        int i = 0;
        std::string data;
        while (i < 5) {
            reader->readFully(&temp[0], 1, 30000);
            std::string dec = sender->unwrap(temp);
            i += 1;
            data += dec;
            const uint8* ptr = (uint8*) dec.c_str();
            if (!(*ptr & 0x80))
                break;
        }
        CodedInputStream stream(reinterpret_cast<const uint8_t *>(data.c_str()), data.length());
        int size;
        bool ret = stream.ReadVarint32((uint32*)&size);
        if (!ret || size <= 0 || size > 65536)
        {
            // Not encrypted error case
            memcpy(&raw[0], &temp[0], 5);
            offset = 5;
            error = true;
        } else {

            // This is the logic used by ReadVarint32. Unfortunately
            // the class does not give a way to tell how data was consumed.
            const uint8* ptr = (uint8*) data.c_str();
            int used = 1;
            for (int i=0; i < (int)data.size(); i++) {
                if (!(*ptr & 0x80))
                    break;
                used += 1;
                ptr += 1;
            }
            int remaining = data.length() - used;
            if (size - remaining) {
                temp.resize(size-remaining);
                reader->readFully(&temp[0], size-remaining, 30000);
                data = data + sender->unwrap(temp);
            }
            raw.assign(data);
            return data.length();
        }
    }

    while (numRetries < 5 && offset < 65536) {
        if (reader->poll(polltime)) {
            int nread = 0;

            try {
                nread = reader->read(&raw[offset], 65536-offset);
            }
            catch (HdfsEndOfStream ex) {
                if (offset == 0)
                    throw;
                break;
            }
            catch (HdfsNetworkException ex) {
                if (offset == 0)
                    throw;
                error = true;
                break;
            }
            if (nread) {
                offset += nread;
                numRetries = 0;
            } else {
                numRetries += 1;
            }
            if (offset > 10)
                polltime = 30;
        } else {
            numRetries += 1;
        }
    }
    if (offset == 0) {
        THROW(HdfsIOException, "Couldn't fill buffer")
    }
    raw.resize(offset);
    return offset;

}
DataReader::DataReader(DataTransferProtocol * sender,
        shared_ptr<BufferedSocketReader> reader, int readTimeout) : buf(128), sender(sender),
        reader(reader), readTimeout(readTimeout)
        {
            // max size of packet
            raw.resize(65536);
            decrypted.resize(65536);
        }

std::vector<char>& DataReader::readPacketHeader(const char* text, int size, int &outsize) {
    int nread = size;
    if (rest.size()) {
        decrypted = rest;
        rest = "";
        if ((int)decrypted.size() < size) {
            bool error = false;
            fillData(reader.get(), raw, error);
            decrypted += sender->unwrap(raw);
        }
    } else {
        bool error = false;
        fillData(reader.get(), raw, error);
        if (!error)
            decrypted = sender->unwrap(raw);
        else
            decrypted = raw;
    }
    CodedInputStream stream(reinterpret_cast<const uint8_t *>(decrypted.c_str()), decrypted.length());
    buf.resize(nread);
    bool ret = stream.ReadRaw(&buf[0], nread);
    if (!ret) {
        THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
          text);
    }
    rest.assign(&decrypted[nread], decrypted.size()-nread);
    outsize = nread;
    return buf;
}

void DataReader::setRest(const char* data, int size) {
    rest.assign(data, size);
}

void DataReader::getMissing(int size) {
    bool error = false;
    if (sender->isWrapped()) {
        if (!sender->needsLength()) {
            while (size > (int)rest.size()) {
                fillData(reader.get(), raw, error);
                decrypted = sender->unwrap(raw);
                rest = rest + decrypted;
            }
        }
     }
}

void DataReader::reduceRest(int size) {
    std::string temp;
    temp.assign(rest.c_str() + size, rest.size()-size);
    rest = temp;
}

std::vector<char>& DataReader::readResponse(const char* text, int &outsize) {
    int size;
    bool error = false;
    if (sender->isWrapped()) {
        if (!sender->needsLength()) {
            if (rest.size()) {
                decrypted = rest;
                rest = "";
            } else {
                fillData(reader.get(), raw, error, sender);
                decrypted = raw;
            }
            CodedInputStream stream(reinterpret_cast<const uint8_t *>(decrypted.c_str()), decrypted.length());
            bool ret = stream.ReadVarint32((uint32*)&size);

            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode size response: %s",
                  text);
            }
            if ((int)decrypted.size() < size) {
                fillData(reader.get(), raw, error);
                decrypted += sender->unwrap(raw);
            }
            buf.resize(size);
            ret = stream.ReadRaw(&buf[0], size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }
            int pos = decrypted.find(&buf[0], 0, size);
            if (pos == (int)string::npos) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }

            rest.assign(&decrypted[size+pos], decrypted.size()-(size+pos));
        } else {
            size = reader->readBigEndianInt32(readTimeout);
            buf.resize(size);
            reader->readFully(&buf[0], size, readTimeout);

            std::string data = sender->unwrap(&buf[0], size);

            bool ret;
            CodedInputStream stream(reinterpret_cast<const uint8_t *>(data.c_str()), data.length());
            ret = stream.ReadVarint32((uint32*)&size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode size response: %s",
                  text);
            }
            buf.resize(size);
            ret = stream.ReadRaw(&buf[0], size);
            if (!ret) {
                THROW(HdfsIOException, "cannot parse wrapped datanode data response: %s",
                  text);
            }
       }
    }
    else {
        size = reader->readVarint32(readTimeout);
        buf.resize(size);
        reader->readFully(&buf[0], size, readTimeout);
    }
    outsize = size;
    return buf;
}


}
}