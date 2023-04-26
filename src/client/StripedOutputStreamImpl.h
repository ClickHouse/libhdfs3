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
#ifndef _HDFS_LIBHDFS3_CLIENT_STRIPEDOUTPUTSTREAMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_STRIPEDOUTPUTSTREAMIMPL_H_

#include "ECPolicy.h"
#include "OutputStreamImpl.h"
#include "PacketHeader.h"
#include "ByteBuffer.h"
#include "ThreadPool.h"
#include "RawErasureEncoder.h"

namespace Hdfs {
namespace Internal {

/**
 * A output stream used to write ec data to hdfs.
 */
class StripedOutputStreamImpl: public OutputStreamImpl {
public:
    StripedOutputStreamImpl();

    explicit StripedOutputStreamImpl(shared_ptr<ECPolicy> ecPolicy);

    ~StripedOutputStreamImpl() = default;

    /**
     * To create or append a file.
     * @param fs hdfs file system.
     * @param path the file path.
     * @param pair the result of create or append.
     * @param flag creation flag, can be Create, Append or Create|Overwrite.
     * @param permission create a new file with given permission.
     * @param createParent if the parent does not exist, create it.
     * @param replication create a file with given number of replication.
     * @param blockSize  create a file with given block size.
     * @param fileId  the file id.
     */
    void open(shared_ptr<FileSystemInter> fs, const char * path,
              std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> & pair,
              int flag, const Permission & permission, bool createParent, int replication,
              int64_t blockSize, int64_t fileId) override;

    /**
     * To append data to file.
     * @param buf the data used to append.
     * @param size the data size.
     */
    void append(const char * buf, int64_t size) override;

    /**
     * Flush all data in buffer and waiting for ack.
     * Will block until get all acks.
     */
    void flush() override;

    /**
     * @ref OutputStream::sync
     */
    void sync() override;

    /**
     * close the stream.
     */
    void close() override;

    /**
     * Output a readable string of this output stream.
     */
    std::string toString() override;

    /**
     * Keep the last error of this stream.
     * @error the error to be kept.
     */
    void setError(const exception_ptr & error) override;

protected:
    class CellBuffers {
    public:
        CellBuffers();
        explicit CellBuffers(const StripedOutputStreamImpl *sosi);
        ~CellBuffers() = default;

        const StripedOutputStreamImpl * sosi;
        std::vector<shared_ptr<ByteBuffer>> buffers;

        std::vector<shared_ptr<ByteBuffer>> getBuffers();
        int addTo(int i, const char * b, int len);
        void clear();
        void flipDataBuffers();
    };

    template <class T>
    class MultipleBlockingQueue {
    public:
        MultipleBlockingQueue();
        explicit MultipleBlockingQueue(int numQueue);
        ~MultipleBlockingQueue() = default;

        void push(int i, T t);
        void pop(int i);
        T& front(int i);
        void clear();

    private:
        std::vector<shared_ptr<std::queue<T>>> queues;
    };

    class Coordinator {
    public:
        Coordinator();
        explicit Coordinator(int numAllBlocks);
        ~Coordinator() = default;

        shared_ptr<MultipleBlockingQueue<LocatedBlock>> getFollowingBlocks();

    private:
        shared_ptr<MultipleBlockingQueue<LocatedBlock>> followingBlocks;
        shared_ptr<MultipleBlockingQueue<LocatedBlock>> endBlocks;
    };

    class StripedDataStreamer {
    public:
        StripedDataStreamer();

        StripedDataStreamer(int index, shared_ptr<Coordinator> coordinator) :
            index(index), coor(coordinator), nextSeqNo(0), bytesCurBlock(0),
            internalError(false), isClosed(false) {
        }

        ~StripedDataStreamer() = default;

        int getIndex() {
            return index;
        }

        void endBlockInternal();

        LocatedBlock getFollowingBlock() {
            return coor->getFollowingBlocks()->front(index);
        }

        void popFollowingBlock() {
            coor->getFollowingBlocks()->pop(index);
        }

        LocatedBlock peekFollowingBlock();

        shared_ptr<PipelineImpl> getPipeline() {
            return pipe;
        }

        void setPipeline(shared_ptr<PipelineImpl> pipeline) {
            pipe = pipeline;
        }

        bool isHealthy() {
            return !isClosed && !getInternalError();
        }

        int64_t getAndIncNextSeqNo() {
            int64_t old = nextSeqNo;
            nextSeqNo++;
            return old;
        }

        int64_t getBytesCurBlock() {
            return bytesCurBlock;
        }

        void setBytesCurBlock(int64_t bytesCurBlock_) {
            bytesCurBlock = bytesCurBlock_;
        }

        void incBytesCurBlock(int64_t len) {
            bytesCurBlock += len;
        }

        void resetPipeline() {
            pipe.reset();
        }

        void close(bool force) {}

        void setInternalError(bool error) {
            internalError = error;
        }

        bool getInternalError() {
            return internalError;
        }

    private:
        int index;
        shared_ptr<Coordinator> coor;
        shared_ptr<PipelineImpl> pipe;
        int64_t nextSeqNo;
        int64_t bytesCurBlock;
        bool internalError;
        bool isClosed;
    };

private:
    int getCurrentIndex();
    bool shouldEndBlockGroup();
    void allocateNewBlock();
    void adjustChunkBoundary();
    shared_ptr<StripedDataStreamer> setCurrentStreamer(int newIdx);
    void writeParity(int index, shared_ptr<ByteBuffer> buffer);
    bool checkAnyParityStreamerIsHealthy();
    void writeParityCells();
    void flushAllInternals();
    void closePipeline();
    int stripeDataSize();
    void encode(shared_ptr<RawErasureEncoder> encoder, int numData,
                std::vector<shared_ptr<ByteBuffer>> buffers);
    shared_ptr<StripedDataStreamer> getCurrentStreamer();
    shared_ptr<StripedDataStreamer> getStripedDataStreamer(int i);
    void setCurrentPacketToEmpty();
    void initAppend(std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus>> & lastBlockWithStatus);
    void appendChunkToPacketInternal(const char * buf, int size);
    void appendChunkToPacket(const char * buf, int size);
    void appendInternal(const char * buf, int64_t size);
    void checkStatus();
    void checkStreamerFailures(bool needFlush);
    void flushBuffer();
    bool generateParityCellsForLastStripe();
    void enqueueAllCurrentPackets();
    void completeFile(bool throwError);
    void openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                      std::pair<shared_ptr<LocatedBlock>, shared_ptr<Hdfs::FileStatus>> & pair,
                      int flag, const Permission & permission, bool createParent, int replication,
                      int64_t blockSize, int64_t fileId);
    void sendPacket(shared_ptr<Packet> packet);
    void setupPipeline();
    void replaceFailedStreamers();
    void closeAllStreamers();
    void handleStreamerFailure(const char * err, Hdfs::exception_ptr eptr,
                               shared_ptr<StripedDataStreamer> streamer);
    std::set<shared_ptr<StripedDataStreamer>> checkStreamers();
    bool isStreamerWriting(int index);
    ExtendedBlock updateBlockForPipeline(std::set<shared_ptr<StripedDataStreamer>> healthyStreamers);
    void updatePipeline(ExtendedBlock newBG);
    int64_t getAckedLength();
    std::vector<int64_t> getBlockLengths();

private:
    // EC
    shared_ptr<Coordinator> coordinator;
    shared_ptr<CellBuffers> cellBuffers;
    shared_ptr<RawErasureEncoder> encoder;
    shared_ptr<ECPolicy> ecPolicy;

    shared_ptr<LocatedBlock> currentBlockGroup;
    shared_ptr<LocatedBlock> prevBlockGroup4Append;
    std::vector<shared_ptr<StripedDataStreamer>> failedStreamers;
    std::map<int, int> corruptBlockCountMap;
    shared_ptr<ThreadPool> flushAllThreadPool;
    std::vector<shared_ptr<Packet>> currentPackets;
    shared_ptr<Packet> currentPacket;
    std::vector<shared_ptr<StripedDataStreamer>> streamers;
    shared_ptr<StripedDataStreamer> currentStreamer;
    shared_ptr<PipelineImpl> currentPipeline;
    int cellSize;
    int numAllBlocks;
    int numDataBlocks;
    int blockGroupIndex;

#ifdef MOCK
private:
    Hdfs::Mock::PipelineStub * stub;
#endif
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_STRIPEDOUTPUTSTREAMIMPL_H_ */
