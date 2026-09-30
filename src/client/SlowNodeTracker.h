/********************************************************************
 * Copyright (c) 2026 -
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
#ifndef _HDFS_LIBHDFS3_CLIENT_SLOWNODETRACKER_H_
#define _HDFS_LIBHDFS3_CLIENT_SLOWNODETRACKER_H_

#include "LockVector.h"
#include "Memory.h"
#include "server/DatanodeInfo.h"
#include "SessionConfig.h"
#include "Thread.h"
#include "Unordered.h"

#include <algorithm>
#include <string>
#include <vector>

namespace Hdfs {
namespace Internal {

class SlowMetrics;

/**
 * A client side view of the slow DataNodes of one FileSystemImpl.
 *
 * Every InputStream of the FileSystem shares the same instance, so a
 * DataNode which is slow for one read is avoided by the concurrent reads as
 * well. A slow DataNode is a soft preference only, it is never treated as an
 * unavailable one.
 */
class SlowNodeTracker {
public:
    /**
     * Construct a SlowNodeTracker instance.
     * @param conf the configuration of the file system.
     */
    explicit SlowNodeTracker(const SessionConfig & conf);

    /**
     * To test whether the slow DataNode avoidance is enabled.
     * @return return true if a slow DataNode should be avoided.
     */
    bool enabled() const;

    /**
     * To record the time cost of a read.
     * @param node the DataNode which serves the read.
     * @param elapsedMs the time cost of the read in milliseconds.
     * @param path the file being read.
     * @param requestedSize the number of bytes the caller asked for.
     * @param readSize the number of bytes has been read.
     */
    void recordRead(const DatanodeInfo & node, int64_t elapsedMs,
                    const std::string & path, int64_t requestedSize,
                    int64_t readSize);

    /**
     * To choose a DataNode to read from.
     *
     * A DataNode which is neither failed nor slow is preferred. If all the
     * usable replicas are slow, the one with the lowest average time cost is
     * chosen, so that a read is never given up just because all its replicas
     * have been slow before.
     *
     * When a slow DataNode is skipped, an info log records the switch from the
     * previous DataNode to the chosen one together with their statistics.
     *
     * @param nodes all the locations of the block, in the NameNode order.
     * @param failedNodes the DataNodes which cannot serve the current block.
     * @param path the file being read, used by the switch log.
     * @param chosen input the DataNode which is used now, output the DataNode
     *               to read from.
     * @return return true if a DataNode has been chosen.
     */
    bool chooseBestNode(const std::vector<DatanodeInfo> & nodes,
                        LockVector<DatanodeInfo> & failedNodes,
                        const std::string & path, DatanodeInfo & chosen);

private:
    std::string nodeKey(const DatanodeInfo & node) const;
    bool isSlow(const DatanodeInfo & node);
    shared_ptr<SlowMetrics> getMetricsLocked(const std::string & key);
    void markSlow(const std::string & key, double avg, int64_t nowMs);
    void cleanSlowNodes(int64_t nowMs);
    void cleanSlowNodesIfNeeded(int64_t nowMs);
    void evictIdleMetricLocked();
    void LogNodeSwitched(const DatanodeInfo & from, const DatanodeInfo & to,
                         const std::string & path);
    bool cacheEnabled() const;
    size_t MaxMetricCount() const;

private:
    const bool metricEnabled;
    const bool kickoutEnabled;
    const int64_t thresholdMs;
    const int64_t count;
    const int64_t intervalMs;
    const int maxCacheSize;
    const int64_t cacheExpirationMs;

    /**
     * The number of the metrics which are kept when the slow node cache is
     * disabled, they are still needed to log a slow DataNode.
     */
    static const int DEFAULT_MAX_METRIC_COUNT = 1024;
    static const int64_t CLEAN_INTERVAL_MS = 1000;

    struct SlowNodeInfo {
        SlowNodeInfo() : expireAtMs(0), avgMs(0.0) {
        }

        SlowNodeInfo(int64_t expireAtMs, double avgMs)
            : expireAtMs(expireAtMs), avgMs(avgMs) {
        }

        int64_t expireAtMs;
        double avgMs;
    };

    mutex slowNodeMutex;
    int64_t lastCleanMs;
    unordered_map<std::string, shared_ptr<SlowMetrics> > metrics;
    unordered_map<std::string, SlowNodeInfo> slowNodes;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_SLOWNODETRACKER_H_ */
