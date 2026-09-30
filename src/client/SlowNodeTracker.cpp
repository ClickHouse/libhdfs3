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
#include "SlowNodeTracker.h"

#include "DateTime.h"
#include "LockVector.h"
#include "Logger.h"
#include "SlowMetrics.h"
#include "Thread.h"

#include <algorithm>
#include <inttypes.h>
#include <limits>

namespace Hdfs {
namespace Internal {

SlowNodeTracker::SlowNodeTracker(const SessionConfig & conf) :
    metricEnabled(conf.isReadWriteMetricEnabled()),
    kickoutEnabled(conf.isSlowDatanodeKickoutEnabled()),
    thresholdMs(conf.getSlowDownstreamWarningThresholdMs()),
    count(conf.getSlowDownstreamWarningCount()),
    intervalMs(conf.getSlowDownstreamWarningIntervalSeconds() * 1000),
    maxCacheSize(conf.getSlowCacheSizeMax()),
    cacheExpirationMs(conf.getSlowCacheExpirationMs()),
    lastCleanMs(0) {
}

bool SlowNodeTracker::enabled() const {
    return metricEnabled && kickoutEnabled;
}

std::string SlowNodeTracker::nodeKey(const DatanodeInfo & node) const {
    if (node.getDatanodeId() != DatanodeInfo::unsetValue()) {
        return node.getDatanodeId() + "(" + node.getIpAddr() + ")";
    }

    return node.getXferAddr();
}

shared_ptr<SlowMetrics> SlowNodeTracker::getMetricsLocked(const std::string & key) {
    unordered_map<std::string, shared_ptr<SlowMetrics> >::iterator it = metrics.find(key);

    if (it != metrics.end()) {
        return it->second;
    }

    /*
     * Every DataNode which has served a read holds a metric, so the map has to
     * be bounded on its own. The DataNode which has been idle for the longest
     * time is dropped.
     */
    if (metrics.size() >= MaxMetricCount()) {
        evictIdleMetricLocked();
    }

    shared_ptr<SlowMetrics> metric(new SlowMetrics(thresholdMs, count, intervalMs));
    metrics[key] = metric;
    return metric;
}

void SlowNodeTracker::evictIdleMetricLocked() {
    if (metrics.empty()) {
        return;
    }

    unordered_map<std::string, shared_ptr<SlowMetrics> >::iterator oldest = metrics.begin();

    for (unordered_map<std::string, shared_ptr<SlowMetrics> >::iterator it = std::next(oldest);
            it != metrics.end(); ++it) {
        if (it->second->lastUpdateMs() < oldest->second->lastUpdateMs()) {
            oldest = it;
        }
    }

    LOG(DEBUG2, "SlowNodeTracker: evict DataNode %s from metric cache.",
        oldest->first.c_str());
    metrics.erase(oldest);
}

void SlowNodeTracker::cleanSlowNodesIfNeeded(int64_t nowMs) {
    if (nowMs - lastCleanMs < CLEAN_INTERVAL_MS) {
        return;
    }

    lastCleanMs = nowMs;
    cleanSlowNodes(nowMs);
}

void SlowNodeTracker::cleanSlowNodes(int64_t nowMs) {
    for (unordered_map<std::string, SlowNodeInfo>::iterator it = slowNodes.begin();
            it != slowNodes.end();) {
        if (it->second.expireAtMs <= nowMs) {
            LOG(DEBUG2, "SlowNodeTracker: DataNode %s is no longer slow.", it->first.c_str());
            it = slowNodes.erase(it);
        } else {
            ++it;
        }
    }

    while (metrics.size() > MaxMetricCount()) {
        evictIdleMetricLocked();
    }
}

bool SlowNodeTracker::cacheEnabled() const {
    return maxCacheSize > 0;
}

size_t SlowNodeTracker::MaxMetricCount() const {
    if (maxCacheSize <= 0) {
        return static_cast<size_t>(DEFAULT_MAX_METRIC_COUNT);
    }

    return static_cast<size_t>(maxCacheSize) * 2;
}

void SlowNodeTracker::markSlow(const std::string & key, double avg, int64_t nowMs) {
    if (!cacheEnabled()) {
        return;
    }

    unordered_map<std::string, SlowNodeInfo>::iterator slow = slowNodes.find(key);
    if (slow != slowNodes.end()) {
        slow->second.avgMs = avg;
        return;
    }

    /*
     * The cache is a fixed size set. When it is full the DataNode which has
     * been slow for the longest time is dropped, so that the DataNodes which
     * have been slow most recently are kept.
     */
    while (slowNodes.size() >= static_cast<size_t>(maxCacheSize)) {
        unordered_map<std::string, SlowNodeInfo>::iterator oldest = slowNodes.begin();

        for (unordered_map<std::string, SlowNodeInfo>::iterator it = std::next(oldest);
                it != slowNodes.end(); ++it) {
            if (it->second.expireAtMs < oldest->second.expireAtMs) {
                oldest = it;
            }
        }

        LOG(DEBUG2, "SlowNodeTracker: evict DataNode %s from slow node cache.",
            oldest->first.c_str());
        slowNodes.erase(oldest);
    }

    slowNodes[key] = SlowNodeInfo(nowMs + cacheExpirationMs, avg);
}

void SlowNodeTracker::recordRead(const DatanodeInfo & node, int64_t elapsedMs,
                                 const std::string & path, int64_t requestedSize,
                                 int64_t readSize) {
    if (!metricEnabled || elapsedMs < thresholdMs) {
        return;
    }

    const std::string key = nodeKey(node);
    shared_ptr<SlowMetrics> metric;

    {
        lock_guard<mutex> lock(slowNodeMutex);
        metric = getMetricsLocked(key);
    }

    const bool wasSlow = metric->inSlowState();
    const bool slowEvent = metric->putMetric(elapsedMs);
    const bool isSlowNode = metric->inSlowState();

    if (slowEvent) {
        LOG(DEBUG2,
            "SlowNodeTracker: slow read on DataNode %s (key %s), file %s, requested size %" PRId64
            ", read size %" PRId64 ", elapsed %" PRId64 " ms, threshold %" PRId64 " ms.",
            node.formatAddress().c_str(), key.c_str(), path.c_str(), requestedSize, readSize, elapsedMs, thresholdMs);
    }

    if (!isSlowNode) {
        return;
    }

    const double avg = metric->getAvg();
    const int64_t nowMs = currentTimeMs();

    {
        lock_guard<mutex> lock(slowNodeMutex);

        if (kickoutEnabled) {
            markSlow(key, avg, nowMs);
        }

        cleanSlowNodes(nowMs);
    }

    if (!wasSlow) {
        LOG(INFO,
            "SlowNodeTracker: DataNode %s (key %s) becomes slow, average time cost "
            "%.2f ms, %" PRId64 " slow reads in %" PRId64 " ms.",
            node.formatAddress().c_str(), key.c_str(), avg, count, intervalMs);
    }
}

bool SlowNodeTracker::isSlow(const DatanodeInfo & node) {
    if (!enabled()) {
        return false;
    }

    const std::string key = nodeKey(node);

    lock_guard<mutex> lock(slowNodeMutex);
    cleanSlowNodesIfNeeded(currentTimeMs());
    return slowNodes.find(key) != slowNodes.end();
}

bool SlowNodeTracker::chooseBestNode(const std::vector<DatanodeInfo> & nodes,
                                     LockVector<DatanodeInfo> & failedNodes,
                                     const std::string & path, DatanodeInfo & chosen) {
    /*
     * The DataNode which is being replaced. It is copied because chosen is
     * overwritten below, and it is not touched before that.
     */
    const DatanodeInfo from = chosen;
    bool hasSkippedSlowNode = false;

    if (!enabled()) {
        for (size_t i = 0; i < nodes.size(); ++i) {
            if (failedNodes.binary_search(nodes[i])) {
                continue;
            }

            chosen = nodes[i];
            return true;
        }

        return false;
    }

    /*
     * Keep the NameNode order, but do not read from a slow DataNode.
     */
    std::vector<bool> slowNodeFlags(nodes.size(), false);

    {
        lock_guard<mutex> lock(slowNodeMutex);
        cleanSlowNodesIfNeeded(currentTimeMs());

        for (size_t i = 0; i < nodes.size(); ++i) {
            slowNodeFlags[i] = slowNodes.find(nodeKey(nodes[i])) != slowNodes.end();
        }
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
        if (failedNodes.binary_search(nodes[i])) {
            continue;
        }

        if (slowNodeFlags[i]) {
            LOG(DEBUG2, "SlowNodeTracker: skip slow DataNode %s.",
                nodes[i].formatAddress().c_str());

            if (!hasSkippedSlowNode) {
                hasSkippedSlowNode = true;
            }

            continue;
        }

        if (hasSkippedSlowNode) {
            LogNodeSwitched(from, nodes[i], path);
        }

        chosen = nodes[i];
        return true;
    }

    /*
     * All the usable replicas are slow. Read from the one which has the
     * lowest average time cost instead of giving the read up.
     */
    std::vector<DatanodeInfo> candidateNodes;
    std::vector<double> candidateAvgs;
    std::vector<shared_ptr<SlowMetrics> > candidateMetrics;

    {
        lock_guard<mutex> lock(slowNodeMutex);
        cleanSlowNodesIfNeeded(currentTimeMs());
        unordered_map<std::string, shared_ptr<SlowMetrics> >::const_iterator it;

        for (size_t i = 0; i < nodes.size(); ++i) {
            if (failedNodes.binary_search(nodes[i])) {
                continue;
            }

            const std::string key = nodeKey(nodes[i]);
            double avg = std::numeric_limits<double>::max();
            shared_ptr<SlowMetrics> metric;
            unordered_map<std::string, SlowNodeInfo>::const_iterator slow = this->slowNodes.find(key);

            if (slow != this->slowNodes.end()) {
                avg = slow->second.avgMs;
            } else if ((it = metrics.find(key)) != metrics.end()) {
                metric = it->second;
            }

            candidateNodes.push_back(nodes[i]);
            candidateAvgs.push_back(avg);
            candidateMetrics.push_back(metric);
        }
    }

    bool found = false;
    double bestAvg = std::numeric_limits<double>::max();
    DatanodeInfo bestNode;

    for (size_t i = 0; i < candidateNodes.size(); ++i) {
        double avg = candidateAvgs[i];

        if (candidateMetrics[i]) {
            avg = candidateMetrics[i]->getAvg();
        }

        if (!found || avg < bestAvg) {
            found = true;
            bestAvg = avg;
            bestNode = candidateNodes[i];
        }
    }

    if (found) {
        if (hasSkippedSlowNode) {
            LOG(INFO,
                "SlowNodeTracker: all available DataNodes are slow, choose DataNode %s "
                "with average time cost %.2f ms for file %s.",
                bestNode.formatAddress().c_str(), bestAvg, path.c_str());
            LogNodeSwitched(from, bestNode, path);
        }

        chosen = bestNode;
        return true;
    }

    return false;
}

void SlowNodeTracker::LogNodeSwitched(const DatanodeInfo & from, const DatanodeInfo & to,
                                      const std::string & path) {
    /*
     * The very first selection has no DataNode to switch from, and a slow
     * DataNode may be reselected as the least slow one.
     */
    if (!from.isSet() || from == to) {
        return;
    }

    shared_ptr<SlowMetrics> fromMetric;
    shared_ptr<SlowMetrics> toMetric;

    {
        lock_guard<mutex> lock(slowNodeMutex);
        unordered_map<std::string, shared_ptr<SlowMetrics> >::const_iterator it;

        it = metrics.find(nodeKey(from));

        if (it != metrics.end()) {
            fromMetric = it->second;
        }

        it = metrics.find(nodeKey(to));

        if (it != metrics.end()) {
            toMetric = it->second;
        }
    }

    int64_t fromCount = 0;
    int64_t toCount = 0;
    double fromAvg = 0.0;
    double toAvg = 0.0;

    if (fromMetric) {
        fromCount = static_cast<int64_t>(fromMetric->slowCounts());
        fromAvg = fromMetric->getAvg();
    }

    if (toMetric) {
        toCount = static_cast<int64_t>(toMetric->slowCounts());
        toAvg = toMetric->getAvg();
    }

    LOG(INFO,
        "SlowNodeTracker: switch from slow DataNode %s to DataNode %s, "
        "file %s, from %" PRId64 " slow reads averaging %.2f ms, "
        "to %" PRId64 " slow reads averaging %.2f ms.",
        from.formatAddress().c_str(), to.formatAddress().c_str(), path.c_str(),
        fromCount, fromAvg, toCount, toAvg);
}

}
}
