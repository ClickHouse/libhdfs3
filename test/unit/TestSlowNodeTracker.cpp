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
#include "gtest/gtest.h"

#define private public
#include "client/SlowNodeTracker.h"
#undef private
#include "DateTime.h"
#include "LockVector.h"
#include "SessionConfig.h"
#include "Thread.h"
#include "XmlConfig.h"

#include <string>

using namespace Hdfs;
using namespace Hdfs::Internal;

static const int64_t THRESHOLD_MS = 100;

static void sleepMs(int64_t ms) {
    sleep_for(milliseconds(ms));
}

static DatanodeInfo makeDataNode(const char * host, const char * ip,
                                 const char * id) {
    DatanodeInfo node;
    node.setHostName(host);
    node.setIpAddr(ip);
    node.setDatanodeId(id);
    node.setXferPort(50010);
    return node;
}

static Config makeConfig(bool enable, int64_t slowReadCount,
                         int64_t cacheExpirationMs, int64_t maxCacheSize) {
    Config conf;
    conf.set("dfs.client.read.write.metric.enable", enable ? "true" : "false");
    conf.set("dfs.client.slow.datanode.kickout.enable", enable ? "true" : "false");
    conf.set("dfs.client.slow.downstream.warning.threshold.ms", THRESHOLD_MS);
    conf.set("dfs.client.slow.downstream.warning.count", slowReadCount);
    conf.set("dfs.client.slow.downstream.warning.interval.seconds", 300);
    conf.set("dfs.client.slow.cache.expiration.ms", cacheExpirationMs);
    conf.set("dfs.client.slow.cache.size.max", maxCacheSize);
    return conf;
}

TEST(TestSlowNodeTracker, DefaultDisabled) {
    Config conf;
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);
    EXPECT_FALSE(tracker.enabled());
    EXPECT_FALSE(sconf.isReadWriteMetricEnabled());
    EXPECT_FALSE(sconf.isSlowDatanodeKickoutEnabled());
}

TEST(TestSlowNodeTracker, ChooseNodeWithoutSlowNode) {
    Config conf = makeConfig(true, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);
    EXPECT_TRUE(tracker.enabled());

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[0]);
}

TEST(TestSlowNodeTracker, SkipSlowNode) {
    Config conf = makeConfig(true, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[0], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[1]);
}

TEST(TestSlowNodeTracker, UseStableDataNodeIdKeyAcrossHostNameChanges) {
    Config conf = makeConfig(true, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1-alias", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(makeDataNode("node1", "10.0.0.1", "id1"),
                       2 * THRESHOLD_MS, "/file", 1024, 1024);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[1]);
}

TEST(TestSlowNodeTracker, SkipSlowNodeAndFailedNode) {
    Config conf = makeConfig(true, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    nodes.push_back(makeDataNode("node3", "10.0.0.3", "id3"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[1], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    failedNodes.push_back(nodes[0]);
    failedNodes.sort();

    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[2]);
}

TEST(TestSlowNodeTracker, FallbackToLeastSlowNode) {
    Config conf = makeConfig(true, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    nodes.push_back(makeDataNode("node3", "10.0.0.3", "id3"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[0], 5 * THRESHOLD_MS, "/file", 1024, 1024);
    tracker.recordRead(nodes[1], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    tracker.recordRead(nodes[2], 9 * THRESHOLD_MS, "/file", 1024, 1024);

    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[1]);
}

TEST(TestSlowNodeTracker, CacheSizeZeroKeepsMetricsOnly) {
    /*
     * A cache size of zero disables the cache, the slow DataNode is still
     * measured but it is not avoided anymore.
     */
    Config conf = makeConfig(true, 1, 60 * 1000, 0);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[0], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[0]);
}

TEST(TestSlowNodeTracker, ExpireSlowNode) {
    Config conf = makeConfig(true, 1, 100, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[0], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[1]);

    sleepMs(200);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[0]);
}

TEST(TestSlowNodeTracker, EvictSlowNodeFromCache) {
    Config conf = makeConfig(true, 1, 60 * 1000, 1);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[0], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    tracker.recordRead(nodes[1], 2 * THRESHOLD_MS, "/file", 1024, 1024);

    /* the first DataNode has been evicted, so it is preferred again */
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[0]);
}

TEST(TestSlowNodeTracker, KeepOriginalOrderWhenDisabled) {
    Config conf = makeConfig(false, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);
    EXPECT_FALSE(tracker.enabled());

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;
    DatanodeInfo chosen;

    tracker.recordRead(nodes[0], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[0]);

    failedNodes.push_back(nodes[0]);
    failedNodes.sort();
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[1]);
}

/*
 * The metrics are keyed by every DataNode which has served a read. They have
 * to stay bounded on their own, the slow node cache does not cover them.
 */
TEST(TestSlowNodeTracker, BoundMetricsForFastReads) {
    Config conf = makeConfig(true, 1, 60 * 1000, 4);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    for (int i = 0; i < 200; ++i) {
        const std::string suffix = std::to_string(i);
        tracker.recordRead(makeDataNode(("n" + suffix).c_str(),
                                        ("10.0.0." + suffix).c_str(),
                                        ("id" + suffix).c_str()),
                           THRESHOLD_MS / 2, "/file", 1024, 1024);
    }

    EXPECT_LE(tracker.metrics.size(), static_cast<size_t>(8));
}

/*
 * A cache size of zero disables the slow node cache, the metrics are still
 * bounded, they are only kept to log a slow DataNode.
 */
TEST(TestSlowNodeTracker, BoundMetricsWhenCacheIsDisabled) {
    Config conf = makeConfig(true, 1, 60 * 1000, 0);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    for (int i = 0; i < 3000; ++i) {
        const std::string suffix = std::to_string(i);
        tracker.recordRead(makeDataNode(("n" + suffix).c_str(),
                                        ("10.0.0." + suffix).c_str(),
                                        ("id" + suffix).c_str()),
                           2 * THRESHOLD_MS, "/file", 1024, 1024);
    }

    EXPECT_LE(tracker.metrics.size(), 2048u);
    EXPECT_EQ(0u, tracker.slowNodesExpireAtMs.size());
    EXPECT_EQ(0u, tracker.slowNodeAvgMs.size());
}

/*
 * The chosen DataNode is passed in as the current one, a switch away from a
 * slow DataNode must not be reported when there is nothing to switch from or
 * when the slow DataNode is picked again.
 */
TEST(TestSlowNodeTracker, ChooseBestNodeAcceptsCurrentNode) {
    Config conf = makeConfig(true, 1, 60 * 1000, 100);
    SessionConfig sconf(conf);
    SlowNodeTracker tracker(sconf);

    std::vector<DatanodeInfo> nodes;
    nodes.push_back(makeDataNode("node1", "10.0.0.1", "id1"));
    nodes.push_back(makeDataNode("node2", "10.0.0.2", "id2"));
    LockVector<DatanodeInfo> failedNodes;

    /* the first selection has no current DataNode */
    DatanodeInfo chosen;
    EXPECT_FALSE(chosen.isSet());
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[0]);

    /* node1 turns slow, the reader has to move to node2 */
    tracker.recordRead(nodes[0], 2 * THRESHOLD_MS, "/file", 1024, 1024);
    EXPECT_TRUE(tracker.chooseBestNode(nodes, failedNodes, "/file", chosen));
    EXPECT_TRUE(chosen == nodes[1]);
}

/*
 * A DataNode which has never been set is not a real one, it is the state of
 * DatanodeInfo before the first selection.
 */
TEST(TestSlowNodeTracker, DefaultDataNodeIsNotSet) {
    DatanodeInfo node;

    EXPECT_FALSE(node.isSet());

    node.setHostName("node1");
    node.setIpAddr("10.0.0.1");
    node.setDatanodeId("id1");
    EXPECT_TRUE(node.isSet());
}
