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
#include "gtest/gtest.h"
#include "SessionConfig.h"
#include "XmlConfig.h"

using namespace Hdfs;
using namespace Hdfs::Internal;

TEST(TestSessionConfig, TestSelect) {
    Config conf;
    SessionConfig session(conf);
    ASSERT_STREQ("hdfs://localhost:9000", session.getDefaultUri().c_str());
}

TEST(TestSessionConfig, SlowNodeDetectionIsDisabledByDefault) {
    Config conf;
    SessionConfig session(conf);

    EXPECT_FALSE(session.isReadWriteMetricEnabled());
    EXPECT_FALSE(session.isSlowDatanodeKickoutEnabled());
    EXPECT_EQ(100, session.getSlowCacheSizeMax());
}

TEST(TestSessionConfig, SlowNodeDetectionFromConfig) {
    Config conf;
    conf.set("dfs.client.read.write.metric.enable", "true");
    conf.set("dfs.client.slow.datanode.kickout.enable", "true");
    conf.set("dfs.client.slow.downstream.warning.threshold.ms", 500);
    conf.set("dfs.client.slow.downstream.warning.count", 3);
    conf.set("dfs.client.slow.cache.size.max", 7);
    SessionConfig session(conf);

    EXPECT_TRUE(session.isReadWriteMetricEnabled());
    EXPECT_TRUE(session.isSlowDatanodeKickoutEnabled());
    EXPECT_EQ(500, session.getSlowDownstreamWarningThresholdMs());
    EXPECT_EQ(3, session.getSlowDownstreamWarningCount());
    EXPECT_EQ(7, session.getSlowCacheSizeMax());
}
