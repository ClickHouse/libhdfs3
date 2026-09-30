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

#include "client/SlowMetrics.h"
#include "DateTime.h"
#include "Thread.h"

#include <limits>

using namespace Hdfs;
using namespace Hdfs::Internal;

static void sleepMs(int64_t ms) {
    sleep_for(milliseconds(ms));
}

TEST(TestSlowMetrics, IgnoreFastRead) {
    SlowMetrics metrics(100, 3, 60 * 1000);
    EXPECT_FALSE(metrics.putMetric(99));
    EXPECT_EQ(0, metrics.slowCounts());
    EXPECT_FALSE(metrics.inSlowState());
}

TEST(TestSlowMetrics, CountSlowRead) {
    SlowMetrics metrics(100, 3, 60 * 1000);
    EXPECT_TRUE(metrics.putMetric(100));
    EXPECT_EQ(1, metrics.slowCounts());
    EXPECT_FALSE(metrics.inSlowState());

    EXPECT_TRUE(metrics.putMetric(500));
    EXPECT_EQ(2, metrics.slowCounts());
    EXPECT_FALSE(metrics.inSlowState());

    EXPECT_TRUE(metrics.putMetric(200));
    EXPECT_EQ(3, metrics.slowCounts());
    EXPECT_TRUE(metrics.inSlowState());
}

TEST(TestSlowMetrics, GetAvg) {
    SlowMetrics metrics(100, 2, 60 * 1000);
    metrics.putMetric(100);
    metrics.putMetric(300);
    /* a read which is not slow must not change the average */
    metrics.putMetric(10);
    EXPECT_DOUBLE_EQ(200.0, metrics.getAvg());
}

TEST(TestSlowMetrics, ExpireSlowRead) {
    SlowMetrics metrics(100, 2, 100);
    EXPECT_TRUE(metrics.putMetric(500));
    EXPECT_FALSE(metrics.inSlowState());
    EXPECT_TRUE(metrics.putMetric(500));
    EXPECT_TRUE(metrics.inSlowState());

    sleepMs(200);
    EXPECT_FALSE(metrics.inSlowState());
    EXPECT_EQ(0, metrics.slowCounts());
}

TEST(TestSlowMetrics, Clear) {
    SlowMetrics metrics(100, 1, 60 * 1000);
    EXPECT_TRUE(metrics.putMetric(500));
    EXPECT_TRUE(metrics.inSlowState());

    metrics.clear();
    EXPECT_EQ(0, metrics.slowCounts());
    EXPECT_FALSE(metrics.inSlowState());
    EXPECT_EQ(0, metrics.lastUpdateMs());
}

TEST(TestSlowMetrics, GetAvgDoesNotOverflowInt64) {
    SlowMetrics metrics(100, 2, 60 * 1000);
    const int64_t value = std::numeric_limits<int64_t>::max();
    EXPECT_TRUE(metrics.putMetric(value));
    EXPECT_TRUE(metrics.putMetric(value));
    EXPECT_DOUBLE_EQ(static_cast<double>(value), metrics.getAvg());
}
