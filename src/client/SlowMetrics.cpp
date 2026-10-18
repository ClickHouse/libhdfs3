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
#include "SlowMetrics.h"

#include "DateTime.h"
#include "Thread.h"

#include <inttypes.h>

namespace Hdfs {
namespace Internal {

SlowMetrics::SlowMetrics(int64_t thresholdMs, int64_t count,
                         int64_t intervalMs)
    : thresholdMs(thresholdMs),
      count(count),
      intervalMs(intervalMs),
      totalValueMs(0.0),
      lastUpdateMs_(0) {
}

int64_t SlowMetrics::lastUpdateMs() const {
    lock_guard<mutex> lock(slowMetricMutex);
    return lastUpdateMs_;
}

void SlowMetrics::deleteTimeoutMetric(int64_t nowMs) {
    while (!slowMetricList.empty()
           && nowMs - slowMetricList.front().timestampMs > intervalMs) {
        totalValueMs -= static_cast<double>(slowMetricList.front().valueMs);
        slowMetricList.pop_front();
    }
}

bool SlowMetrics::putMetric(int64_t valueMs) {
    const int64_t nowMs = currentTimeMs();

    lock_guard<mutex> lock(slowMetricMutex);
    lastUpdateMs_ = nowMs;

    if (valueMs < thresholdMs) {
        return false;
    }

    deleteTimeoutMetric(nowMs);
    slowMetricList.push_back(SlowMetric(nowMs, valueMs));
    totalValueMs += static_cast<double>(valueMs);
    return true;
}

bool SlowMetrics::inSlowState() {
    return count > 0 && static_cast<int64_t>(slowCounts()) >= count;
}

double SlowMetrics::getAvg() {
    const int64_t nowMs = currentTimeMs();

    lock_guard<mutex> lock(slowMetricMutex);
    deleteTimeoutMetric(nowMs);

    if (slowMetricList.empty()) {
        return 0.0;
    }

    return totalValueMs / slowMetricList.size();
}

size_t SlowMetrics::slowCounts() {
    const int64_t nowMs = currentTimeMs();

    lock_guard<mutex> lock(slowMetricMutex);
    deleteTimeoutMetric(nowMs);
    return slowMetricList.size();
}

void SlowMetrics::clear() {
    lock_guard<mutex> lock(slowMetricMutex);
    slowMetricList.clear();
    totalValueMs = 0.0;
    lastUpdateMs_ = 0;
}

}
}
