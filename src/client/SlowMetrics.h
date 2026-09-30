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
#ifndef _HDFS_LIBHDFS3_CLIENT_SLOWMETRICS_H_
#define _HDFS_LIBHDFS3_CLIENT_SLOWMETRICS_H_

#include "Thread.h"

#include <deque>

namespace Hdfs {
namespace Internal {

/**
 * A sliding window which counts the slow reads of one DataNode.
 *
 * Only the samples which cost at least thresholdMs are counted, which is the
 * same rule the Hadoop client applies to a slow read.
 */
class SlowMetrics {
public:
    /**
     * Construct a SlowMetrics instance.
     * @param thresholdMs a read which costs at least thresholdMs is a slow read.
     * @param count the number of slow reads which makes the DataNode slow.
     * @param intervalMs the window used to count the slow reads.
     */
    SlowMetrics(int64_t thresholdMs, int64_t count, int64_t intervalMs);

    /**
     * Add a read sample.
     * @param valueMs the time cost of the read in milliseconds.
     * @return return true if the sample is counted as a slow read.
     */
    bool putMetric(int64_t valueMs);

    /**
     * To test whether the DataNode is in slow state now.
     * @return return true if the slow reads reach the given count in the
     *         current window.
     */
    bool inSlowState();

    /**
     * To get the average time cost of the counted slow reads.
     * @return the average time cost in milliseconds.
     */
    double getAvg();

    /**
     * To get the number of the counted slow reads.
     * @return the number of the counted slow reads.
     */
    size_t slowCounts();

    /**
     * To get the time stamp of the last recorded read.
     * @return the time stamp in milliseconds, 0 if no read has been recorded.
     */
    int64_t lastUpdateMs() const;

    /**
     * To drop all the counted slow reads.
     */
    void clear();

private:
    void deleteTimeoutMetric(int64_t nowMs);

private:
    struct SlowMetric {
        SlowMetric(int64_t timestampMs, int64_t valueMs)
            : timestampMs(timestampMs), valueMs(valueMs) {
        }

        int64_t timestampMs;
        int64_t valueMs;
    };

    int64_t thresholdMs;
    int64_t count;
    int64_t intervalMs;
    std::deque<SlowMetric> slowMetricList;
    double totalValueMs;
    int64_t lastUpdateMs_;
    mutable mutex slowMetricMutex;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_SLOWMETRICS_H_ */
