/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.gigaspaces.metrics;

import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class MetricRegistrySnapshot {
    private final long timestamp;
    private final Map<MetricTagsSnapshot, MetricGroupSnapshot> groups;

    protected MetricRegistrySnapshot(long timestamp, Map<MetricTagsSnapshot, MetricGroupSnapshot> groups) {
        this.timestamp = timestamp;
        this.groups = groups;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<MetricTagsSnapshot, MetricGroupSnapshot> getGroups() {
        return groups;
    }

    public int getTotalMetrics() {
        int totalMetrics = 0;
        for (MetricGroupSnapshot value : groups.values())
            totalMetrics += value.getMetricsValues().size();
        return totalMetrics;
    }
}
