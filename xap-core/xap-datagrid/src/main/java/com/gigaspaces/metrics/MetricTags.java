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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricTags {
    private final Map<String, Object> tags;
    private final Map<String, DynamicMetricTag> dynamicTags;
    private final String id;
    private Map<String, Object> tagsSnapshot;
    private MetricTagsSnapshot snapshot;

    public MetricTags(Map<String, Object> tags) {
        this(tags, Collections.EMPTY_MAP);
    }

    public MetricTags(Map<String, Object> tags, Map<String, DynamicMetricTag> dynamicTags) {
        this.tags = Collections.unmodifiableMap(tags);
        this.dynamicTags = dynamicTags;
        this.id = tags.toString() + (dynamicTags.isEmpty() ? "" : "_" + dynamicTags.keySet().toString());
        if (dynamicTags.isEmpty())
            tagsSnapshot = tags;
        else {
            tagsSnapshot = new HashMap<String, Object>(tags);
            for (Map.Entry<String, DynamicMetricTag> entry : dynamicTags.entrySet())
                tagsSnapshot.put(entry.getKey(), entry.getValue().getValue());
        }
        this.snapshot = new MetricTagsSnapshot(this.tagsSnapshot);
    }

    public MetricTags extend(Map<String, String> newTags, Map<String, DynamicMetricTag> newDynamicTags) {
        if (newTags.isEmpty() && newDynamicTags.isEmpty())
            return this;
        Map<String, Object> mergedTags = new HashMap<String, Object>(this.tags);
        mergedTags.putAll(newTags);
        Map<String, DynamicMetricTag> mergedDynamicTags = Collections.EMPTY_MAP;
        if (!this.dynamicTags.isEmpty())
            mergedDynamicTags = new HashMap<String, DynamicMetricTag>(this.dynamicTags);
        if (newDynamicTags != null && !newDynamicTags.isEmpty()) {
            if (mergedDynamicTags.isEmpty())
                mergedDynamicTags = new HashMap<String, DynamicMetricTag>(newDynamicTags);
            else
                mergedDynamicTags.putAll(newDynamicTags);
        }
        return new MetricTags(mergedTags, mergedDynamicTags);
    }

    public Map<String, Object> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof MetricTags))
            return false;
        MetricTags other = (MetricTags) obj;
        return this.id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public MetricTagsSnapshot snapshot() {
        if (!dynamicTags.isEmpty()) {
            boolean hasChanges = false;
            for (Map.Entry<String, DynamicMetricTag> entry : dynamicTags.entrySet()) {
                String tag = entry.getKey();
                Object currValue = entry.getValue().getValue();
                Object prevValue = tagsSnapshot.get(tag);
                if (!prevValue.equals(currValue)) {
                    hasChanges = true;
                    tagsSnapshot.put(tag, currValue);
                }
            }
            if (hasChanges)
                snapshot = new MetricTagsSnapshot(tagsSnapshot);
        }
        return snapshot;
    }
}
