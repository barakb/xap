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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Barak Bar Orion 11/26/14.
 */
@com.gigaspaces.api.InternalApi
public class MetricRegistryTest {

    @Test
    public void testRemoveByPrefix() {
        final MetricRegistry metricRegistry = new MetricRegistry("foo");

        final LongCounter counter1 = new LongCounter();
        counter1.inc(1);
        metricRegistry.register("a.b.c", newTagsFoo(), counter1);
        final LongCounter counter2 = new LongCounter();
        counter2.inc(2);
        metricRegistry.register("a.b.d", newTagsFoo(), counter2);
        final LongCounter counter3 = new LongCounter();
        counter3.inc(3);
        metricRegistry.register("a.e", newTagsBar(), counter3);

        testBeforeRemove(metricRegistry);
        metricRegistry.removeByPrefix("a.b", newTagsFoo());
        testAfterRemove(metricRegistry);
    }

    private void testBeforeRemove(MetricRegistry metricRegistry) {
        final MetricRegistrySnapshot snapshot = metricRegistry.snapshot(1);
        Assert.assertEquals(2, snapshot.getGroups().size());

        MetricGroupSnapshot groupFoo = snapshot.getGroups().get(newTagsFoo().snapshot());
        Assert.assertEquals(2, groupFoo.getMetricsValues().size());
        Assert.assertEquals(1L, groupFoo.getMetricsValues().get("a.b.c"));
        Assert.assertEquals(2L, groupFoo.getMetricsValues().get("a.b.d"));

        MetricGroupSnapshot groupBar = snapshot.getGroups().get(newTagsBar().snapshot());
        Assert.assertEquals(1, groupBar.getMetricsValues().size());
        Assert.assertEquals(3L, groupBar.getMetricsValues().get("a.e"));
    }

    private void testAfterRemove(MetricRegistry metricRegistry) {
        final MetricRegistrySnapshot snapshot = metricRegistry.snapshot(1);
        Assert.assertEquals(1, snapshot.getGroups().size());

        MetricGroupSnapshot groupBar = snapshot.getGroups().get(newTagsBar().snapshot());
        Assert.assertEquals(1, groupBar.getMetricsValues().size());
        Assert.assertEquals(3L, groupBar.getMetricsValues().get("a.e"));
    }

    private MetricTags newTagsFoo() {
        return newTags("foo", "alpha");
    }

    private MetricTags newTagsBar() {
        return newTags("bar", "bravo");
    }

    private MetricTags newTags(String name, String value) {
        Map<String, Object> tags = new HashMap<String, Object>();
        tags.put(name, value);
        return new MetricTags(tags);
    }
}
