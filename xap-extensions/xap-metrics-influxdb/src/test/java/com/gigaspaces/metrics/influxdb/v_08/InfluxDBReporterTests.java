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

package com.gigaspaces.metrics.influxdb.v_08;

import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistry;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricTags;
import com.gigaspaces.metrics.influxdb.InfluxDBDispatcher;
import com.gigaspaces.metrics.influxdb.InfluxDBReporterFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBReporterTests {

    @Test
    public void testCounters() {
        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();

        LongCounter counter1 = new LongCounter();
        registry.register("counter1", tags, counter1);

        // Test single counter on empty counter:
        MetricRegistrySnapshot snapshot1 = registry.snapshot(10l);
        Assert.assertEquals(report(snapshot1),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[10,0]]}]");

        // Test single counter on incremented counter:
        counter1.inc();
        MetricRegistrySnapshot snapshot2 = registry.snapshot(20l);
        Assert.assertEquals(report(snapshot2),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[20,1]]}]");

        // Test a batch of two snapshots:
        Assert.assertEquals(report(snapshot1, snapshot2),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[10,0],[20,1]]}]");

        // Create another counter and test:
        LongCounter counter2 = new LongCounter();
        registry.register("counter2", tags, counter2);
        MetricRegistrySnapshot snapshot3 = registry.snapshot(30l);
        Assert.assertEquals(report(snapshot3),
                "[{\"name\":\"counter2\",\"columns\":[\"time\",\"value\"],\"points\":[[30,0]]}" +
                        ",{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[30,1]]}]");
    }

    @Test
    public void testGauges() {
        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();

        MyGauge gauge1 = new MyGauge();
        registry.register("gauge1", tags, gauge1);

        // Test single gauge on empty gauge:
        MetricRegistrySnapshot snapshot1 = registry.snapshot(10l);
        Assert.assertEquals(report(snapshot1),
                "[{\"name\":\"gauge1\",\"columns\":[\"time\",\"value\"],\"points\":[[10,null]]}]");
        // Test single counter on incremented counter:
        gauge1.setValue("foo");
        MetricRegistrySnapshot snapshot2 = registry.snapshot(20l);
        Assert.assertEquals(report(snapshot2),
                "[{\"name\":\"gauge1\",\"columns\":[\"time\",\"value\"],\"points\":[[20,\"foo\"]]}]");

        // Test a batch of two snapshots:
        Assert.assertEquals(report(snapshot1, snapshot2),
                "[{\"name\":\"gauge1\",\"columns\":[\"time\",\"value\"],\"points\":[[10,null],[20,\"foo\"]]}]");

        // Create another gauge and test:
        MyGauge gauge2 = new MyGauge();
        registry.register("gauge2", tags, gauge2);
        MetricRegistrySnapshot snapshot3 = registry.snapshot(30l);
        Assert.assertEquals(report(snapshot3),
                "[{\"name\":\"gauge1\",\"columns\":[\"time\",\"value\"],\"points\":[[30,\"foo\"]]}" +
                        ",{\"name\":\"gauge2\",\"columns\":[\"time\",\"value\"],\"points\":[[30,null]]}]");
    }

    @Test
    public void testMaxReportLength() {
        final int maxReportLength = 100;
        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();

        LongCounter counter1 = new LongCounter();
        registry.register("counter1", tags, counter1);

        // Test single counter on empty counter:
        MetricRegistrySnapshot snapshot1 = registry.snapshot(10l);
        List<String> report1 = report(maxReportLength, snapshot1);
        Assert.assertEquals(1, report1.size());
        Assert.assertEquals(report1.get(0),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[10,0]]}]");

        // Test single counter on incremented counter:
        counter1.inc();
        MetricRegistrySnapshot snapshot2 = registry.snapshot(20l);
        List<String> report2 = report(maxReportLength, snapshot2);
        Assert.assertEquals(1, report2.size());
        Assert.assertEquals(report2.get(0),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[20,1]]}]");

        // Test a batch of two snapshots:
        List<String> report12 = report(maxReportLength, snapshot1, snapshot2);
        Assert.assertEquals(1, report12.size());
        Assert.assertEquals(report12.get(0),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[10,0],[20,1]]}]");

        // Create another counter and test:
        LongCounter counter2 = new LongCounter();
        registry.register("counter2", tags, counter2);
        MetricRegistrySnapshot snapshot3 = registry.snapshot(30l);
        List<String> report3 = report(maxReportLength, snapshot3);
        Assert.assertEquals(2, report3.size());
        Assert.assertEquals(report3.get(0),
                "[{\"name\":\"counter2\",\"columns\":[\"time\",\"value\"],\"points\":[[30,0]]}]");
        Assert.assertEquals(report3.get(1),
                "[{\"name\":\"counter1\",\"columns\":[\"time\",\"value\"],\"points\":[[30,1]]}]");
    }

    private String report(MetricRegistrySnapshot... snapshots) {
        TestReporter reporter = new TestReporterFactory().create();
        reporter.report(CollectionUtils.toList(snapshots));
        String output = reporter.output.get(0);
        //System.out.println(output);
        Assert.assertEquals(output.length(), output.getBytes().length);
        return output;
    }

    private List<String> report(int maxReportLength, MetricRegistrySnapshot... snapshots) {
        TestReporterFactory factory = new TestReporterFactory();
        factory.setMaxReportLength(maxReportLength);
        TestReporter reporter = factory.create();
        reporter.report(CollectionUtils.toList(snapshots));
        return reporter.output;
    }

    private MetricTags createTags() {
        Map<String, Object> tags = new HashMap<String, Object>();
        tags.put("foo", "bar");
        return new MetricTags(tags);
    }

    private static class MyGauge extends Gauge<String> {

        private String value;

        @Override
        public String getValue() {
            return value;
        }

        public MyGauge setValue(String value) {
            this.value = value;
            return this;
        }
    }

    private static class TestReporter extends InfluxDBReporter {
        public final List<String> output = new ArrayList<String>();

        public TestReporter(InfluxDBReporterFactory factory) {
            super(factory);
        }

        @Override
        protected InfluxDBDispatcher createDispatcher(InfluxDBReporterFactory factory) {
            return new InfluxDBDispatcher() {
                @Override
                protected void doSend(String content) throws IOException {
                    output.add(content);
                }
            };
        }
    }

    private static class TestReporterFactory extends InfluxDBReporterFactory {

        @Override
        public TestReporter create() {
            return new TestReporter(this);
        }
    }
}
