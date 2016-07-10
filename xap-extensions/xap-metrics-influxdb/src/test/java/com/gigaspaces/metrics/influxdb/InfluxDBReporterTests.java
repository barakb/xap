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

package com.gigaspaces.metrics.influxdb;

import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistry;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricTags;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class InfluxDBReporterTests {
    private static final String END_OF_METRIC = "\n";

    @Test
    public void testCounters() {
        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();

        LongCounter counter1 = new LongCounter();
        registry.register("counter1", tags, counter1);

        // Test single counter on empty counter:
        MetricRegistrySnapshot snapshot1 = registry.snapshot(10l);
        Assert.assertEquals(report(snapshot1), "counter1,foo=bar value=0i 10");

        // Test single counter on incremented counter:
        counter1.inc();
        MetricRegistrySnapshot snapshot2 = registry.snapshot(20l);
        Assert.assertEquals(report(snapshot2), "counter1,foo=bar value=1i 20");

        // Test a batch of two snapshots:
        Assert.assertEquals(report(snapshot1, snapshot2),
                "counter1,foo=bar value=0i 10" + END_OF_METRIC + "counter1,foo=bar value=1i 20");

        // Create another counter and test:
        LongCounter counter2 = new LongCounter();
        registry.register("counter2", tags, counter2);
        MetricRegistrySnapshot snapshot3 = registry.snapshot(30l);
        Assert.assertEquals(report(snapshot3),
                "counter2,foo=bar value=0i 30" + END_OF_METRIC + "counter1,foo=bar value=1i 30");
    }

    @Test
    public void testGauges() {
        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();

        MyGauge gauge1 = new MyGauge();
        registry.register("gauge1", tags, gauge1);

        // Test single gauge on empty gauge:
        gauge1.setValue("");
        MetricRegistrySnapshot snapshot1 = registry.snapshot(10l);
        Assert.assertEquals(report(snapshot1), "gauge1,foo=bar value=\"\" 10");

        // Test single counter on incremented counter:
        gauge1.setValue("foo");
        MetricRegistrySnapshot snapshot2 = registry.snapshot(20l);
        Assert.assertEquals(report(snapshot2), "gauge1,foo=bar value=\"foo\" 20");

        // Test a batch of two snapshots:
        Assert.assertEquals(report(snapshot1, snapshot2), "gauge1,foo=bar value=\"\" 10" + END_OF_METRIC +
                "gauge1,foo=bar value=\"foo\" 20");

        // Create another gauge and test:
        MyGauge gauge2 = new MyGauge();
        gauge2.setValue("bar");
        registry.register("gauge2", tags, gauge2);
        MetricRegistrySnapshot snapshot3 = registry.snapshot(30l);
        Assert.assertEquals(report(snapshot3), "gauge1,foo=bar value=\"foo\" 30" + END_OF_METRIC +
                "gauge2,foo=bar value=\"bar\" 30");
    }

    @Test
    public void testMaxReportLength() {
        final int maxReportLength = 70;
        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();

        LongCounter counter1 = new LongCounter();
        registry.register("counter1", tags, counter1);

        // Test single counter on empty counter:
        MetricRegistrySnapshot snapshot1 = registry.snapshot(10l);
        List<String> report1 = report(maxReportLength, snapshot1);
        Assert.assertEquals(1, report1.size());
        Assert.assertEquals(report1.get(0), "counter1,foo=bar value=0i 10");

        // Test single counter on incremented counter:
        counter1.inc();
        MetricRegistrySnapshot snapshot2 = registry.snapshot(20l);
        List<String> report2 = report(maxReportLength, snapshot2);
        Assert.assertEquals(1, report2.size());
        Assert.assertEquals(report2.get(0), "counter1,foo=bar value=1i 20");

        // Test a batch of two snapshots:
        List<String> report12 = report(maxReportLength, snapshot1, snapshot2);
        Assert.assertEquals(1, report12.size());
        Assert.assertEquals(report12.get(0), "counter1,foo=bar value=0i 10" + END_OF_METRIC +
                "counter1,foo=bar value=1i 20");

        // Create another counter and test:
        LongCounter counter2 = new LongCounter();
        registry.register("counter2", tags, counter2);
        MetricRegistrySnapshot snapshot3 = registry.snapshot(30l);
        List<String> report3 = report(maxReportLength, snapshot1, snapshot3);
        Assert.assertEquals(2, report3.size());
        Assert.assertEquals(report3.get(0), "counter1,foo=bar value=0i 10" + END_OF_METRIC + "counter2,foo=bar value=0i 30");
        Assert.assertEquals(report3.get(1), "counter1,foo=bar value=1i 30");
    }

    @Test
    public void testValueTypes() {
        final AtomicReference<Object> value = new AtomicReference<Object>();
        Gauge<Object> gauge = new Gauge<Object>() {
            @Override
            public Object getValue() throws Exception {
                return value.get();
            }
        };

        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();
        registry.register("gauge1", tags, gauge);

        // Test Integer numeric types:
        value.set(new Byte("1"));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=1i 10");
        value.set(new Short("2"));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=2i 10");
        value.set(new Integer(3));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=3i 10");
        value.set(new Long(4));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=4i 10");

        // Test descimal numeric types:
        value.set(new Float(5));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=5.0 10");
        value.set(new Float(5.1));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=5.1 10");
        value.set(new Double(6));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=6.0 10");
        value.set(new Double(6.1));
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=6.1 10");

        // Test boolean:
        value.set(Boolean.FALSE);
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=F 10");
        value.set(Boolean.TRUE);
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=T 10");

        // Test string:
        value.set("");
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=\"\" 10");
        value.set("foo");
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=\"foo\" 10");
        value.set("foo bar");
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=\"foo bar\" 10");
        value.set("foo=bar");
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=\"foo=bar\" 10");
        value.set("foo='bar'");
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=\"foo='bar'\" 10");
        value.set("foo=\"bar\"");
        Assert.assertEquals(report(registry.snapshot(10l)), "gauge1,foo=bar value=\"foo=\\\"bar\\\"\" 10");

        // Test null:
        value.set(null);
        try {
            report(registry.snapshot(10l));
            Assert.fail("Should have failed - reporting null is not supported.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("InfluxDB does not support null values", e.getMessage());
        }

        // Test unsupported numeric type:
        value.set(new java.math.BigDecimal("0"));
        try {
            report(registry.snapshot(10l));
            Assert.fail("Should have failed - unsupported numeric class.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Unsupported Number class - java.math.BigDecimal", e.getMessage());
        }

        // Test unsupported numeric type:
        value.set(new java.util.Date());
        try {
            report(registry.snapshot(10l));
            Assert.fail("Should have failed - unsupported class.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Unsupported value class: java.util.Date", e.getMessage());
        }
    }

    @Test
    public void testConfiguration() {
        Properties properties = new Properties();

        // Test minimal properties (http by default):
        properties.setProperty("host", "foo");
        properties.setProperty("database", "mydb");
        testHttpConfig(properties, "http://foo:8086/write?db=mydb");

        // Test all http properties:
        properties.clear();
        properties.setProperty("protocol", "http");
        properties.setProperty("host", "foo");
        properties.setProperty("port", "1234");
        properties.setProperty("database", "mydb");
        properties.setProperty("retention-policy", "bar");
        properties.setProperty("username", "john");
        properties.setProperty("password", "shhh");
        properties.setProperty("precision", "ms");
        properties.setProperty("consistency", "quorum");
        testHttpConfig(properties, "http://foo:1234/write?db=mydb&rp=bar&u=john&p=shhh&precision=ms&consistency=quorum");

        // Test minimal udp properties:
        properties.clear();
        properties.setProperty("protocol", "udp");
        properties.setProperty("host", "foo");
        testUdpConfig(properties, "foo", 4444);

        // Test all udp properties:
        properties.clear();
        properties.setProperty("protocol", "udp");
        properties.setProperty("host", "foo");
        properties.setProperty("port", "1234");
        testUdpConfig(properties, "foo", 1234);

    }

    private void testHttpConfig(Properties properties, String expectedUrl) {
        InfluxDBReporterFactory factory = new InfluxDBReporterFactory();
        factory.load(properties);
        InfluxDBReporter reporter = new InfluxDBReporter(factory);
        InfluxDBHttpDispatcher httpDispatcher = (InfluxDBHttpDispatcher) reporter.getDispatcher();
        Assert.assertEquals(expectedUrl, httpDispatcher.getUrl().toString());
    }

    private void testUdpConfig(Properties properties, String expectedHost, int expectedPort) {
        InfluxDBReporterFactory factory = new InfluxDBReporterFactory();
        factory.load(properties);
        InfluxDBReporter reporter = new InfluxDBReporter(factory);
        InfluxDBUdpDispatcher udpDispatcher = (InfluxDBUdpDispatcher) reporter.getDispatcher();
        try {
            Assert.assertEquals(expectedHost, udpDispatcher.getConnection().getAddress().getHostName());
            Assert.assertEquals(expectedPort, udpDispatcher.getConnection().getAddress().getPort());
        } finally {
            udpDispatcher.close();
        }
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
