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

import com.gigaspaces.metrics.MetricReporter;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class InfluxDBReporterFactoryTests {

    @Test
    public void versionTest() {
        InfluxDBReporterFactory factory = new InfluxDBReporterFactory();
        Properties properties = new Properties();
        properties.setProperty("host", "foo");
        properties.setProperty("database", "mydb");

        // Test default:
        factory.load(properties);
        MetricReporter reporter = factory.create();
        Assert.assertThat(reporter, CoreMatchers.instanceOf(com.gigaspaces.metrics.influxdb.v_08.InfluxDBReporter.class));

        // Test latest:
        properties.setProperty("version", "latest");
        factory.load(properties);
        reporter = factory.create();
        Assert.assertThat(reporter, CoreMatchers.instanceOf(InfluxDBReporter.class));

        // Test 0.8:
        properties.setProperty("version", "0.8");
        factory.load(properties);
        reporter = factory.create();
        Assert.assertThat(reporter, CoreMatchers.instanceOf(com.gigaspaces.metrics.influxdb.v_08.InfluxDBReporter.class));

        // Test 0.9:
        properties.setProperty("version", "0.9");
        factory.load(properties);
        reporter = factory.create();
        Assert.assertThat(reporter, CoreMatchers.instanceOf(InfluxDBReporter.class));

        // Test unsupported:
        properties.setProperty("version", "0.7");
        factory.load(properties);
        try {
            factory.create();
            Assert.fail("Should fail - unsupported version");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Unsupported version - 0.7", e.getMessage());
        }
    }
}
