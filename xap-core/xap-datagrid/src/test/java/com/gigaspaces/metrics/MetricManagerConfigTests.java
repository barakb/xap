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

import com.gigaspaces.metrics.reporters.ConsoleReporterFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

@com.gigaspaces.api.InternalApi
public class MetricManagerConfigTests {

    @Test
    public void loadAbsentFile() {
        final String fileName = "no-such-file.xml";
        MetricManagerConfig mc = MetricManagerConfig.loadFromXml(fileName);
        Assert.assertTrue(mc.getReportersFactories().isEmpty());
    }

    @Test
    public void loadInvalidXmlFile() {
        final String fileName = this.getClass().getResource("metrics-not-xml.xml").getFile();
        try {
            MetricManagerConfig.loadFromXml(fileName);
        } catch (RuntimeException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
            if (!e.getMessage().contains(fileName))
                Assert.fail("Error message does not contain filename: " + e.getMessage());
        }
    }

    @Test
    public void loadEmpty() {
        final String fileName = this.getClass().getResource("metrics-empty.xml").getFile();
        MetricManagerConfig config = MetricManagerConfig.loadFromXml(fileName);
        Assert.assertEquals(0, config.getReportersFactories().size());
        Assert.assertEquals(2, config.getSamplersConfig().size());
        Assert.assertEquals(0, config.getSamplersConfig().get("off").getSamplingRate());
        Assert.assertEquals(MetricSamplerConfig.DEFAULT_SAMPLING_RATE, config.getSamplersConfig().get("default").getSamplingRate());
    }

    @Test
    public void loadSamplers() {
        final String fileName = this.getClass().getResource("metrics-samplers.xml").getFile();
        MetricManagerConfig config = MetricManagerConfig.loadFromXml(fileName);
        Assert.assertEquals(4, config.getSamplersConfig().size());
        Assert.assertEquals(0, config.getSamplersConfig().get("off").getSamplingRate());
        Assert.assertEquals(TimeUnit.MINUTES.toMillis(2), config.getSamplersConfig().get("default").getSamplingRate());
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMillis(1), config.getSamplersConfig().get("foo1").getSamplingRate());
        Assert.assertEquals(TimeUnit.SECONDS.toMillis(1), config.getSamplersConfig().get("foo2").getSamplingRate());
    }

    @Test
    public void loadReporters() {
        final String fileName = this.getClass().getResource("metrics-reporters.xml").getFile();
        MetricManagerConfig config = MetricManagerConfig.loadFromXml(fileName);
        Assert.assertEquals(2, config.getReportersFactories().size());

        MetricReporterFactory console = config.getReportersFactories().get("console");
        Assert.assertTrue(console instanceof ConsoleReporterFactory);

        MetricReporterFactory foo = config.getReportersFactories().get("foo");
        Assert.assertTrue(foo instanceof MyCustomReporterFactory);
        Assert.assertEquals("test", ((MyCustomReporterFactory) foo).bar);
    }

    @Test
    public void loadMetrics() {
        final String fileName = this.getClass().getResource("metrics-metrics.xml").getFile();
        MetricManagerConfig config = MetricManagerConfig.loadFromXml(fileName);
        Assert.assertEquals("default", config.getPatternSet().findBestMatch("foo"));
        Assert.assertEquals("custom1", config.getPatternSet().findBestMatch("xap"));
        Assert.assertEquals("custom1", config.getPatternSet().findBestMatch("xap_foo"));
        Assert.assertEquals("custom2", config.getPatternSet().findBestMatch("xap_*_*_memory"));
        Assert.assertEquals("custom2", config.getPatternSet().findBestMatch("xap_*_*_memory_max"));
        Assert.assertEquals("custom2", config.getPatternSet().findBestMatch("xap_foo_bar_memory"));
        Assert.assertEquals("custom2", config.getPatternSet().findBestMatch("xap_foo_bar_memory_max"));
    }
}
