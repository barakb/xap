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

package org.openspaces.test.core.metrics;

import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/org/openspaces/test/core/metrics/service-metric-test.xml")
public class CustomMetricsTest {
    @Autowired
    protected ApplicationContext ac;

    protected String[] getConfigLocations() {
        return new String[]{"/org/openspaces/test/core/metrics/service-metric-test.xml"};
    }

    @Test
    public void testServiceMetricAnnotation() throws Exception {
        final MockMetricRegistrator metricRegistrator = (MockMetricRegistrator) ac.getBean("metricRegistrator");

        Assert.assertEquals(0, metricRegistrator.metrics.size());
        Assert.assertEquals(2, metricRegistrator.children.size());

        MockMetricRegistrator bean1Metrics = metricRegistrator.children.get("myBean");
        Assert.assertEquals(2, bean1Metrics.metrics.size());
        Assert.assertEquals(0, bean1Metrics.children.size());
        Gauge fooMetric1 = (Gauge) bean1Metrics.metrics.get("foo");
        Assert.assertNotNull(fooMetric1);
        LongCounter barMetric1 = (LongCounter) bean1Metrics.metrics.get("bar");
        Assert.assertNotNull(barMetric1);

        MockMetricRegistrator bean2Metrics = metricRegistrator.children.get("custom-name");
        Assert.assertEquals(2, bean2Metrics.metrics.size());
        Assert.assertEquals(0, bean2Metrics.children.size());
        Gauge fooMetric2 = (Gauge) bean2Metrics.metrics.get("foo2");
        Assert.assertNotNull(fooMetric2);
        LongCounter barMetric2 = (LongCounter) bean2Metrics.metrics.get("bar2");
        Assert.assertNotNull(barMetric2);

        Assert.assertEquals(0, fooMetric1.getValue());
        Assert.assertEquals(0, barMetric1.getCount());
        Assert.assertEquals(0, fooMetric2.getValue());
        Assert.assertEquals(0, barMetric2.getCount());

        final MetricsBean metricsBean1 = (MetricsBean) ac.getBean("myBean");
        metricsBean1.setFoo(7);
        metricsBean1.getBar().inc();
        Assert.assertEquals(7, fooMetric1.getValue());
        Assert.assertEquals(1, barMetric1.getCount());

        final MetricsBean2 metricsBean2 = (MetricsBean2) ac.getBean("myBean2");
        metricsBean2.foo.set(77);
        metricsBean2.bar.inc();
        Assert.assertEquals(77, fooMetric2.getValue());
        Assert.assertEquals(1, barMetric2.getCount());

    }
}
