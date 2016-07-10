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

import com.gigaspaces.metrics.BeanMetricManager;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;

import org.openspaces.pu.container.ProcessingUnitContainerContext;
import org.openspaces.pu.container.ProcessingUnitContainerContextAware;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricsBean2 implements ProcessingUnitContainerContextAware {

    public final AtomicInteger foo = new AtomicInteger();
    public final LongCounter bar = new LongCounter();

    private BeanMetricManager metricsManager;

    @Override
    public void setProcessingUnitContainerContext(ProcessingUnitContainerContext processingUnitContainerContext) {
        this.metricsManager = processingUnitContainerContext.createBeanMetricManager("custom-name");
        this.metricsManager.register("foo2", new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return foo.get();
            }
        });
        this.metricsManager.register("bar2", bar);
    }
}
