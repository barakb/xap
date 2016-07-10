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

import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.ServiceMetric;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricsBean {

    private final AtomicInteger foo = new AtomicInteger();
    private final LongCounter bar = new LongCounter();

    @ServiceMetric(name = "foo")
    public Integer getFoo() {
        return foo.get();
    }

    public void setFoo(int value) {
        foo.set(value);
    }

    @ServiceMetric(name = "bar")
    public LongCounter getBar() {
        return bar;
    }
}
