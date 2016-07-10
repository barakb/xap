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

/**
 * A gauge metric is an instantaneous reading of a particular value. To instrument a queue's depth,
 * for example:<br>
 * <pre><code>
 * final Queue&lt;String&gt; queue = new ConcurrentLinkedQueue&lt;String&gt;();
 * final Gauge&lt;Integer&gt; queueDepth = new Gauge&lt;Integer&gt;() {
 *     public Integer getValue() {
 *         return queue.size();
 *     }
 * };
 * </code></pre>
 *
 * @param <T> the type of the metric's value
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class Gauge<T> extends Metric {
    /**
     * Returns the metric's current value.
     *
     * @return the metric's current value
     */
    public abstract T getValue() throws Exception;

    protected double calculatePercent(long value, long total) {
        return total == 0 ? -1.0 : ((double) value) / total * 100;
    }

    protected static Double validate(double value) {
        return Double.isNaN(value) || Double.isInfinite(value) ? -1.0 : value;
    }
}
