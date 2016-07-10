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

package com.gigaspaces.internal.utils.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a segmented counter while the get() returns an accurate result.
 *
 * @author Guy
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class SegmentedAtomicInteger extends Number {
    private static final long serialVersionUID = 431083030353672207L;

    final private AtomicInteger[] counters;
    final private int _initialValue;

    public SegmentedAtomicInteger() {
        this(0);
    }

    public SegmentedAtomicInteger(int initialValue) {
        this(initialValue, 13);
    }

    public SegmentedAtomicInteger(int initialValue, int concurrencyLevel) {
        _initialValue = initialValue;
        counters = new AtomicInteger[concurrencyLevel];
        for (int i = 0; i < counters.length; ++i) {
            counters[i] = new AtomicInteger();
        }
        counters[0].set(initialValue);
    }

    public void decrement() {
        getCounter().decrementAndGet();
    }

    public void increment() {
        getCounter().incrementAndGet();
    }

    public void add(int delta) {
        getCounter().addAndGet(delta);
    }

    public int decrementAndGet() {
        decrement();
        return get();
    }

    public int incrementAndGet() {
        increment();
        return get();
    }

    public int addAndGet(int delta) {
        add(delta);
        return get();
    }

    public int get() {
        int size = 0;
        for (AtomicInteger counter : counters) {
            size += counter.get();
        }
        return size;
    }

    @Override
    public double doubleValue() {
        return get();
    }

    @Override
    public float floatValue() {
        return get();
    }

    @Override
    public int intValue() {
        return get();
    }

    @Override
    public long longValue() {
        return get();
    }

    /**
     * Reset the counter in a non atomic fashion, at a quiscient state this method will reset the
     * counter to its initial value, but in a concurrent state, interleaving operations affect is
     * not deterministic and may be overridden or accumulated. A normal use would be a reset of
     * statistic counter that the precise accumulation of statistics is not mandatory.
     */
    public void reset() {
        for (int i = 0; i < counters.length; ++i) {
            counters[i].set(0);
        }
        counters[0].set(_initialValue);
    }

    private AtomicInteger getCounter() {
        int index = (int) Thread.currentThread().getId() % counters.length;
        return counters[index];
    }
}
