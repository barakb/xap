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
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class CyclicAtomicInteger {
    private final AtomicInteger _value;
    private final int maxValue;

    public CyclicAtomicInteger(int maxValue) {
        this.maxValue = maxValue;
        this._value = new AtomicInteger();
    }

    public int getMaxValue() {
        return maxValue;
    }

    public final int get() {
        return _value.get();
    }

    public final int incrementAndGet() {
        for (; ; ) {
            int current = _value.get();
            int next = current == maxValue ? 0 : current + 1;
            if (_value.compareAndSet(current, next))
                return next;
        }
    }

    public final int getAndIncrement() {
        for (; ; ) {
            int current = _value.get();
            int next = current == maxValue ? 0 : current + 1;
            if (_value.compareAndSet(current, next))
                return current;
        }
    }

	/*
    private static final Unsafe unsafe = Unsafe.getUnsafe();
    private static final long valueOffset;

    static
    {
        try
        {
        	valueOffset = unsafe.objectFieldOffset(CyclicAtomicInteger.class.getDeclaredField("value"));
        }
        catch (Exception ex)
        {
        	throw new Error(ex);
        }
	}

    private volatile int value;

    public final int get()
    {
        return value;
    }

    public final int incrementAndGet()
    {
        for (;;)
        {
            int current = get();
            int next = current == maxValue ? 0 : current + 1;
            if (compareAndSet(current, next))
                return next;
        }
    }
    
    private final boolean compareAndSet(int expect, int update)
    {
    	return unsafe.compareAndSwapInt(this, valueOffset, expect, update);
    }
    */
}
