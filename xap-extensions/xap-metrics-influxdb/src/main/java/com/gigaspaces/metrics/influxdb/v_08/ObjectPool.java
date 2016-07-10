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

import java.lang.reflect.Array;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class ObjectPool<T> {
    private T[] pool;
    private int remaining;

    public ObjectPool<T> initialize(int capacity) {
        if (pool == null || pool.length != capacity) {
            this.pool = createPool(capacity);
            this.remaining = capacity;
        }
        return this;
    }

    public T acquire() {
        if (remaining > 0) {
            remaining--;
            T result = pool[remaining];
            pool[remaining] = null;
            return result;
        }
        return newInstance();
    }

    public void release(T instance) {
        if (remaining < pool.length)
            pool[remaining++] = instance;
    }

    protected T[] createPool(int capacity) {
        T instance = newInstance();
        T[] pool = (T[]) Array.newInstance(instance.getClass(), capacity);
        pool[0] = instance;
        for (int i = 1; i < capacity; i++)
            pool[i] = newInstance();
        return pool;
    }

    protected abstract T newInstance();
}
