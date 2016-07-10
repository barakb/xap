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

package com.j_spaces.kernel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends ConcurrentHashMap, keeping AtomicInteger as size counter. Notice that when using
 * <code>clear()</code> the size can be incorrect.
 *
 * @author Guy Korland
 */
@com.gigaspaces.api.InternalApi
public class SizeConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
    private static final long serialVersionUID = -1322501772526603179L;

    final private AtomicInteger _size = new AtomicInteger();

    public SizeConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
        super(initialCapacity, loadFactor, concurrencyLevel);
    }

    public SizeConcurrentHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public SizeConcurrentHashMap() {
        super();
    }

    public SizeConcurrentHashMap(Map<K, V> t) {
        super(t);
    }

    @Override
    final public V putIfAbsent(K key, V value) {
        V o = super.putIfAbsent(key, value);
        if (o == null) {
            _size.incrementAndGet();
        }
        return o;
    }

    @Override
    final public V put(K key, V value) {
        V o = super.put(key, value);
        if (o == null) {
            _size.incrementAndGet();
        }
        return o;
    }

    @Override
    final public V remove(Object key) {
        V o = super.remove(key);
        if (o != null) {
            _size.decrementAndGet();
        }
        return o;
    }

    @Override
    final public boolean remove(Object key, Object value) {
        if (super.remove(key, value)) {
            _size.decrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    final public int size() {
        return _size.get();
    }

    @Override
    final public void clear() {
        super.clear();
        _size.set(super.size());
    }

}
