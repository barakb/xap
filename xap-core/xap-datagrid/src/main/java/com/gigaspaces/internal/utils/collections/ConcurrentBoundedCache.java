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


package com.gigaspaces.internal.utils.collections;

import com.j_spaces.kernel.SystemProperties;

import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Concurrent bounded cache. It uses a ConcurrentHashMap and a counter to set the upper bound to the
 * cache size. If one of the threads reaches the upperBound, the cache is cleared.
 *
 * @author Boris
 * @version 1.0
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class ConcurrentBoundedCache<Key, Value>
        implements Map<Key, Value> {
    // Concurrent map to store the values
    protected volatile ConcurrentHashMap<Key, SoftReference<Value>> _map;
    protected AtomicInteger approximateSize = new AtomicInteger(0);
    protected static final Object clearLock = new Object();
    protected static final Object lazyLock = new Object();
    protected final static long upperBound = Long.getLong(SystemProperties.BOUNDED_QUERY_CACHE_SIZE, SystemProperties.BOUNDED_QUERY_CACHE_SIZE_DEFAULT);
    protected final static boolean enabled = upperBound > 0;

    /**
     * Default constructor. Constructs a new empty cache.
     */
    public ConcurrentBoundedCache() {
    }

    /**
     * Lazy initialization of the map
     */
    private void initialize() {
        if (_map == null) {
            synchronized (lazyLock) {
                if (_map == null) {
                    _map = new ConcurrentHashMap<Key, SoftReference<Value>>();
                }
            }
        }
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#put
     * Note: if upperBound is reached, clear the cache
     */
    public Value put(Key key, Value value) {
        if (!enabled)
            return null;
        initialize();
        SoftReference<Value> prev;
        if (approximateSize.getAndIncrement() >= upperBound) {
            synchronized (clearLock) {
                if (approximateSize.get() >= upperBound) {
                    clear();
                    approximateSize.set(1);
                }
            }
        }
        SoftReference<Value> wrappedValue = new SoftReference<Value>(value);
        prev = _map.putIfAbsent(key, wrappedValue);
        if (prev != null) {
            return prev.get();
        } else {
            return null;
        }
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#get
     */
    public Value get(Object key) {
        if (!enabled)
            return null;
        initialize();
        SoftReference<Value> valueSoftReference = _map.get(key);
        if (valueSoftReference != null) {
            return valueSoftReference.get();
        } else {
            return null;
        }
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#remove
     */
    public Value remove(Object key) {
        if (!enabled)
            return null;
        initialize();
        SoftReference<Value> remove = _map.remove(key);
        approximateSize.decrementAndGet();
        if (remove != null) {
            return remove.get();
        } else {
            return null;
        }
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#containsKey()
     */
    public boolean containsKey(Object key) {
        if (!enabled)
            return false;
        return _map.containsKey(key);
    }


    /*
     * @see java.util.concurrent.ConcurrentHashMap##isEmpty()
     */
    public boolean isEmpty() {
        if (!enabled)
            return true;
        return _map.isEmpty();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap##keySet()
     */
    public Set<Key> keySet() {
        if (!enabled)
            return Collections.emptySet();
        return _map.keySet();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap##size()
     */
    public int size() {
        if (!enabled)
            return 0;
        return _map.size();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#clear()
     */
    public void clear() {
        if (!enabled)
            return;
        _map = new ConcurrentHashMap<Key, SoftReference<Value>>();
        approximateSize.set(0);
    }

    /*
     * Unsupported
     */
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    /*
     * Unsupported
     */
    public Set<java.util.Map.Entry<Key, Value>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * Unsupported
     */
    public void putAll(Map<? extends Key, ? extends Value> t) {
        throw new UnsupportedOperationException();

    }

    /*
     * Unsupported
     */
    public Collection<Value> values() {
        throw new UnsupportedOperationException();
    }

}