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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides a ConcurrentMap implementation. This implementation is based on CopyOnUpdate for each
 * modifying operation. However the internal map is not volatile, meaning a modification may not be
 * visible to a following read, hence this map should be used when in such cases the upper layer
 * would insert the same value under this key that the was originally inserted, such as one time
 * cache initialization which always ends up with the same result.
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class CopyOnUpdateOnceMap<K, V>
        implements ConcurrentMap<K, V> {
    private Map<K, V> _map = new HashMap<K, V>();

    public void clear() {
        _map = new HashMap<K, V>();
    }

    public boolean containsKey(Object key) {
        return _map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return _map.containsValue(value);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return _map.entrySet();
    }

    public V get(Object key) {
        return _map.get(key);
    }

    public boolean isEmpty() {
        return _map.isEmpty();
    }

    public Set<K> keySet() {
        return _map.keySet();
    }

    public synchronized V put(K key, V value) {
        Map<K, V> copyMap = copyMap();
        V put = copyMap.put(key, value);
        _map = copyMap;
        return put;
    }

    public synchronized void putAll(Map<? extends K, ? extends V> t) {
        Map<K, V> copyMap = copyMap();
        copyMap.putAll(t);
        _map = copyMap;
    }

    public synchronized V remove(Object key) {
        Map<K, V> copyMap = copyMap();
        V remove = copyMap.remove(key);
        _map = copyMap;
        return remove;
    }

    public int size() {
        return _map.size();
    }

    public Collection<V> values() {
        return _map.values();
    }

    private Map<K, V> copyMap() {
        return new HashMap<K, V>(_map);
    }

    public synchronized V putIfAbsent(K key, V value) {
        V v = _map.get(key);
        if (v == null) {
            Map<K, V> copyMap = copyMap();
            copyMap.put(key, value);
            _map = copyMap;
        }
        return v;
    }

    public synchronized boolean remove(Object key, Object value) {
        V v = _map.get(key);
        if (v != null && v.equals(value)) {
            Map<K, V> copyMap = copyMap();
            copyMap.remove(key);
            _map = copyMap;
            return true;
        }
        return false;
    }

    public synchronized V replace(K key, V value) {
        V v = _map.get(key);
        if (v != null) {
            Map<K, V> copyMap = copyMap();
            copyMap.put(key, value);
            _map = copyMap;
        }
        return v;
    }

    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V v = _map.get(key);
        if (v != null && v.equals(oldValue)) {
            Map<K, V> copyMap = copyMap();
            copyMap.put(key, newValue);
            _map = copyMap;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return _map.toString();
    }

}
