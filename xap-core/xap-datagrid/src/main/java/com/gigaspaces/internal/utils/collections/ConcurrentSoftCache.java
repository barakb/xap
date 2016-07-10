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

import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Soft reference concurrent cache. It uses a ConcurrentHashMap and a soft reference for the
 * values.
 *
 * An entry in a <tt>ConcurrentSoftCache</tt> will be removed on JVM memory shortage.
 *
 * A double-reference is used to store the values, so that the finalize() method can be used to
 * remove the entry from cache when it is collected by the garbage collector.
 *
 * <i>Note</i> The entry will be removed even if there is a hard reference to it somewhere, since
 * the double-reference is used.
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 * @deprecated Since 10.1.0 - use ConcurrentBoundedSoftCache instead
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class ConcurrentSoftCache<Key, Value>
        implements Map<Key, Value> {
    // Concurrent map to store the values with soft double reference
    protected final ConcurrentHashMap<Key, SoftReference<DoubleRef<Key, Value>>> _map;

    /**
     * Default constructor. Constructs a new empty cache.
     */
    public ConcurrentSoftCache() {
        _map = new ConcurrentHashMap<Key, SoftReference<DoubleRef<Key, Value>>>();
    }


    /*
     * @see java.util.concurrent.ConcurrentHashMap#put
     */
    public Value put(Key key, Value value) {
        return dereference(_map.put(key, reference(key, value)));
    }


    /**
     * Returns the value to which the specified key is mapped in this table.
     *
     * @param key a key in the table.
     * @return the value to which the key is mapped in this table; <tt>null</tt> if the key is not
     * mapped to any value in this table or it was already garbage collected.
     * @throws NullPointerException if the key is <tt>null</tt>.
     */
    public Value get(Object key) {
        return dereference(_map.get(key));
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#remove(java.lang.Object)
     */
    public Value remove(Object key) {
        return dereference(_map.remove(key));
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#containsKey()
     */
    public boolean containsKey(Object key) {
        return _map.containsKey(key);
    }


    /*
     * @see java.util.concurrent.ConcurrentHashMap#isEmpty()
     */
    public boolean isEmpty() {
        return _map.isEmpty();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#keySet()
     */
    public Set<Key> keySet() {
        return _map.keySet();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#size()
     */
    public int size() {
        return _map.size();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#clear()
     */
    public void clear() {
        _map.clear();
    }


    /*
     * @see java.util.concurrent.ConcurrentHashMap#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#entrySet()
     */
    public Set<java.util.Map.Entry<Key, Value>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#putAll(java.util.Map)
     */
    public void putAll(Map<? extends Key, ? extends Value> t) {
        throw new UnsupportedOperationException();

    }

    /*
     * @see java.util.concurrent.ConcurrentHashMap#values()
     */
    public Collection<Value> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a soft double reference to the object
     */
    private SoftReference<DoubleRef<Key, Value>> reference(Key key, Value value) {
        return new SoftReference<DoubleRef<Key, Value>>(new DoubleRef<Key, Value>(key,
                value));
    }

    /**
     * Extract the referenced value
     *
     * @return the value referenced by ref
     */
    private Value dereference(SoftReference<DoubleRef<Key, Value>> ref) {
        if (ref == null)
            return null;

        DoubleRef<Key, Value> doubleRef = ref.get();
        return doubleRef == null ? null : doubleRef.getValue();
    }

    /**
     * DoubleRef is a wrapper class for the real value. Double referencing allows to clean the
     * object from cache when its garbage collected.
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private final class DoubleRef<K, V> {
        private final K _key;
        private final V _value;

        DoubleRef(K key, V value) {
            _key = key;
            _value = value;
        }

        public K getKey() {
            return _key;
        }

        public V getValue() {
            return _value;
        }

        /**
         * Remove any reference to this object from the cache
         */
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            // when deleting - make sure that the correct reference is deleted,
            // since there might exist several SoftReferences on the same key
            // in case of multi threaded access
            _map.remove(_key, _value);
        }


    }

}