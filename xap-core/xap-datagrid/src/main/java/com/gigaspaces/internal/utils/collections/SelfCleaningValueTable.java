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

import com.gigaspaces.internal.utils.concurrent.GSThread;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds a weak reference for each value and make sure each value will be cleaned when is weakly
 * held.
 *
 * @param <K> key type
 * @param <V> value type
 * @author Guy Korland
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SelfCleaningValueTable<K, V> implements ConcurrentMap<K, V> {
    // Maps a each key to its weak value
    final private ConcurrentHashMap<K, ValueHolder<K, V>> _table;
    // collected weak references
    final private ReferenceQueue<V> _freeEntryQueue;
    // Cleans the weakly held values
    final private Cleaner<K, V> _cleanerThread;


    /**
     * Creates a new Table and start a cleaner.
     */
    public SelfCleaningValueTable() {
        _table = new ConcurrentHashMap<K, ValueHolder<K, V>>();
        _freeEntryQueue = new ReferenceQueue<V>();

        _cleanerThread = new Cleaner<K, V>("SelfCleaningValueTable", _freeEntryQueue, _table);
        // null the context class loader, so there will be no reference for it (usually used in static blocks
        // and assuming that the context class loader is not needed with the thread).
        _cleanerThread.setContextClassLoader(null);
        _cleanerThread.start();
    }

    /*
     * @see java.util.Map#clear()
     */
    public void clear() {
        _table.clear();
    }

    /*
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public V put(K key, V value) {
        return extractValue(_table.put(key, createValueHolder(key, value)));
    }

    /*
     * @see java.util.Map#get(java.lang.Object)
     */
    public V get(Object key) {
        return extractValue(_table.get(key));
    }

    private V extractValue(ValueHolder<K, V> holder) {
        return holder == null ? null : holder.get();
    }

    /*
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();

   	/* Closes this table by terminating its cleaner Thread and cleaning
        * all values stored.
   	 */
        _cleanerThread.interrupt();
        _table.clear();
    }

    /*
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        return _table.containsKey(key);
    }

    /*
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        return _table.containsKey(value);
    }

    /*
     * @see java.util.Map#entrySet()
     */
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return _table.isEmpty();
    }

    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /*
     * @see java.util.Map#remove(java.lang.Object)
     */
    public V remove(Object key) {
        return extractValue(_table.remove(key));
    }

    /*
     * @see java.util.Map#size()
     */
    public int size() {
        return _table.size();
    }

    /*
     * @see java.util.Map#values()
     */
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
     */
    public V putIfAbsent(K key, V value) {
        return extractValue(_table.putIfAbsent(key, createValueHolder(key, value)));
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
     */
    public boolean remove(Object key, Object value) {
        return _table.remove(key, createValueHolder((K) key, (V) value));
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
     */
    public V replace(K key, V value) {
        return extractValue(_table.replace(key, createValueHolder(key, value)));
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public boolean replace(K key, V oldValue, V newValue) {
        return _table.replace(key, createValueHolder(key, oldValue), createValueHolder(key, newValue));
    }

    /**
     * Creates a new KeyHolder that connected to a ReferenceQueue.
     *
     * @return new KeyHolder
     */
    private ValueHolder<K, V> createValueHolder(K key, V value) {
        return new ValueHolder<K, V>(key, value, _freeEntryQueue);
    }

    /**
     * Cleaning Thread, clean all the EntryInfos of the collected Objects.
     */
    final static private class Cleaner<K, V> extends GSThread {
        final private ReferenceQueue<V> _queue;
        final private Map<K, ValueHolder<K, V>> _table;

        /**
         * Creates a new cleaner.
         *
         * @param queue      the queue that holds the entries to clean.
         * @param entryInfos the Map to clean.
         */
        public Cleaner(String parentName, ReferenceQueue<V> queue, Map<K, ValueHolder<K, V>> entryInfos) {
            super(parentName + "$Cleaner");
            _queue = queue;
            _table = entryInfos;
            setDaemon(true);
        }

        /**
         * Block on the queue until an entry is been cleaned by the GC
         */
        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    ValueHolder<K, V> ref = (ValueHolder<K, V>) _queue.remove(); // block op'
                    _table.remove(ref.getKey()); // clean the _table table
                } catch (InterruptedException e) {

                    //Restore the interrupted status
                    interrupt();

                    //fall through
                    break;
                }
            }
        }
    }


    /**
     * Value in the SelfCleaningValueTable.
     *
     * @param <T> the type of the Object that need be saved as value.
     * @param <K> the type of the Object that need be saved as Key.
     * @author Guy Korland
     * @since 6.5
     */
    private static class ValueHolder<K, T> extends WeakReference<T> {
        final private K _key;

        /**
         * Creates a new value for the {@link SelfCleaningValueTable}.
         *
         * @param referent reference for the Object that should be saved
         * @param queue    Queue for the cleaner
         */
        public ValueHolder(K key, T referent, ReferenceQueue<T> queue) {
            super(referent, queue);
            _key = key;
        }

        public K getKey() {
            return _key;
        }

        @Override
        public boolean equals(Object o) {
            ValueHolder<K, T> other = (ValueHolder<K, T>) o;
            T otherValue = other.get();
            T value = get();
            return value != null && otherValue != null && value.equals(otherValue);
        }
    }

}
