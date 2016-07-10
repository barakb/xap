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
 * Holds a weak key for each value and make sure each value will be cleaned when key is weakly
 * held.
 *
 * @param <K> key type
 * @param <V> value type
 * @author Guy Korland
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SelfCleaningTable<K, V> implements ConcurrentMap<K, V> {

    private interface KeyHolder<T> {
        public T get();
    }

    public interface ICleanerListener<V> {
        void weakEntryRemoved(V value);
    }

    // Maps a each key to its value
    final private ConcurrentMap<KeyHolder<K>, V> _table;
    // collected weak references
    final private ReferenceQueue<K> _freeEntryQueue;
    // Cleans the weakly held keys
    final private Cleaner<K, V> _cleanerThread;
    // Identifying name
    private final String _name;

    public SelfCleaningTable(String name) {
        this(name, null);
    }

    /**
     * Creates a new Table and start a cleaner.
     */
    public SelfCleaningTable(String name, ICleanerListener<V> cleanerListener) {
        this(name, cleanerListener, new ConcurrentHashMap<KeyHolder<K>, V>());
    }

    /**
     * Creates a new Table and start a cleaner.
     */
    public SelfCleaningTable(String name, ICleanerListener<V> cleanerListener, ConcurrentMap underlyingMap) {
        _name = name;
        _table = underlyingMap;
        _freeEntryQueue = new ReferenceQueue<K>();

        _cleanerThread = new Cleaner<K, V>(_name + "-SelfCleaningTable", _freeEntryQueue, _table, cleanerListener);
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
        return _table.put(createKeyHolder(key, _freeEntryQueue), value);
    }

    /*
     * @see java.util.Map#get(java.lang.Object)
     */
    public V get(Object key) {
        TemplateKeyHolder<K> templateKeyHolder = TemplateKeyHolder.getTemplateKeyHolder((K) key);
        try {
            return _table.get(templateKeyHolder);
        } finally {
            templateKeyHolder.set(null);
        }
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
        close();
    }

    public void close() {
        _cleanerThread.interrupt();
        _table.clear();
    }

    /*
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        TemplateKeyHolder<K> templateKeyHolder = TemplateKeyHolder.getTemplateKeyHolder((K) key);
        try {
            return _table.containsKey(templateKeyHolder);
        } finally {
            templateKeyHolder.set(null);
        }
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
        TemplateKeyHolder<K> templateKeyHolder = TemplateKeyHolder.getTemplateKeyHolder((K) key);
        try {
            return _table.remove(templateKeyHolder);
        } finally {
            templateKeyHolder.set(null);
        }
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
        return _table.values();
    }


    /**
     * Creates a new KeyHolder that connected to a ReferenceQueue.
     *
     * @return new KeyHolder
     */
    private KeyHolder<K> createKeyHolder(K key, ReferenceQueue<K> freeEntryQueue) {
        return new DataKeyHolder<K>(key, freeEntryQueue);
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
     */
    public V putIfAbsent(K key, V value) {
        return _table.putIfAbsent(createKeyHolder(key, _freeEntryQueue), value);
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
     */
    public boolean remove(Object key, Object value) {
        TemplateKeyHolder<K> templateKeyHolder = TemplateKeyHolder.getTemplateKeyHolder((K) key);
        try {
            return _table.remove(templateKeyHolder, value);
        } finally {
            templateKeyHolder.set(null);
        }
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
     */
    public V replace(K key, V value) {
        TemplateKeyHolder<K> templateKeyHolder = TemplateKeyHolder.getTemplateKeyHolder(key);
        try {
            return _table.replace(templateKeyHolder, value);
        } finally {
            templateKeyHolder.set(null);
        }
    }

    /*
     * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
     */
    public boolean replace(K key, V oldValue, V newValue) {
        TemplateKeyHolder<K> templateKeyHolder = TemplateKeyHolder.getTemplateKeyHolder(key);
        try {
            return _table.replace(templateKeyHolder, oldValue, newValue);
        } finally {
            templateKeyHolder.set(null);
        }

    }

    @Override
    public String toString() {
        return _table.toString();
    }

    /**
     * Cleaning Thread, clean all the EntryInfos of the collected Objects.
     */
    final static private class Cleaner<K, V> extends GSThread {
        final private ReferenceQueue<K> _queue;
        final private Map<KeyHolder<K>, V> _table;
        final private ICleanerListener<V> _listener;

        /**
         * Creates a new cleaner.
         *
         * @param queue      the queue that holds the entries to clean.
         * @param entryInfos the Map to clean.
         */
        public Cleaner(String parentName, ReferenceQueue<K> queue, Map<KeyHolder<K>, V> entryInfos, ICleanerListener<V> listener) {
            super(parentName + "$Cleaner");
            _queue = queue;
            _table = entryInfos;
            _listener = listener;
            this.setDaemon(true);
        }

        /**
         * Block on the queue until an entry is been cleaned by the GC
         */
        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    KeyHolder<K> ref = (KeyHolder<K>) _queue.remove(); // block op'
                    V removed = _table.remove(ref); // clean the _table table
                    if (_listener != null && removed != null)
                        _listener.weakEntryRemoved(removed);
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
     * Key in the SelfCleaningTable.
     *
     * @param <T> the type of the Object that need be saved as key.
     * @author Guy Korland
     * @since 6.5
     */
    private static class DataKeyHolder<T> extends WeakReference<T> implements KeyHolder<T> {
        // holds the referent hashCode so we can find this key in the table when the cleaner need to clean it
        final int _hashCode;

        /**
         * Creates a new key for the {@link SelfCleaningTable}.
         *
         * @param referent reference for the Object that should be mapped
         * @param queue    Queue for the cleaner
         */
        public DataKeyHolder(T referent, ReferenceQueue<T> queue) {
            super(referent, queue);
            _hashCode = referent == null ? 13 : referent.hashCode();
        }

        /**
         * Creates a new key for the {@link SelfCleaningTable}, used by the find
         */
        public DataKeyHolder(T referent) {
            /**
             * Fix BugID CORE-550: super(T referent) is called and not super(T referent, null)
             * To avoid NPE in 1.4 on IBM JVMs
             */
            super(referent);
            _hashCode = referent == null ? 13 : referent.hashCode();
        }

        @Override
        public String toString() {
            return "" + get();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object keyHolder) {
            // same Object or the same key value
            // might happen that the local referent is null, but then the two are not equals
            if (keyHolder == this)
                return true;
            T value = get();
            T otherValue = ((KeyHolder<T>) keyHolder).get();
            return value == otherValue || value != null && value.equals(otherValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return _hashCode;
        }
    }

    /**
     * Used in get() operation as a "template" wrapper
     *
     * @param <T> Key type
     * @author guy
     * @since 8.0
     */
    private static class TemplateKeyHolder<T> implements KeyHolder<T> {
        final static private ThreadLocal<TemplateKeyHolder> _holderCache = new ThreadLocal<TemplateKeyHolder>() {
            @Override
            protected TemplateKeyHolder initialValue() {
                return new TemplateKeyHolder();
            }
        };

        public static <T> TemplateKeyHolder<T> getTemplateKeyHolder(T referent) {
            TemplateKeyHolder<T> holder = _holderCache.get();
            holder.set(referent);
            return holder;
        }

        // holds the referent hashCode for performance
        private T _referent;

        /**
         * Creates a new key for the {@link SelfCleaningTable}.
         *
         * @param referent reference for the Object that should be mapped
         */
        private TemplateKeyHolder() {
        }

        public T get() {
            return _referent;
        }

        public void set(T referent) {
            _referent = referent;
        }

        @Override
        public String toString() {
            return "" + _referent;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object keyHolder) {
            // same Object or the same key value
            // might happen that the local referent is null, but then the two are not equals
            if (keyHolder == this)
                return true;
            T value = get();
            T otherValue = ((KeyHolder<T>) keyHolder).get();
            return value == otherValue || value != null && value.equals(otherValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return _referent == null ? 13 : _referent.hashCode();
        }
    }
}
