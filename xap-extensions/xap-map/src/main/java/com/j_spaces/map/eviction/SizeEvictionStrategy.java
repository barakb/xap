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

package com.j_spaces.map.eviction;

import com.j_spaces.core.client.cache.map.BaseCacheEntry;
import com.j_spaces.javax.cache.Cache;
import com.j_spaces.javax.cache.CacheEntry;
import com.j_spaces.kernel.Sizeof;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * First try to evict the biggest object.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
public class SizeEvictionStrategy extends AbstractEvictionStrategy {
    SortedSet<SizeBaseCacheEntry> _entries =
            Collections.synchronizedSortedSet(new TreeSet<SizeBaseCacheEntry>());

    /**
     * {@inheritDoc}
     */
    public void clear() {
        _entries.clear();
    }

    /**
     * {@inheritDoc}
     */
    public void discardEntry(CacheEntry entry) {
        _entries.remove(entry);
    }

    /**
     * {@inheritDoc}
     */
    public int evict(Cache cache) {
        int evicted = 0;
        for (int i = 0; i < _batchSize; ++i) {
            SizeBaseCacheEntry entry = null;
            try {
                entry = _entries.last();
            } catch (NoSuchElementException e) {
                return evicted;
            }

            _entries.remove(entry);
            if (cache.evict(entry.getKey()))
                ++evicted;
        }
        return evicted;
    }

    /**
     * {@inheritDoc}
     */
    public void touchEntry(CacheEntry entry) {
    }

    /**
     * {@inheritDoc}
     */
    public CacheEntry createEntry(Object key, Object value, long ttl, int version) {
        SizeBaseCacheEntry entry = new SizeBaseCacheEntry(key, value, ttl, version);
        _entries.add(entry);

        return entry;
    }

    final static private class SizeBaseCacheEntry extends BaseCacheEntry implements Comparable<SizeBaseCacheEntry> {
        final private int _size;

        public SizeBaseCacheEntry(Object key, Object value, long timeToLive, int version) {
            super(key, value, timeToLive, version);

            _size = Sizeof.sizeof(value);
        }

        public int compareTo(SizeBaseCacheEntry o) {
            int diff = _size - o._size;
            if (diff == 0 && this != o) // in case two entries have the same size
                diff = hashCode() - o.hashCode();
            return diff;
        }
    }

}
