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

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The heap LFUDA ( Least Frequently Used) policy keeps popular objects in cache regardless of their
 * size and thus optimizes byte hit rate at the expense of hit rate since one large, popular object
 * will prevent many smaller, slightly less popular objects from being cached.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
public class LFUEvictionStrategy extends AbstractEvictionStrategy {
    final private TreeSet<FUCacheEntry> _sorted = new TreeSet<FUCacheEntry>();

    /**
     * {@inheritDoc}
     */
    synchronized public void clear() {
        _sorted.clear();
    }

    /**
     * {@inheritDoc}
     */
    synchronized public void discardEntry(CacheEntry entry) {
        _sorted.remove(entry);
    }

    /**
     * {@inheritDoc}
     */
    synchronized public int evict(Cache cache) {
        int evicted = 0;
        for (int i = 0; i < _batchSize && _sorted.size() > 0; ++i) {
            FUCacheEntry entry = _sorted.first();
            if (entry == null)
                return evicted;

            _sorted.remove(entry);
            if (cache.evict(entry.getKey()))
                ++evicted;
        }
        return evicted;
    }

    /**
     * {@inheritDoc}
     */
    synchronized public void touchEntry(CacheEntry entry) {
        if (_sorted.remove(entry)) {
            FUCacheEntry e = (FUCacheEntry) entry;
            e.touch();
            _sorted.add(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    synchronized public CacheEntry createEntry(Object key, Object value, long ttl, int version) {
        FUCacheEntry entry = new FUCacheEntry(key, value, ttl, version);
        _sorted.add(entry);
        return entry;
    }


    static private class FUCacheEntry extends BaseCacheEntry implements Comparable<FUCacheEntry> {
        volatile private int _touchCounter = 0; // almost count

        final private static AtomicInteger _nextID = new AtomicInteger();

        // used only by compareTo() to break a tie
        final private int _id = _nextID.getAndIncrement();

        public FUCacheEntry(Object key, Object value, long timeToLive, int version) {
            super(key, value, timeToLive, version);
        }

        public void touch() {
            int count = _touchCounter;
            _touchCounter = count + 1;
        }

        public int compareTo(FUCacheEntry entry) {
            if (entry == this)
                return 0;

            int diff = _touchCounter - entry._touchCounter;

            if (diff == 0)
                return _id - entry._id;

            return diff;
        }
    }


}
