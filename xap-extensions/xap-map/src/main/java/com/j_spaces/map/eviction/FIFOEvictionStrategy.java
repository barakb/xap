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

import com.gigaspaces.internal.server.space.eviction.ChainsSegments;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.eviction.EvictableServerEntry;
import com.j_spaces.core.client.cache.map.BaseCacheEntry;
import com.j_spaces.javax.cache.Cache;
import com.j_spaces.javax.cache.CacheEntry;

import java.util.Iterator;

/**
 * first-in first-out (as opposed to LIFO, last-in first-out) Items come out in the same order they
 * came in.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
public class FIFOEvictionStrategy extends AbstractEvictionStrategy {
    protected ChainsSegments _chain = new ChainsSegments();

    public void clear() {
        _chain = new ChainsSegments();
    }

    /**
     * {@inheritDoc}
     */
    public void discardEntry(CacheEntry entry) {
        _chain.remove((CacheEntryCell) entry);
    }

    public int evict(Cache cache) {
        int evicted = 0;
        Iterator<EvictableServerEntry> evictionCandidates = _chain.evictionCandidates();
        while (evicted < _batchSize && evictionCandidates.hasNext()) {
            CacheEntryCell entry = (CacheEntryCell) evictionCandidates.next();
            if (entry == null || !_chain.remove(entry)) {
                continue;
            }

            if (cache.evict(entry.getKey())) {
                ++evicted;
            }
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
        CacheEntryCell newCell = new CacheEntryCell(key, value, ttl, version);
        _chain.insert(newCell);
        return newCell;
    }


    static private class CacheEntryCell extends BaseCacheEntry implements EvictableServerEntry {
        private Object _evictionPayLoad;

        public CacheEntryCell(Object key, Object value, long timeToLive, int version) {
            super(key, value, timeToLive, version);
        }

        public Object getEvictionPayLoad() {
            return _evictionPayLoad;
        }

        public void setEvictionPayLoad(Object evictionPayLoad) {
            _evictionPayLoad = evictionPayLoad;
        }


        public boolean isTransient() {
            return false;
        }

        public SpaceTypeDescriptor getSpaceTypeDescriptor() {
            throw new UnsupportedOperationException();
        }

        public Object getFixedPropertyValue(int position) {
            throw new UnsupportedOperationException();
        }

        public Object getPropertyValue(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getPathValue(String path) {
            throw new UnsupportedOperationException();
        }

        public long getExpirationTime() {
            throw new UnsupportedOperationException();

        }

        public int getVersion() {
            throw new UnsupportedOperationException();

        }

        @Override
        public String getUID() {
            return null;
        }
    }

}
