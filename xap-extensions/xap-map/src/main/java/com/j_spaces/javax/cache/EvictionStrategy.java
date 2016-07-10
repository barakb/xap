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



/*
 * Created on 02/06/2005
 *
 */
package com.j_spaces.javax.cache;

/**
 * EvictionStrategy decides which entry to evict when the cache reaches its limits.
 *
 * The eviction strategy acts as a CacheEntry factory, this way if the cache has a new element
 * inserted then the eviction strategy would be informed of such an operation and can keep track of
 * the state of the cache. In order to keep track of the entries use the cache informs the eviction
 * strategy on each use ("touch") of a CacheEntry.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 * @deprecated
 */
@Deprecated
public interface EvictionStrategy {
    /**
     * Clear EvictionStrategy internal structures only and doesn't clear cache.
     */
    void clear();

    /**
     * Creates new CacheEntry for the Cache and probably saves the entry in the internal
     * structures.
     *
     * @param key     the entry key
     * @param value   the entry value
     * @param ttl     the time to keep the entry in cache
     * @param version the entry version
     * @return a CacheEntry that holds all the parameters and probably saved in the eviction
     * strategy internal structures
     */
    CacheEntry createEntry(Object key, Object value, long ttl, int version);

    /**
     * Discard an Entry from the EvictionStrategy internal structures.
     *
     * @param entry the entry to discard from the eviction strategy internal structures
     */
    void discardEntry(CacheEntry entry);

    /**
     * Evicts batch of CacheEntry from the Cache according to the eviction strategy.
     *
     * @param cache the cache to be evicted
     * @return the amount of entries evicted
     */
    int evict(Cache cache);

    /**
     * Informs the eviction strategy on a CacheEntry use.
     *
     * @param entry the touched entry
     */
    void touchEntry(CacheEntry entry);
}
