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

/**
 * No eviction  strategy
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
public class NoneEvictionStrategy extends AbstractEvictionStrategy {
    /**
     * {@inheritDoc}
     */
    public void clear() {
    }

    /**
     * {@inheritDoc}
     */
    public void discardEntry(CacheEntry entry) {
    }

    /**
     * {@inheritDoc}
     */
    public int evict(Cache cache) {
        return 0;
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
        return new BaseCacheEntry(key, value, ttl, version);
    }
}
