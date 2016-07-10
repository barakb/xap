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


package com.gigaspaces.server.eviction;

import com.gigaspaces.internal.server.space.eviction.AllInCacheSpaceEvictionStrategy;
import com.gigaspaces.internal.server.space.eviction.ConcurrentLruSpaceEvictionStrategy;

/**
 * Base class for space eviction strategy implementations. When the space memory manager detects
 * that memory is becoming scarce, it will try to evict entries from the space. The eviction
 * strategy can be altered by using different SpaceEvictionStrategy implementations.
 *
 * @author Yechiel Feffer
 * @see AllInCacheSpaceEvictionStrategy
 * @see ConcurrentLruSpaceEvictionStrategy
 * @since 9.1
 */

public abstract class SpaceEvictionStrategy {
    private SpaceEvictionManager _evictionManager;
    private SpaceEvictionStrategyConfig _evictionConfig;
    private volatile boolean _closed;

    /**
     * Called during the space initialization.
     */
    public void initialize(SpaceEvictionManager evictionManager, SpaceEvictionStrategyConfig config) {
        this._evictionManager = evictionManager;
        this._evictionConfig = config;
    }

    /**
     * Called when the space is closed.
     */
    public void close() {
        _closed = true;
    }

    /**
     * Due to the nature of the system, calls to {@link #onRemove(EvictableServerEntry)} can occur
     * in concurrent with either {@link #onRead(EvictableServerEntry)} or {@link
     * #onUpdate(EvictableServerEntry)} or even before them. In order to prevent this behavior this
     * property should return true (This is the default) and the system will guard this with locks.
     * This should only be set to false by advanced usages which needs to be able to work under this
     * behavior.
     */
    public boolean requiresConcurrencyProtection() {
        return true;
    }

    /**
     * Called when a new entry is written to the space.
     *
     * @param entry The new entry
     */
    public void onInsert(EvictableServerEntry entry) {
    }

    /**
     * Called when an entry is loaded into the space from an external data source.
     *
     * @param entry The loaded entry
     */
    public void onLoad(EvictableServerEntry entry) {
    }

    /**
     * Called when an entry is read from the space.
     *
     * @param entry The read entry
     */
    public void onRead(EvictableServerEntry entry) {
    }

    /**
     * Called when an entry is updated in the space.
     *
     * @param entry The updated entry
     */
    public void onUpdate(EvictableServerEntry entry) {
    }

    /**
     * Called when an entry is removed from the space.
     *
     * @param entry The removed entry
     */
    public void onRemove(EvictableServerEntry entry) {
    }

    /**
     * Called when the space requires entries to be evicted.
     *
     * @param numOfEntries Number of entries to be evicted
     * @return The number of entries actually evicted
     */
    public abstract int evict(int numOfEntries);

    /**
     * Returns the eviction manager provided during initialization.
     */
    protected SpaceEvictionManager getEvictionManager() {
        return _evictionManager;
    }

    /**
     * Returns the eviction configuration provided during initialization.
     */
    protected SpaceEvictionStrategyConfig getEvictionConfig() {
        return _evictionConfig;
    }

    /**
     * Returns true if the space is closed, false otherwise.
     */
    protected boolean isClosed() {
        return _closed;
    }
}
