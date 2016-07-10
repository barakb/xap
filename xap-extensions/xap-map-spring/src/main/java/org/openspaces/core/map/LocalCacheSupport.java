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


package org.openspaces.core.map;

import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.javax.cache.EvictionStrategy;
import com.j_spaces.map.eviction.NoneEvictionStrategy;

/**
 * A bean holding local cache support (when using Map API) configuration.
 *
 * @author kimchy
 */
public class LocalCacheSupport {

    public static final String LOCAL_CACHE_UPDATE_MODE_PUSH = "push";

    public static final String LOCAL_CACHE_UPDATE_MODE_PULL = "pull";

    private int localCacheUpdateMode = SpaceURL.UPDATE_MODE_PULL;

    private boolean versioned = false;

    private EvictionStrategy evictionStrategy;

    private boolean putFirst = true;

    private int sizeLimit = 100000;

    /**
     * Controls if this local cache will be versioned or not. Note, when settings this to
     * <code>true</code>, make sure that the actual space is versioned as well.
     */
    public void setVersioned(boolean versioned) {
        this.versioned = versioned;
    }

    /**
     * Controls if this local cache will be versioned or not. Note, when settings this to
     * <code>true</code>, make sure that the actual space is versioned as well.
     */
    public boolean isVersioned() {
        return versioned;
    }

    /**
     * Sets the eviction strategy for the local cache.
     */
    public void setEvictionStrategy(EvictionStrategy evictionStrategy) {
        this.evictionStrategy = evictionStrategy;
    }

    /**
     * Sets the eviction strategy for the local cache.
     */
    public EvictionStrategy getEvictionStrategy() {
        if (evictionStrategy == null) {
            return new NoneEvictionStrategy();
        }
        return evictionStrategy;
    }

    /**
     * If set to {@link SpaceURL#UPDATE_MODE_PULL} (<code>1</code>) each update triggers an
     * invalidation event at every cache instance. The invalidate event marks the object in the
     * local cache instances as invalid. Therefore, an attempt to read this object triggers a reload
     * process in the master space. This configuration is useful in cases where objects are updated
     * frequently, but the updated value is required by the application less frequently.
     *
     * <p>If set to {@link SpaceURL#UPDATE_MODE_PUSH} (<code>2</code>) the master pushes the updates
     * to the local cache, which holds a reference to the same updated object.
     *
     * @see #setUpdateModeName(String)
     */
    public void setUpdateMode(int localCacheUpdateMode) {
        this.localCacheUpdateMode = localCacheUpdateMode;
    }

    /**
     * If set to {@link SpaceURL#UPDATE_MODE_PULL} (<code>1</code>) each update triggers an
     * invalidation event at every cache instance. The invalidate event marks the object in the
     * local cache instances as invalid. Therefore, an attempt to read this object triggers a reload
     * process in the master space. This configuration is useful in cases where objects are updated
     * frequently, but the updated value is required by the application less frequently.
     *
     * <p>If set to {@link SpaceURL#UPDATE_MODE_PUSH} (<code>2</code>) the master pushes the updates
     * to the local cache, which holds a reference to the same updated object.
     *
     * @see #setUpdateMode(int)
     */
    public int getLocalCacheUpdateMode() {
        return localCacheUpdateMode;
    }

    /**
     * Allows to set the local cache update mode using a descriptive name instead of integer
     * constants using {@link #setUpdateMode(int) localCacheUpdateMode}. Accepts either
     * <code>push</code> or <code>pull</code>.
     *
     * @see #setUpdateMode (int)
     */
    public void setUpdateModeName(String localCacheUpdateModeName) {
        if (LOCAL_CACHE_UPDATE_MODE_PULL.equalsIgnoreCase(localCacheUpdateModeName)) {
            setUpdateMode(SpaceURL.UPDATE_MODE_PULL);
        } else if (LOCAL_CACHE_UPDATE_MODE_PUSH.equalsIgnoreCase(localCacheUpdateModeName)) {
            setUpdateMode(SpaceURL.UPDATE_MODE_PUSH);
        } else {
            throw new IllegalArgumentException("Wrong localCacheUpdateModeName [" + localCacheUpdateModeName + "], "
                    + "should be either '" + LOCAL_CACHE_UPDATE_MODE_PULL + "' or '" + LOCAL_CACHE_UPDATE_MODE_PUSH
                    + "'");
        }
    }

    /**
     * When performing a put operation, you may perform the put operation both into the local cache
     * and the master space. This will speed up subsequent get operations. Default to
     * <code>true</code>.
     */
    public void setPutFirst(boolean putFirst) {
        this.putFirst = putFirst;
    }

    /**
     * When performing a put operation, you may perform the put operation both into the local cache
     * and the master space. This will speed up subsequent get operations. Default to
     * <code>true</code>.
     */
    public boolean isPutFirst() {
        return putFirst;
    }

    /**
     * Sets the size limit of the local cache. Default to <code>100000</code>.
     */
    public void setSizeLimit(int sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    /**
     * Sets the size limit of the local cache. Default to <code>100000</code>.
     */
    public int getSizeLimit() {
        return sizeLimit;
    }
}
