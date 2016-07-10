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
 * The listener interface for receiving cache events.
 *
 * @version 1.0
 * @since 5.0
 * @deprecated
 */
@Deprecated
public interface CacheListener {
    /**
     * Triggered when the cache is cleared.
     */
    void onClear();

    /**
     * Triggered when a cache mapping is removed due to eviction.
     *
     * @param key the key that was evicted
     */
    void onEvict(Object key);

    /**
     * Triggered when a cache mapping is created due to the cache loader being consulted.
     *
     * @param key the key that was loaded
     */
    void onLoad(Object key);

    /**
     * Triggered when a cache mapping is created due to calling Cache.put().
     *
     * @param key the key that was put
     */
    void onPut(Object key);

    /**
     * Triggered when a cache mapping is removed due to calling Cache.remove().
     *
     * @param key the key that was removed
     */
    void onRemove(Object key);
}
