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

import com.j_spaces.javax.cache.EvictionStrategy;

/**
 * Implements EvictionStrategy and acts as a base for all the eviction strategies
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
public abstract class AbstractEvictionStrategy implements EvictionStrategy {
    protected int _batchSize;

    /**
     * Set the batch size to evict on each <code>evict()</code> call.
     *
     * @param batchSize number of entries to evict.
     */
    public void setBatchSize(int batchSize) {
        _batchSize = batchSize;
    }
}
