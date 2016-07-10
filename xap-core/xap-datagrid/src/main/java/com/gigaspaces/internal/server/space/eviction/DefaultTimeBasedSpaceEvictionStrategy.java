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

package com.gigaspaces.internal.server.space.eviction;

import com.gigaspaces.internal.server.space.SpaceConfigReader;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_TIMEBASED_EVICTION_MEMORY_TIME_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_TIMEBASED_EVICTION_MEMORY_TIME_PROP;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_TIMEBASED_EVICTION_REINSERTED_MEMORY_TIME_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_TIMEBASED_EVICTION_REINSERTED_MEMORY_TIME_PROP;

/**
 * @author Yechiel Feffer
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class DefaultTimeBasedSpaceEvictionStrategy extends TimeBasedSpaceEvictionStrategy {
    private static final int HarvestMainInterval = 2 * 1000;
    private static final int HarvestShortLivedInterval = 2 * 1000;

    public DefaultTimeBasedSpaceEvictionStrategy(SpaceConfigReader configReader) {
        super(configReader.getIntSpaceProperty(CACHE_MANAGER_TIMEBASED_EVICTION_MEMORY_TIME_PROP, Integer.toString(CACHE_MANAGER_TIMEBASED_EVICTION_MEMORY_TIME_DEFAULT)),
                configReader.getIntSpaceProperty(CACHE_MANAGER_TIMEBASED_EVICTION_REINSERTED_MEMORY_TIME_PROP, Integer.toString(CACHE_MANAGER_TIMEBASED_EVICTION_REINSERTED_MEMORY_TIME_DEFAULT)),
                HarvestMainInterval,
                HarvestShortLivedInterval);
    }
}
