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

package com.j_spaces.core.client.cache.map;

import com.j_spaces.javax.cache.CacheEntry;

/**
 * Dummy entry used by the MapCache as an indicator for outgoing gets
 *
 * @author Guy korland
 * @version 1.0
 * @since 4.5
 */
class DummyCacheEntry implements CacheEntry {
    public int getHits() {
        return 0;
    }

    public long getLastAccessTime() {
        return 0;
    }

    public long getLastUpdateTime() {
        return 0;
    }

    public long getCreationTime() {
        return 0;
    }

    public long getExpirationTime() {
        return 0;
    }

    public long getCacheEntryVersion() {
        return 0;
    }

    public boolean isValid() {
        return false;
    }

    public long getCost() {
        return 0;
    }

    public Object getKey() {
        return null;
    }

    public Object getValue() {
        return null;
    }

    public Object setValue(Object value) {
        return null;
    }
}
