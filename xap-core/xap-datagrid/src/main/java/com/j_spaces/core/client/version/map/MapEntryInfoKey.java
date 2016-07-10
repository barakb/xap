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

package com.j_spaces.core.client.version.map;

import com.j_spaces.core.client.version.EntryInfoKey;

import java.lang.ref.ReferenceQueue;

/**
 * Key in the MapVersionTable.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0EAG Build#1400-010
 */
@com.gigaspaces.api.InternalApi
public class MapEntryInfoKey extends EntryInfoKey<Object> {

    // the IMap key for this Object
    final private Object _key;

    /**
     * Creates a new MapEntryInfoKey, used to keep it in MapVersionTable.
     *
     * @param value the IMap value
     * @param key   the IMap key
     * @param queue for the cleaner
     */
    public MapEntryInfoKey(Object value, Object key, ReferenceQueue<Object> queue) {
        super(value, queue);

        _hashCode = _hashCode ^ key.hashCode();
        _key = key;
    }

    /**
     * Creates a new MapEntryInfoKey, used to find existing key in MapVersionTable.
     *
     * @param value the IMap value
     * @param key   the IMap key
     */
    public MapEntryInfoKey(Object value, Object key) {
        super(value);

        _hashCode = _hashCode ^ key.hashCode();
        _key = key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        MapEntryInfoKey infoKey = (MapEntryInfoKey) object;

        // same Object and the same key value
        return super.equals(object) && _key.equals(infoKey._key);
    }
}