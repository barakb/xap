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
import com.j_spaces.core.client.version.VersionTable;

import java.lang.ref.WeakReference;

/**
 * Holds the version for each Object for optimistic locking.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0EAG Build#1400-010
 */
@com.gigaspaces.api.InternalApi
public class MapVersionTable extends VersionTable<Integer> {
    /**
     * lazy instantiation to create the MapVersionTable singleton; as a result, the singleton
     * instance is not created until the getInstance() method is called for the first time. This
     * technique ensures that singleton instances are created only when needed. The implementation
     * relies on the well-specified initialization phase of execution within the Java Virtual
     * Machine (JVM). This weak singleton patterns ensures that it will be gc'd if its the last
     * instance which can be finalized.
     *
     * Note that getInstance() is synchronized, and it would be wiser to save a local non-static
     * reference to it. Also, ensure that this object can be gc'd. <code> private MapVersionTable
     * _versionTable = MapVersionTableHolder.getInstance(); </code>
     */
    private static WeakReference<MapVersionTable> weakVersionTable;

    /**
     * @return a weak singleton MapVersionTable instance.
     */
    public static synchronized MapVersionTable getInstance() {
        MapVersionTable versionTable = weakVersionTable == null ? null : weakVersionTable.get();
        if (versionTable == null) // first/last reference
        {
            versionTable = new MapVersionTable();
            weakVersionTable = new WeakReference<MapVersionTable>(versionTable);
        }

        return versionTable;
    }

    /**
     * Add a new value+key to version table
     */
    public void setEntryVersion(Object value, Object key, int version) {
        EntryInfoKey infoKey = new MapEntryInfoKey(value, key, _freeEntryQueue);
        setEntryVersion(infoKey, version);
    }

    /**
     * get value+key version from table according to the value.
     *
     * @return the value+key version
     */
    public int getEntryVersion(Object value, Object key) {
        Integer version = getEntryVersion(new MapEntryInfoKey(value, key));
        return version == null ? 0 : version;
    }
}
