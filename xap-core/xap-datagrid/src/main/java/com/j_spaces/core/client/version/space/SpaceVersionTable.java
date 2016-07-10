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

package com.j_spaces.core.client.version.space;

import com.j_spaces.core.client.EntryInfo;
import com.j_spaces.core.client.version.EntryInfoKey;
import com.j_spaces.core.client.version.VersionTable;

import java.lang.ref.WeakReference;

/**
 * Holds the version (EntryInfo) for each Entry for optimistic locking.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0EAG Build#1400-010
 */
@com.gigaspaces.api.InternalApi
public class SpaceVersionTable extends VersionTable<EntryInfo> {
    /**
     * lazy instantiation to create the SpaceVersionTable singleton; as a result, the singleton
     * instance is not created until the getInstance() method is called for the first time. This
     * technique ensures that singleton instances are created only when needed. The implementation
     * relies on the well-specified initialization phase of execution within the Java Virtual
     * Machine (JVM). This weak singleton patterns ensures that it will be gc'd if its the last
     * instance which can be finalized. <p/> Note that getInstance() is synchronized, and it would
     * be wiser to save a local non-static reference to it. Also, ensure that this object can be
     * gc'd. <code> private SpaceVersionTable _versionTable = SpaceVersionTableHolder.getInstance();
     * </code>
     */
    private static WeakReference<SpaceVersionTable> weakVersionTable;

    /**
     * @return a weak singleton MapVersionTable instance.
     */
    public static synchronized SpaceVersionTable getInstance() {
        SpaceVersionTable versionTable = weakVersionTable == null ? null : weakVersionTable.get();
        if (versionTable == null) // first/last reference
        {
            versionTable = new SpaceVersionTable();
            weakVersionTable = new WeakReference<SpaceVersionTable>(versionTable);
        }

        return versionTable;
    }

    /**
     * Map Entry to EntryInfo.
     */
    public void setEntryVersion(Object entry, EntryInfo entryInfo) {
        EntryInfoKey<Object> infoKey = new EntryInfoKey<Object>(entry, _freeEntryQueue);
        setEntryVersion(infoKey, entryInfo);
    }

    /**
     * get entry version from table.
     *
     * @return the Entry EntryInfo
     */
    public EntryInfo getEntryVersion(Object entry) {
        return getEntryVersion(new EntryInfoKey<Object>(entry));
    }


}
