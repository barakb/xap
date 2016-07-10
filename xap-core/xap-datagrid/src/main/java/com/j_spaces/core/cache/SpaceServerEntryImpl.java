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

package com.j_spaces.core.cache;

import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.SpaceServerEntry;

/**
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceServerEntryImpl implements SpaceServerEntry {
//TBD TBD TBD BLOB-STORE need special handling _ted is not relevant


    public static long MAX_TIME_TO_WAIT_FOR_INDEX_REMOVAL = 3 * 1000 * 60;

    private final ITransactionalEntryData _ted;
    private final IEntryCacheInfo _eci;
    private volatile boolean _removedFromForeignIndex;
    private volatile boolean _waitingForRemove;

    public SpaceServerEntryImpl(IEntryCacheInfo eci, CacheManager cm) {
        _ted = eci.getEntryHolder(cm).getTxnEntryData();
        _eci = eci;
    }

    @Override
    public String getUid() {
        return _eci.getUID();
    }

    public IEntryCacheInfo getEntryCacheInfo() {
        return _eci;
    }


    @Override
    public SpaceTypeDescriptor getSpaceTypeDescriptor() {
        return _ted.getSpaceTypeDescriptor();
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return _ted.getFixedPropertyValue(position);
    }

    @Override
    public Object getPropertyValue(String name) {
        return _ted.getPropertyValue(name);
    }

    @Override
    public Object getPathValue(String path) {
        return _ted.getPathValue(path);
    }

    @Override
    public int getVersion() {
        return _ted.getVersion();
    }

    @Override
    public long getExpirationTime() {
        return _ted.getExpirationTime();
    }

    public boolean isRemovedFromForeignIndex() {
        return _removedFromForeignIndex;
    }

    public void setRemovedFromForeignIndex() {
        _removedFromForeignIndex = true;
        if (_waitingForRemove) {
            synchronized (this) {
                notify();
            }
        }
    }

    public void waitForRemovalFromForeignIndex() {
        if (isRemovedFromForeignIndex())
            return;
        synchronized (this) {
            _waitingForRemove = true;
            if (isRemovedFromForeignIndex())
                return;
            long upto = System.currentTimeMillis() + MAX_TIME_TO_WAIT_FOR_INDEX_REMOVAL;
            long left = MAX_TIME_TO_WAIT_FOR_INDEX_REMOVAL;
            while (true) {
                try {
                    wait(left);
                } catch (Exception ex) {
                    throw new RuntimeException("entry encountered exception while waiting for remove uid=" + _eci.getUID() + " ex=" + ex);
                }
                if (isRemovedFromForeignIndex())
                    return;
                left = upto - System.currentTimeMillis();
                if (left <= 0)
                    break;
            }
            if (!isRemovedFromForeignIndex())
                throw new RuntimeException("entry is not removed after timed waiting uid=" + _eci.getUID());

        }
    }


    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        SpaceServerEntryImpl other = (SpaceServerEntryImpl) o;
        return _eci == other._eci;
    }

}
