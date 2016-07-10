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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyMultipleUidsOpInfo;

import java.util.Set;

/**
 * The synchronizing direct-persistency  embedded list op info
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedMultiUidsSyncOpInfo extends AbstratEmbeddedSyncOpInfo {
    private final Set<String> _phantoms;

    public EmbeddedMultiUidsSyncOpInfo(DirectPersistencyMultipleUidsOpInfo originalOpInfo, Set<String> phantoms) {
        super(originalOpInfo);
        _phantoms = phantoms;
    }

    @Override
    public boolean containsAnyPhantom() {
        return _phantoms != null && !_phantoms.isEmpty();
    }

    @Override
    public boolean containsPhantom(String uid) {
        return _phantoms != null && _phantoms.contains(uid);
    }


    @Override
    public void resetPhantom(String uid) {
        _phantoms.remove(uid);
    }

    public Set<String> getPhantoms() {
        return _phantoms;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(super.toString()).append("multi uids=");
        for (String uid : ((DirectPersistencyMultipleUidsOpInfo) getOriginalOpInfo()).getUids()) {
            sb.append(uid).append(" ");
        }
        return sb.toString();
    }

    @Override
    public boolean isMultiUids() {
        return true;
    }

}
