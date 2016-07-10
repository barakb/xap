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

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySingleUidOpInfo;

/**
 * The synchronizing direct-persistency  embedded list op info
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedSingeUidSyncOpInfo extends AbstratEmbeddedSyncOpInfo {
    private boolean _phantom;

    public EmbeddedSingeUidSyncOpInfo(DirectPersistencySingleUidOpInfo originalOpInfo, boolean phantom) {
        super(originalOpInfo);
        _phantom = phantom;
    }

    @Override
    public boolean containsPhantom(String uid) {
        return (_phantom && uid.equals(getOriginalOpInfo().getUid()));
    }

    @Override
    public boolean containsAnyPhantom() {
        return _phantom;
    }

    @Override
    public void resetPhantom(String uid) {
        if (!uid.equals(getOriginalOpInfo().getUid()))
            throw new IllegalArgumentException();
        _phantom = false;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(super.toString()).append("single uid=").append(getOriginalOpInfo().getUid());
        return sb.toString();
    }

    @Override
    public boolean isMultiUids() {
        return false;
    }

}
