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

import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;

/**
 * The synchronizing direct-persistency  embedded list op info
 *
 * @author yechielf
 * @since 11.0
 */
public abstract class AbstratEmbeddedSyncOpInfo implements IEmbeddedSyncOpInfo {

    private final IDirectPersistencyOpInfo _originalOpInfo;
    private boolean _transferred;
    private volatile boolean _persistedToMainList;

    public AbstratEmbeddedSyncOpInfo(IDirectPersistencyOpInfo originalOpInfo) {
        _originalOpInfo = originalOpInfo;
    }

    @Override
    public IDirectPersistencyOpInfo getOriginalOpInfo() {
        return _originalOpInfo;
    }

    @Override
    public int getEmbeddedSegmentNumber() {
        return _originalOpInfo.getSegmentNumber();
    }

    @Override
    public int getSeqInEmbeddedSegment() {
        return _originalOpInfo.getOrderWithinSegment();
    }

    @Override
    public boolean isPersistedToMainList() {
        return _persistedToMainList;
    }

    @Override
    public void setPersistedToMainList() {
        _persistedToMainList = true;
    }


    @Override
    public boolean canTransferToMainList(long currentGenId) {
        return (getOriginalOpInfo().isPersisted() && (_originalOpInfo.hasRedoKey() || _originalOpInfo.getGenerationId() != currentGenId));
    }

    @Override
    public abstract boolean containsAnyPhantom();

    @Override
    public abstract void resetPhantom(String uid);

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(" generation=").append(_originalOpInfo.getGenerationId()).append(" seq=").append(_originalOpInfo.getSequenceNumber());
        return sb.toString();
    }

    @Override
    public abstract boolean isMultiUids();

    @Override
    public int hashCode() {
        return ((Long) (_originalOpInfo.getSequenceNumber())).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        AbstratEmbeddedSyncOpInfo other = (AbstratEmbeddedSyncOpInfo) o;
        return (other.getOriginalOpInfo().getSequenceNumber() == getOriginalOpInfo().getSequenceNumber() &&
                other.getOriginalOpInfo().getGenerationId() == getOriginalOpInfo().getGenerationId());
    }
}
