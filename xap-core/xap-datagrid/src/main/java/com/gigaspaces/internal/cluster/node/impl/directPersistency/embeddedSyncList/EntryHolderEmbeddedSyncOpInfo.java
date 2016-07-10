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

/**
 * @author Boris
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class EntryHolderEmbeddedSyncOpInfo {

    private final long _generationId;
    private final long _sequenceId;
    private final boolean _phantom; // take op
    private final boolean _partOfMultipleUidsInfo;

    public EntryHolderEmbeddedSyncOpInfo(long _generationId, long _sequenceId, boolean _phantom, boolean partOfMultipleUidsInfo) {
        this._generationId = _generationId;
        this._sequenceId = _sequenceId;
        this._phantom = _phantom;
        this._partOfMultipleUidsInfo = partOfMultipleUidsInfo;
    }

    public long getGenerationId() {
        return _generationId;
    }

    public long getSequenceId() {
        return _sequenceId;
    }

    public boolean isPhantom() {
        return _phantom;
    }

    public boolean isPartOfMultipleUidsInfo() {
        return _partOfMultipleUidsInfo;
    }

    @Override
    public String toString() {
        return "EmbeddedSyncOpInfo{" +
                "_generationId=" + _generationId +
                ", _sequenceId=" + _sequenceId +
                ", _phantom=" + _phantom +
                ", _partOfMutipleUidsInfo=" + _partOfMultipleUidsInfo +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntryHolderEmbeddedSyncOpInfo that = (EntryHolderEmbeddedSyncOpInfo) o;

        if (_generationId != that._generationId) return false;
        if (_sequenceId != that._sequenceId) return false;
        if (_phantom != that._phantom) return false;
        return _partOfMultipleUidsInfo == that._partOfMultipleUidsInfo;

    }

    @Override
    public int hashCode() {
        int result = (int) (_generationId ^ (_generationId >>> 32));
        result = 31 * result + (int) (_sequenceId ^ (_sequenceId >>> 32));
        result = 31 * result + (_phantom ? 1 : 0);
        result = 31 * result + (_partOfMultipleUidsInfo ? 1 : 0);
        return result;
    }
}
