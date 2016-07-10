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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.admin.DirectPersistencySyncListAdmin;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.IEmbeddedSyncOpInfo;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * The element of synchronizing a direct-persistency uid
 *
 * @author yechielf
 * @since 10.2
 */
public abstract class AbstractDirectPersistencyOpInfo implements IDirectPersistencyOpInfo {

    private static final long serialVersionUID = 1L;

    //_generationId + seq is the unique key of this entry in ssd/disk
    private long _generationId;
    private long _seq;

    private long _redoKey;   //usually set only in memory representation
    private boolean _inMainList;
    private boolean _persisted;  //the space entry was persisted (can be delayed in bulks)
    //embedded list related
    private transient volatile IEmbeddedSyncOpInfo _embeddedSyncOpInfo;


    private static final byte FLAG_BACKUP = 1 << 0;


    public AbstractDirectPersistencyOpInfo(long generationId, int seq1, int seq2) {
        _generationId = generationId;
        _seq = seq1;
        _seq = (_seq << 32) + seq2;
        _redoKey = -1;
    }

    public AbstractDirectPersistencyOpInfo(long generationId, long seq) {
        _generationId = generationId;
        _seq = seq;
        _redoKey = -1;
    }

    public AbstractDirectPersistencyOpInfo() {
    }


    @Override
    public long getGenerationId() {
        return _generationId;
    }

    @Override
    public abstract boolean isMultiUids();

    @Override
    public abstract String getUid();

    @Override
    public abstract List<String> getUids();


    /* -1 if not set */
    @Override
    public Long getRedoKey() {
        return _redoKey;
    }

    @Override
    public boolean hasRedoKey() {
        return _redoKey != -1;
    }

    @Override
    public void setRedoKey(long redokey) {
        _redoKey = redokey;
    }


    @Override
    public boolean isInMainList() {
        return _inMainList;
    }

    @Override
    public void setInMainList() {
        _inMainList = true;
    }

    @Override
    public boolean isPersisted() {
        return _persisted;
    }

    @Override
    public void setPersisted() {
        _persisted = true;
    }

    @Override
    public long getSequenceNumber() {
        return _seq;
    }

    @Override
    public int getSegmentNumber() {
        return getSegmentNumber(_seq);
    }

    public static int getSegmentNumber(long seq) {
        return (int) (seq >>> 32);
    }

    @Override
    public int getOrderWithinSegment() {
        return getOrderWithinSegment(_seq);
    }

    public static int getOrderWithinSegment(long seq) {
        return (int) ((seq << 32) >>> 32);
    }


    @Override
    public boolean isConfirmedByRemote(IDirectPersistencySyncHandler handler) {
        return (hasRedoKey() && getRedoKey() <= handler.getLastConfirmed());
    }


    @Override
    public void setEmbeddedSyncOpInfo(IEmbeddedSyncOpInfo embeddedSyncOpInfo) {
        _embeddedSyncOpInfo = embeddedSyncOpInfo;
    }

    @Override
    public IEmbeddedSyncOpInfo getEmbeddedSyncOpInfo() {
        return _embeddedSyncOpInfo;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_generationId);
        out.writeLong(_seq);
        if (_redoKey > -1) {
            out.writeBoolean(true);
            out.writeLong(_redoKey);
        } else
            out.writeBoolean(false);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _generationId = in.readLong();
        _seq = in.readLong();

        //currently unused- get current platform version and original platform version
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        PlatformLogicalVersion myversion = DirectPersistencySyncListAdmin.getVersions().get(_generationId);
        if (myversion == null)
            throw new RuntimeException("platform version not found for generationid=" + _generationId);

        if (in.readBoolean())
            _redoKey = in.readLong();
        else
            _redoKey = -1;
        _persisted = true;
    }

    @Override
    public String toString() {
        return "AbstractDirectPersistencyOpInfo{" +
                "_generationId=" + _generationId +
                ", _seq=" + _seq +
                ", _redoKey=" + _redoKey +
                ", _inMainList=" + _inMainList +
                ", _persisted=" + _persisted +
                ", _embeddedSyncOpInfo=" + _embeddedSyncOpInfo +
                '}';
    }
}
