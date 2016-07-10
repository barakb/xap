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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.AnswerPacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author anna
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntrySpaceOperationResult extends SpaceOperationResult {
    private static final long serialVersionUID = 1L;

    private IEntryPacket _entryPacket;
    private int _numOfEntriesMatched;
    private int syncReplicationLevel;

    /**
     * Required for Externalizable
     */
    public ReadTakeEntrySpaceOperationResult() {
    }

    public ReadTakeEntrySpaceOperationResult(AnswerPacket answerPacket, Exception ex) {
        _entryPacket = answerPacket != null ? answerPacket.m_EntryPacket : null;
        setExecutionException(ex);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("entryPacket", _entryPacket);
    }

    public int getSyncReplicationLevel() {
        return syncReplicationLevel;
    }

    public void setSyncReplicationLevel(int syncReplicationLevel) {
        this.syncReplicationLevel = syncReplicationLevel;
    }

    public IEntryPacket getEntryPacket() {
        return _entryPacket;
    }

    public void setEntryPacket(IEntryPacket entry) {
        _entryPacket = entry;
    }

    public void setNumOfEntriesMatched(int numOfEntriesMatched) {
        _numOfEntriesMatched = numOfEntriesMatched;
    }

    public int getNumOfEntriesMatched() {
        return _numOfEntriesMatched;
    }

    private static final short FLAG_ENTRY_PACKET = 1 << 0;
    private static final short FLAG_NUM_OF_ENTRIES_MATCHED = 1 << 1;
    private static final short FLAG_SYNC_REPLICATION_LEVEL = 1 << 2;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if (_entryPacket != null) {
                IOUtils.writeObject(out, _entryPacket);
            }
            if (_numOfEntriesMatched > 0) {
                out.writeInt(_numOfEntriesMatched);
            }
            if (0 < syncReplicationLevel) {
                out.writeInt(syncReplicationLevel);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        final short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_ENTRY_PACKET) != 0) {
                this._entryPacket = IOUtils.readObject(in);
            }
            if ((flags & FLAG_NUM_OF_ENTRIES_MATCHED) != 0) {
                this._numOfEntriesMatched = in.readInt();
            }
            this.syncReplicationLevel = ((flags & FLAG_SYNC_REPLICATION_LEVEL) != 0) ? in.readInt() : 0;

        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_entryPacket != null)
            flags |= FLAG_ENTRY_PACKET;
        if (_numOfEntriesMatched > 0)
            flags |= FLAG_NUM_OF_ENTRIES_MATCHED;
        if (0 < syncReplicationLevel) {
            flags |= FLAG_SYNC_REPLICATION_LEVEL;
        }
        return flags;
    }

}
