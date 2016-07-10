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

package com.j_spaces.core.client;

import com.gigaspaces.annotation.pojo.SpaceClass;

import net.jini.core.entry.Entry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Do not use this class! It is deprecated and serves for internal purposed only.
 *
 * @version 3.2
 * @deprecated Use {@link SpaceClass} instead.
 **/
@Deprecated
@com.gigaspaces.api.InternalApi
public class MetaDataEntry implements Entry {
    private static final long serialVersionUID = 1L;

    private EntryInfo entryInfo;
    private boolean isFifo;

    /**
     * if this tag is true - it means that the entry is transient even if the space is persistent
     * space
     **/
    private boolean isTransient;

    /**
     * If true, Lease object would not return from the write/writeMultiple operations.
     */
    private boolean m_NOWriteLeaseMode;


    /**
     * no args public constructor
     */
    public MetaDataEntry() {
    }

    /**
     * Set entry info.
     *
     * @param entryInfo Entry Info.
     **/
    public void __setEntryInfo(EntryInfo entryInfo) {
        this.entryInfo = entryInfo;
    }

    /**
     * Returns entry info.
     *
     * @return Entry info.
     **/
    public EntryInfo __getEntryInfo() {
        return entryInfo;
    }

    /**
     * Enable/Disable FIFO mechanism.
     *
     * @param fifo <code>true</code> enable FIFO, otherwise <code>false</code>.
     **/
    public void setFifo(boolean fifo) {
        isFifo = fifo;
    }

    /**
     * Returns FIFO status.
     *
     * @return <code>true</code> if FIFO enabled, otherwise <code>false</code>.
     **/
    public boolean isFifo() {
        return isFifo;
    }

    /**
     * Makes this entry a persistent entry. Can be called only if the entry has not been written yet
     * to the space.
     **/
    public void makePersistent() {
        this.isTransient = false;
    }

    /**
     * Makes this entry a transient entry.
     **/
    public void makeTransient() {
        this.isTransient = true;
    }

    /**
     * Returns <code>true</code> if entry is transient, otherwise <code>false</code>.
     *
     * @return <code>true</code> if entry is transient, otherwise <code>false</code>.
     **/
    public boolean isTransient() {
        return isTransient;
    }

    /**
     * Sets the entry to be transient (true) or persistent (false).
     */
    public void setTransient(boolean isTransient) {
        this.isTransient = isTransient;
    }

    /**
     * Set <code>true</code> do not return Lease object after write, <code>false</code> return Lease
     * object after write.
     *
     * @param noWriteLeaseMode write mode.
     **/
    public void setNOWriteLeaseMode(boolean noWriteLeaseMode) {
        this.m_NOWriteLeaseMode = noWriteLeaseMode;
    }

    /**
     * Check write mode.
     *
     * @return <code>true</code> if do not return Lease object after write, otherwise
     * <code>false</code>.
     **/
    public boolean isNOWriteLeaseMode() {
        return m_NOWriteLeaseMode;
    }

    /**
     * Control the serialization stream by ourself.
     **/
    private void writeObject(ObjectOutputStream out)
            throws IOException {
        _writeExternal(out);
    }

    /**
     * Control the de-serialization stream by ourself.
     **/
    private void readObject(ObjectInputStream in)
            throws IOException {
        _readExternal(in);
    }


    /**
     * Writes to the ObjectOutput stream by optimized way the meta-info of MetaDataEntry.
     **/
    protected void _writeExternal(ObjectOutput out)
            throws IOException {
        if (entryInfo != null) {
            out.writeBoolean(true);
            if (entryInfo.m_UID != null) {
                out.writeBoolean(true);
                out.writeUTF(entryInfo.m_UID);
            } else {
                out.writeBoolean(false);
            }
            out.writeInt(entryInfo.m_VersionID);
            out.writeLong(entryInfo.m_TimeToLive);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(isFifo);
        out.writeBoolean(isTransient);
        out.writeBoolean(m_NOWriteLeaseMode);
    }

    /**
     * Reads from the ObjectInput stream by optimized way the meta-info of MetaDataEntry.
     **/
    protected void _readExternal(ObjectInput in)
            throws IOException {
        if (in.readBoolean()) {
            String uid = null;
            if (in.readBoolean())
                uid = in.readUTF();
            int version = in.readInt();
            long ttl = in.readLong();

            entryInfo = new EntryInfo(uid, version, ttl);
        }

        isFifo = in.readBoolean();
        isTransient = in.readBoolean();
        m_NOWriteLeaseMode = in.readBoolean();
    }
}