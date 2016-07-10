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

package com.gigaspaces.internal.cluster.node.impl.packets.data.filters;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.AbstractEntryType;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.entry.UnusableEntryException;

import java.rmi.MarshalledObject;

/**
 * Wraps a new {@link IReplicationPacketEntryData} that has an inner entry and esposes it self as a
 * replication filter entry
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationFilterEntryDataWrapper extends AbstractEntryType
        implements IReplicationFilterEntry {
    private static final long serialVersionUID = 1L;

    private final AbstractReplicationPacketSingleEntryData _data;
    private final IEntryPacket _entryPacket;
    private final ReplicationOperationType _operationType;
    private final int _objectType;


    public ReplicationFilterEntryDataWrapper(AbstractReplicationPacketSingleEntryData data, IEntryPacket entryPacket,
                                             ITypeDesc typeDesc, ReplicationOperationType operationType, int objectType) {
        super(typeDesc);
        _data = data;
        _entryPacket = entryPacket;
        _operationType = operationType;
        _objectType = objectType;
    }


    public void discard() {
        _data.clear();
    }


    public int getObjectType() {
        return _objectType;
    }

    public ReplicationOperationType getOperationType() {
        return _operationType;
    }


    public boolean isDiscarded() {
        return _data.isEmpty();
    }


    public void setFieldsValues(Object[] values) {
        _entryPacket.setFieldsValues(values);
    }

    public void setTimeToLive(long time) {
        _entryPacket.setTTL(time);
    }

    public MarshalledObject getHandback() {
        return null;
    }

    public int getNotifyType() {
        return 0;
    }

    @Override
    public Object getObject(IJSpace space) throws UnusableEntryException {
        if (_entryPacket == null)
            return null;

        _entryPacket.setTypeDesc(_typeDesc, false);
        return _entryPacket.toObject(_entryPacket.getEntryType());
    }

    public Object getFieldValue(int position) throws IllegalArgumentException,
            IllegalStateException {
        return _entryPacket.getFieldValue(position);
    }

    public Object getFieldValue(String fieldName)
            throws IllegalArgumentException, IllegalStateException {
        return _entryPacket.getPropertyValue(fieldName);
    }


    public Object[] getFieldsValues() {
        return _entryPacket.getFieldValues();
    }


    public java.util.Map.Entry getMapEntry() {
        return null;
    }


    public long getTimeToLive() {
        return _entryPacket.getTTL();
    }


    public String getUID() {
        return _entryPacket.getUID();
    }


    public int getVersion() {
        return _entryPacket.getVersion();
    }


    public boolean isTransient() {
        return _entryPacket.isTransient();
    }


    public Object setFieldValue(String fieldName, Object value)
            throws IllegalArgumentException, IllegalStateException {
        _entryPacket.setPropertyValue(fieldName, value);
        return _entryPacket.getPropertyValue(fieldName);
    }


    public Object setFieldValue(int position, Object value)
            throws IllegalArgumentException, IllegalStateException {
        _entryPacket.setFieldValue(position, value);
        //this is the code corresponding to today's behavior.
        return _entryPacket.getFieldValue(position);
    }

    @Override
    public String toString() {
        return "ReplicationFilterEntryDataWrapper [" + _data + "]";
    }

}
