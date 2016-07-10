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

package com.gigaspaces.internal.cluster.node.impl.replica.data.filters;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.AbstractEntryType;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.rmi.MarshalledObject;
import java.util.Map.Entry;

@com.gigaspaces.api.InternalApi
public class ReplicationFilterEntryReplicaDataWrapper
        extends AbstractEntryType
        implements IReplicationFilterEntry {

    private final IEntryPacket _entryPacket;
    private boolean _discarded;

    public ReplicationFilterEntryReplicaDataWrapper(IEntryPacket entryPacket,
                                                    ITypeDesc typeDesc) {
        super(typeDesc);
        _entryPacket = entryPacket;
    }

    private static final long serialVersionUID = 1L;

    public void discard() {
        _discarded = true;
    }

    public int getObjectType() {
        return ObjectTypes.ENTRY;
    }

    public ReplicationOperationType getOperationType() {
        return ReplicationOperationType.WRITE;
    }

    public boolean isDiscarded() {
        return _discarded;
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

    public Object getFieldValue(int position) throws IllegalArgumentException,
            IllegalStateException {
        return _entryPacket.getFieldValue(position);
    }

    public Object[] getFieldsValues() {
        return _entryPacket.getFieldValues();
    }

    public Entry getMapEntry() {
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
        // this is the code corresponding to today's behavior.
        return _entryPacket.getFieldValue(position);
    }

    public Object getFieldValue(String fieldName)
            throws IllegalArgumentException, IllegalStateException {
        return _entryPacket.getPropertyValue(fieldName);
    }

}
