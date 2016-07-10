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

import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.AbstractEntryType;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.entry.UnusableEntryException;

import java.rmi.MarshalledObject;
import java.util.Map.Entry;

@com.gigaspaces.api.InternalApi
public class ReplicationFilterUidDataWrapper
        extends AbstractEntryType implements IReplicationFilterEntry {

    private static final long serialVersionUID = 1L;

    private final AbstractReplicationPacketSingleEntryData _data;
    private final String _uid;
    private final ReplicationOperationType _operationType;
    private final int _objectType;
    private final ITimeToLiveUpdateCallback _timeToLiveCallback;


    public ReplicationFilterUidDataWrapper(
            AbstractReplicationPacketSingleEntryData data,
            String uid, ITypeDesc typeDesc,
            ReplicationOperationType operationType,
            int objectType, ITimeToLiveUpdateCallback timeToLiveCallback) {
        super(typeDesc);
        _data = data;
        _uid = uid;
        _operationType = operationType;
        _objectType = objectType;
        _timeToLiveCallback = timeToLiveCallback;
    }

    protected AbstractReplicationPacketSingleEntryData getData() {
        return _data;
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
        throw new UnsupportedOperationException();
    }

    public void setTimeToLive(long time) {
        if (_timeToLiveCallback != null)
            _timeToLiveCallback.updateTimeToLive(time);
    }

    public MarshalledObject getHandback() {
        return null;
    }

    public int getNotifyType() {
        return 0;
    }


    @Override
    public Object getObject(IJSpace space) throws UnusableEntryException {
        throw new UnsupportedOperationException("getObject() is not supported for " + _operationType);
    }

    public Object getFieldValue(int position) throws IllegalArgumentException,
            IllegalStateException {
        throw new UnsupportedOperationException();
    }

    public Object[] getFieldsValues() {
        return null;
    }

    public Entry getMapEntry() {
        throw new UnsupportedOperationException();
    }

    public long getTimeToLive() {
        if (_timeToLiveCallback == null)
            return 0L;
        return _timeToLiveCallback.getTimeToLive();
    }

    public String getUID() {
        return _uid;
    }

    public int getVersion() {
        return 0;
    }

    public boolean isTransient() {
        return _data.isTransient();
    }

    public Object setFieldValue(String fieldName, Object value)
            throws IllegalArgumentException, IllegalStateException {
        throw new UnsupportedOperationException();
    }

    public Object setFieldValue(int position, Object value)
            throws IllegalArgumentException, IllegalStateException {
        throw new UnsupportedOperationException();
    }

    public Object getFieldValue(String fieldName)
            throws IllegalArgumentException, IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "ReplicationFilterUidDataWrapper [" + _data + "]";
    }

}
