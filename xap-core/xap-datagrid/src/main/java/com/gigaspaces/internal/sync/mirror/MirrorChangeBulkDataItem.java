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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperationType;
import com.gigaspaces.sync.change.ChangeOperation;
import com.gigaspaces.sync.change.DataSyncChangeSet;
import com.j_spaces.sadapter.datasource.IDataConverter;
import com.j_spaces.sadapter.datasource.InternalBulkItem;

import java.util.Collection;
import java.util.Map;

/**
 * Wraps a change operation as bulk item
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class MirrorChangeBulkDataItem
        implements InternalBulkItem, DataSyncChangeSet {

    private final ITypeDesc _typeDesc;
    private final String _uid;
    private final long _timeToLive;
    private final Collection _mutators;
    private final int _version;
    private final Object _id;

    public MirrorChangeBulkDataItem(ITypeDesc typeDesc, String uid, Object id, int version, long timeToLive, Collection<SpaceEntryMutator> mutators) {
        _typeDesc = typeDesc;
        _uid = uid;
        _id = id;
        _version = version;
        _timeToLive = timeToLive;
        _mutators = mutators;
    }

    @Override
    public Object getItem() {
        return null;
    }

    @Override
    public short getOperation() {
        return BulkItem.CHANGE;
    }

    @Override
    public String getTypeName() {
        return _typeDesc.getTypeName();
    }

    @Override
    public String getIdPropertyName() {
        return _typeDesc.getIdPropertyName();
    }

    @Override
    public Object getIdPropertyValue() {
        return _id;
    }

    @Override
    public Map<String, Object> getItemValues() {
        return null;
    }

    @Override
    public String getUid() {
        return _uid;
    }

    @Override
    public DataSyncOperationType getDataSyncOperationType() {
        return DataSyncOperationType.CHANGE;
    }

    @Override
    public Object getDataAsObject() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpaceDocument getDataAsDocument() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return _typeDesc;
    }

    @Override
    public boolean supportsGetTypeDescriptor() {
        return true;
    }

    @Override
    public boolean supportsDataAsObject() {
        return false;
    }

    @Override
    public boolean supportsDataAsDocument() {
        return false;
    }

    @Override
    public void setConverter(IDataConverter<IEntryPacket> converter) {

    }

    @Override
    public Collection<ChangeOperation> getOperations() {
        return _mutators;
    }

    @Override
    public Object getId() {
        return _id;
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public long getTimeToLive() {
        return _timeToLive;
    }

    @Override
    public boolean supportsGetSpaceId() {
        return true;
    }

    @Override
    public Object getSpaceId() {
        return getId();
    }

    /*
     * @see com.j_spaces.sadapter.datasource.BulkDataItem#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BulkDataItem<Op: CHANGE, Entry<");
        sb.append(getTypeName());
        sb.append(", UID: " + getUid());
        sb.append(", ID: " + _id);
        sb.append(", Changes: " + getOperations());
        sb.append(">");
        return sb.toString();
    }

}
