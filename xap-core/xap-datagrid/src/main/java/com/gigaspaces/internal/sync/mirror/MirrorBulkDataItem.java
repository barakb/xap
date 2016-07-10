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

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperationType;
import com.j_spaces.sadapter.datasource.IDataConverter;
import com.j_spaces.sadapter.datasource.InternalBulkItem;

import java.util.HashMap;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class MirrorBulkDataItem implements InternalBulkItem {
    private final short _operation;
    protected final IEntryPacket _dataPacket;
    protected IDataConverter<IEntryPacket> _converter;

    public MirrorBulkDataItem(IEntryPacket dataPacket, short operation) {
        _operation = operation;
        _dataPacket = dataPacket;
    }

    public Object getItem() {
        if (_converter != null)
            return _converter.toObject(_dataPacket);

        return this;
    }

    public short getOperation() {
        return _operation;
    }

    public IDataConverter<IEntryPacket> getConverter() {
        return _converter;
    }

    public void setConverter(IDataConverter<IEntryPacket> converter) {
        _converter = converter;
    }

    @Override
    public String toString() {
        String operation = null;

        switch (_operation) {
            case REMOVE:
                operation = "REMOVE";
                break;
            case UPDATE:
                operation = "UPDATE";
                break;
            case WRITE:
                operation = "WRITE";
                break;
            case PARTIAL_UPDATE:
                operation = "PARTIAL UPDATE";
                break;

            default:
                operation = "UNKNOWN";
                break;
        }

        return "BulkDataItem<Op: " + operation + ", " + _dataPacket.toString() + ">";
    }

    public String getTypeName() {
        return _dataPacket.getTypeName();
    }

    public String getIdPropertyName() {
        return _dataPacket.getTypeDescriptor().getIdPropertyName();
    }

    public Object getIdPropertyValue() {
        return _dataPacket.getID();
    }

    @Override
    public boolean supportsGetSpaceId() {
        return true;
    }

    @Override
    public Object getSpaceId() {
        return getIdPropertyValue();
    }

    public Map<String, Object> getItemValues() {
        Map<String, Object> props = new HashMap<String, Object>();

        ITypeDesc typeDesc = _dataPacket.getTypeDescriptor();
        for (int i = 0; i < typeDesc.getNumOfFixedProperties(); i++) {
            props.put(typeDesc.getFixedProperty(i).getName(), _dataPacket.getFieldValue(i));
        }

        if (_dataPacket.getDynamicProperties() != null)
            props.putAll(_dataPacket.getDynamicProperties());
        return props;
    }

    @Override
    public String getUid() {
        return _dataPacket.getUID();
    }

    @Override
    public DataSyncOperationType getDataSyncOperationType() {
        switch (_operation) {
            case BulkItem.WRITE:
                return DataSyncOperationType.WRITE;
            case BulkItem.UPDATE:
                return DataSyncOperationType.UPDATE;
            case BulkItem.PARTIAL_UPDATE:
                return DataSyncOperationType.PARTIAL_UPDATE;
            default:
                return DataSyncOperationType.REMOVE;
        }
    }

    @Override
    public Object getDataAsObject() {
        if (!supportsDataAsObject())
            throw new UnsupportedOperationException();
        return getItem();
    }

    @Override
    public SpaceDocument getDataAsDocument() {
        if (!supportsDataAsDocument())
            throw new UnsupportedOperationException();
        return _converter.toDocument(_dataPacket);
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        if (!supportsGetTypeDescriptor())
            throw new UnsupportedOperationException();
        return _dataPacket.getTypeDescriptor();
    }

    @Override
    public boolean supportsGetTypeDescriptor() {
        return _dataPacket.getTypeDescriptor() != null;
    }

    @Override
    public boolean supportsDataAsObject() {
        return supportsGetTypeDescriptor() && _dataPacket.getTypeDescriptor().isConcreteType();
    }

    @Override
    public boolean supportsDataAsDocument() {
        return true;
    }

}
