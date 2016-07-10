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

package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.sync.DataSyncOperationType;

import java.util.HashMap;
import java.util.Map;

/**
 * BulkDataItem is used by the space for execution of bulk operations <br> in case of transactions
 * and mirror replication.<br>
 *
 * BulkDataItem contains the data object and the operation that needs to be executed.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class BulkDataItem extends EntryAdapter implements InternalBulkItem {
    private static final long serialVersionUID = -2854220047028924547L;

    private final short _operation;

    /**
     * Constructs a {@link BulkDataItem} out of IEntryHolder and TypeTableEntry.
     *
     * @param entryHolder underlying entry holder
     * @param typeDesc    underlying entry type information
     * @param operation   one of the qualified {@link BulkItem} operations.
     */
    public BulkDataItem(IEntryHolder entryHolder, ITypeDesc typeDesc, short operation) {
        super(entryHolder, typeDesc);
        _operation = operation;
    }

    /**
     * Constructs a {@link BulkDataItem} out of IEntryHolder and TypeTableEntry.<br> Given converter
     * is used for internal entry conversion.<br>
     */
    public BulkDataItem(IEntryHolder entryHolder, ITypeDesc typeDesc, short operation, IDataConverter<IEntryPacket> converter) {
        super(entryHolder, typeDesc, converter);
        _operation = operation;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.sadapter.datasource.BulkDataItem#getItem()
     */
    public Object getItem() {
        return toObject();
    }

    /* (non-Javadoc)
     * @see com.j_spaces.sadapter.datasource.BulkDataItem#getOperation()
     */
    public short getOperation() {
        return _operation;
    }


    /*
     * @see com.j_spaces.sadapter.datasource.BulkDataItem#toString()
	 */
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

        return "BulkDataItem<Op: " + operation + ", "
                + super.toString() + ">";
    }

    public String getTypeName() {
        return _entryHolder.getClassName();
    }

    private ITypeDesc getTypeDesc() {
        return _entryHolder.getServerTypeDesc().getTypeDesc();
    }

    public String getIdPropertyName() {
        return getTypeDesc().getIdPropertyName();
    }

    public Object getIdPropertyValue() {
        ITypeDesc typeDesc = getTypeDesc();
        if (typeDesc.getIdPropertyName() != null && typeDesc.isAutoGenerateId())
            return _entryHolder.getUID();

        return _entryHolder.getEntryData().getPropertyValue(getIdPropertyName());
    }

    @Override
    public Object getSpaceId() {
        return getIdPropertyValue();
    }

    @Override
    public boolean supportsGetSpaceId() {
        return StringUtils.hasText(getTypeDesc().getIdPropertyName());
    }

    public Map<String, Object> getItemValues() {
        final ITypeDesc typeDesc = getTypeDesc();
        final IEntryData entryData = _entryHolder.getEntryData();
        final Map<String, Object> props = new HashMap<String, Object>();

        for (int i = 0; i < typeDesc.getNumOfFixedProperties(); i++)
            props.put(typeDesc.getFixedProperty(i).getName(), entryData.getFixedPropertyValue(i));
        if (entryData.getDynamicProperties() != null)
            props.putAll(entryData.getDynamicProperties());
        return props;
    }

    @Override
    public String getUid() {
        return _entryHolder.getUID();
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
        return toObject();
    }

    @Override
    public SpaceDocument getDataAsDocument() {
        if (!supportsDataAsDocument())
            throw new UnsupportedOperationException();
        return toDocument();
    }

    @Override
    public boolean supportsGetTypeDescriptor() {
        return _typeDesc != null;
    }

    @Override
    public boolean supportsDataAsObject() {
        return (_typeDesc != null && _typeDesc.isConcreteType()) || getMapEntry() != null;
    }

    @Override
    public boolean supportsDataAsDocument() {
        return true;
    }
}
