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

package com.j_spaces.core;

import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.server.storage.UserTypeEntryData;
import com.gigaspaces.internal.transport.AbstractEntryPacket;
import com.gigaspaces.internal.transport.TransportPacketType;

import java.util.Map;

@com.gigaspaces.api.InternalApi
public class LocalCacheResponseEntryPacket extends AbstractEntryPacket {
    private static final long serialVersionUID = 1L;

    private final Object _object;
    private final ITypeIntrospector<Object> _introspector;
    private boolean _returnOnlyUIDs;
    private String _uid;

    /**
     * Required for Externalizable
     */
    public LocalCacheResponseEntryPacket() {
        throw new IllegalStateException("This constructor is required for Externalizable and should not be called directly.");
    }

    public LocalCacheResponseEntryPacket(UserTypeEntryData entryData, String uid) {
        super(entryData.getEntryTypeDesc().getTypeDesc(), entryData.getEntryTypeDesc().getEntryType());
        super.setSerializeTypeDesc(false);
        this._object = entryData.getUserObject();
        this._introspector = entryData.getEntryTypeDesc().getIntrospector();

        //if object doesn't have an id field - keep the uid in the packet
        // it is needed by the local cache
        if (_typeDesc.getIdentifierPropertyId() == -1)
            _uid = uid;

    }

    public TransportPacketType getPacketType() {
        return TransportPacketType.ENTRY_PACKET;
    }

    public String getTypeName() {
        return _typeDesc.getTypeName();
    }

    @Override
    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        if (_object != null && entryType == _entryType)
            return _object;

        return super.toObject(entryType, storageTypeDeserialization);
    }

    public Object getFieldValue(int index) {
        return _introspector.getValue(_object, index);
    }

    public void setFieldValue(int index, Object value) {
        _introspector.setValue(_object, value, index);
    }

    public Object[] getFieldValues() {
        return _typeDesc.getIntrospector(_entryType).getSerializedValues(_object);
    }

    public void setFieldsValues(Object[] values) {
        throw new UnsupportedOperationException("Internal Error - Please contact GigaSpaces Support.");
    }

    public String[] getMultipleUIDs() {
        return null;
    }

    public void setMultipleUIDs(String[] uids) {
    }

    public boolean isReturnOnlyUids() {
        return _returnOnlyUIDs;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
        this._returnOnlyUIDs = returnOnlyUIDs;
    }

    public String getUID() {
        if (_uid != null)
            return _uid;
        return _introspector.getUID(_object);
    }

    public void setUID(String uid) {
        _introspector.setUID(_object, uid);
    }

    public int getVersion() {
        return _introspector.getVersion(_object);
    }

    public void setVersion(int version) {
        _introspector.setVersion(_object, version);
    }

    public long getTTL() {
        return _introspector.getTimeToLive(_object);
    }

    public void setTTL(long ttl) {
        _introspector.setTimeToLive(_object, ttl);
    }


    public boolean isFifo() {
        return false;
    }

    public boolean isTransient() {
        return _introspector.isTransient(_object);
    }

    public boolean isNoWriteLease() {
        return false;
    }

    public ICustomQuery getCustomQuery() {
        return null;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
    }

    public Map<String, Object> getDynamicProperties() {
        return _introspector.getDynamicProperties(_object);
    }

    @Override
    public void setDynamicProperties(Map<String, Object> properties) {
        _introspector.setDynamicProperties(_object, properties);
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    @Override
    public boolean hasFixedPropertiesArray() {
        return false;
    }


}
