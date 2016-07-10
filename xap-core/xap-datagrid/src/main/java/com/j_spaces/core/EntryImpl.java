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

import com.gigaspaces.internal.client.cache.ISpaceCache;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.server.storage.IEntryHolder;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;

import java.rmi.MarshalledObject;
import java.util.Map;

/**
 * Entry with type table information.
 *
 * @author moran
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class EntryImpl extends AbstractEntryType {
    private static final long serialVersionUID = -1936556410152597597L;

    protected final IEntryHolder _entryHolder;

    /**
     * Constructs an Entry implementation including its class-type meta-data.
     *
     * @param entryHolder the entry holder (internal entry representation).
     * @param typeDesc    the class-type meta-data of this entry.
     */
    public EntryImpl(IEntryHolder entryHolder, ITypeDesc typeDesc) {
        super(typeDesc);
        this._entryHolder = entryHolder;
    }

    public Map.Entry getMapEntry() {
        return null;
    }

    public String getUID() {
        return _entryHolder.getUID();
    }

    @Override
    public String getClassName() {
        return _entryHolder.getClassName();
    }

    public Object[] getFieldsValues() {
        return _entryHolder.getEntryData().getFixedPropertiesValues();
    }

    public boolean isTransient() {
        return _entryHolder.isTransient();
    }

    public long getTimeToLive() {
        return _entryHolder.getEntryData().getExpirationTime();
    }

    public int getVersion() {
        return _entryHolder.getEntryData().getVersion();
    }

    public Object getFieldValue(String fieldName) {
        return _entryHolder.getEntryData().getPropertyValue(fieldName);
    }

    public Object getFieldValue(int index) {
        return _entryHolder.getEntryData().getFixedPropertyValue(index);
    }

    public Object setFieldValue(String fieldName, Object value) {
        for (int i = 0; i < _typeDesc.getNumOfFixedProperties(); ++i) {
            if (_typeDesc.getFixedProperty(i).getName().equals(fieldName)) {
                return setFieldValue(i, value);
            }
        }

        if (_entryHolder.getEntryData().getDynamicProperties() != null)
            return _entryHolder.getEntryData().getDynamicProperties().put(fieldName, value);
        else
            throw new IllegalArgumentException("Unknown field name '" + fieldName + "'");
    }

    ////NOTE: we bypass the snapshot mechanism!!!!!!
    public Object setFieldValue(int index, Object value) {//NOTE: we bypass the snapshot mechanism!!!!!!
        Object[] values = getFieldsValues();
        Object old = values[index];
        values[index] = value;
        return old;
    }

    /*
     * @see com.j_spaces.core.IGSEntry#getEntry(com.j_spaces.core.IJSpace)
     */
    public Entry getEntry(IJSpace space) throws UnusableEntryException {
        return toObject(space, EntryType.OBJECT_JAVA);
    }

    private <T> T toObject(IJSpace space, EntryType entryType) throws UnusableEntryException {
        final ITypeDesc typeDesc = getDirectSpace(space).getTypeManager().getTypeDescByName(this.getClassName(), this.getCodebase());
        final ITypeIntrospector<T> introspector = typeDesc.getIntrospector(entryType);
        return introspector.toObject(this, typeDesc);
    }

    /**
     * Get direct space proxy, in local space returns the local space.
     */
    private static IDirectSpaceProxy getDirectSpace(IJSpace space) {
        if (space instanceof ISpaceCache)
            return ((ISpaceCache) space).getLocalSpace();

        return (IDirectSpaceProxy) space;
    }

    /**
     * Returns the handbag represented by this Entry.
     *
     * @return handbag object.
     */
    public MarshalledObject getHandback() {
        return _entryHolder.getHandback();
    }

    /**
     * Returns the notify type represented by this Entry.
     *
     * @return return one of {@link com.j_spaces.core.client.NotifyModifiers}
     */
    public int getNotifyType() {
        return _entryHolder.getNotifyType();
    }
}
