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

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.server.space.metadata.ServerTypeDesc;

import net.jini.core.entry.UnusableEntryException;

/**
 * A wrapper around an entry's class-type meta-data.
 *
 * @author moran
 * @version 1.0
 * @since 5.1
 */
public abstract class AbstractEntryType implements IGSEntry {
    private static final long serialVersionUID = -2890305376610469713L;

    private static final ITypeDesc DUMMY_TYPE_TABLE_ENTRY = new ServerTypeDesc(-1, "com.gigaspacs.DummyType").getTypeDesc();

    protected final ITypeDesc _typeDesc;

    /**
     * Constructs an abstract entry with is class-type meta-data.
     *
     * @param typeDesc the entry class-type.
     */
    public AbstractEntryType(ITypeDesc typeDesc) {
        this._typeDesc = typeDesc == null ? DUMMY_TYPE_TABLE_ENTRY : typeDesc;
    }

    public boolean hasDummyTTE() {
        return _typeDesc == DUMMY_TYPE_TABLE_ENTRY;
    }

    /**
     * {@inheritDoc}
     */
    public String getClassName() {
        return _typeDesc.getTypeName();
    }

    public PropertyInfo[] getProperties() {
        return _typeDesc.getProperties();
    }

    public String[] getFieldsNames() {
        final PropertyInfo[] properties = getProperties();
        String[] res = new String[properties.length];
        for (int i = 0; i < properties.length; i++) {
            res[i] = properties[i].getName();
        }

        return res;
    }

    public boolean[] getIndexIndicators() {
        final PropertyInfo[] properties = getProperties();
        boolean[] res = new boolean[properties.length];
        for (int i = 0; i < properties.length; i++)
            res[i] = isIndexedField(properties[i].getName());

        return res;
    }

    public String[] getFieldsTypes() {
        final PropertyInfo[] properties = getProperties();
        String[] res = new String[properties.length];
        for (int i = 0; i < properties.length; i++)
            res[i] = properties[i].getTypeName();

        return res;
    }

    /**
     * {@inheritDoc}
     */
    public String getCodebase() {
        return _typeDesc.getCodeBase();
    }

    /**
     * {@inheritDoc}
     */
    public String[] getSuperClassesNames() {
        return _typeDesc.getRestrictSuperClassesNames();
    }

    public String getRoutingFieldName() {
        return _typeDesc.getRoutingPropertyName();
    }

    /**
     * {@inheritDoc}
     *
     * @return the field name of the first indexed field (can be null).
     */
    public String getPrimaryKeyName() {
        boolean[] indexIndicators = getIndexIndicators();

        if (indexIndicators != null) {
            for (int i = 0; i < indexIndicators.length; i++)
                if (indexIndicators[i])
                    return getFieldsNames()[i];
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isFifo() {
        return _typeDesc.isFifoDefault();
    }

    public String getFieldType(String fieldName) {
        try {
            return _typeDesc.getFixedProperty(fieldName).getTypeName();
        } catch (Exception e) {
            throw new IllegalStateException("The field types array was not properly set", e);
        }
    }

    public boolean isIndexedField(String fieldName) {
        try {
            return _typeDesc.getIndexType(fieldName).isIndexed();
        } catch (Exception e) {
            throw new IllegalStateException("The field indexes array was not properly set", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean isReplicatable() {
        return _typeDesc.isReplicable();
    }

    public int getFieldPosition(String fieldName) {
        for (int i = 0; i < _typeDesc.getNumOfFixedProperties(); ++i)
            if (_typeDesc.getFixedProperty(i).getName().equals(fieldName))
                return i;

        throw new IllegalArgumentException("Field name " + fieldName + " is not available");
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(IJSpace space) throws UnusableEntryException {
        return ((ISpaceProxy) space).getDirectProxy().getTypeManager().getObjectFromIGSEntry(this);
    }
}
