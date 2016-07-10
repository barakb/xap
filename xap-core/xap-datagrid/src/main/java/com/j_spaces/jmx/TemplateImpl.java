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


package com.j_spaces.jmx;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.TemplateInfo;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;

/**
 * Implements IGSTemplate
 *
 * @author Guyk
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class TemplateImpl implements IGSEntry {
    private static final long serialVersionUID = -6239273862115667186L;

    private ITypeDesc _typeDesc;
    private TemplateInfo _template;

    /*
     * Empty default contructor
     */
    public TemplateImpl() {
    }

    /**
     * @param template
     * @param typeDesc
     */
    public TemplateImpl(TemplateInfo template, ITypeDesc typeDesc) {
        _typeDesc = typeDesc;
        _template = template;
    }

    public String getClassName() {
        return _typeDesc.getTypeName();
    }

    public String getCodebase() {
        return _typeDesc.getCodeBase();
    }

    public String[] getSuperClassesNames() {
        return _typeDesc.getRestrictSuperClassesNames();
    }

    public String[] getFieldsNames() {
        return _typeDesc.getPropertiesNames();
    }

    public String[] getFieldsTypes() {
        return _typeDesc.getPropertiesTypes();
    }

    public boolean[] getIndexIndicators() {
        return _typeDesc.getPropertiesIndexTypes();
    }

    public String getRoutingFieldName() {
        return _typeDesc.getRoutingPropertyName();
    }

    public String getPrimaryKeyName() {
        boolean[] indexIndicators = getIndexIndicators();

        if (indexIndicators != null) {
            for (int i = 0; i < indexIndicators.length; i++)
                if (indexIndicators[i])
                    return getFieldsNames()[i];
        }

        return null;
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

    public boolean isReplicatable() {
        return _typeDesc.isReplicable();
    }

    public Object getFieldValue(String fieldName)
            throws IllegalArgumentException, IllegalStateException {
        return getFieldValue(getFieldPosition(fieldName));
    }

    public Object getFieldValue(int position)
            throws IllegalArgumentException, IllegalStateException {
        return getFieldsValues()[position];
    }

    public int getFieldPosition(String fieldName) {
        for (int i = 0; i < _typeDesc.getNumOfFixedProperties(); ++i)
            if (_typeDesc.getFixedProperty(i).getName().equals(fieldName))
                return i;

        throw new IllegalArgumentException("Field name " + fieldName + " is not available");
    }

    @Deprecated
    public Entry getEntry(IJSpace space)
            throws UnusableEntryException {
        // TODO Auto-generated method stub
        return null;
    }

    public Object[] getFieldsValues() {
        return _template.getValues();
    }

    public java.util.Map.Entry getMapEntry() {
        // TODO Auto-generated method stub
        return null;
    }

    public Object getObject(IJSpace space) throws UnusableEntryException {
        // TODO Auto-generated method stub
        return null;
    }

    public long getTimeToLive() {
        return _template.getExpiresAt().getTime();
    }

    public String getUID() {
        return null;
    }

    public int getVersion() {
        return 0;
    }

    public boolean isFifo() {
        return _template.isFIFO();
    }

    public Object setFieldValue(String fieldName, Object value)
            throws IllegalArgumentException, IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    public Object setFieldValue(int position, Object value)
            throws IllegalArgumentException, IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isTransient() {
        return true;
    }
}
