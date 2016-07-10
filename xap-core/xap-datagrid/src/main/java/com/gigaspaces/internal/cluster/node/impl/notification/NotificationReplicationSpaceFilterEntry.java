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

package com.gigaspaces.internal.cluster.node.impl.notification;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.AbstractEntryType;
import com.j_spaces.core.client.NotifyModifiers;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;

import java.rmi.MarshalledObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class NotificationReplicationSpaceFilterEntry
        extends AbstractEntryType
        implements ISpaceFilterEntry {

    private static final long serialVersionUID = 1L;

    private final boolean _isTransient;
    private final String _uid;
    private final IEntryData _currentEntryData;
    private final IEntryData _previousEntryData;

    protected Map<String, Object> _dynamicValueChanges = null;
    protected Object[] _fixedValueChanges = null;
    protected boolean[] _fixedValueChangedIndicator = null;

    public NotificationReplicationSpaceFilterEntry(IEntryData currentEntryData, IEntryData previousEntryData, String uid, boolean isTransient) {
        super(currentEntryData.getEntryTypeDesc().getTypeDesc());
        _currentEntryData = currentEntryData;
        _previousEntryData = previousEntryData;
        _uid = uid;
        _isTransient = isTransient;
    }

    public NotificationReplicationSpaceFilterEntry(IEntryData entryData, String uid, boolean isTransient) {
        this(entryData, null, uid, isTransient);
    }

    public boolean isModified() {
        return _dynamicValueChanges != null || _fixedValueChanges != null;
    }

    @Override
    public Entry getMapEntry() {
        return null;
    }

    @Override
    public String getUID() {
        return _uid;
    }

    @Override
    public Object[] getFieldsValues() {
        Object[] result = _currentEntryData.getFixedPropertiesValues();

        if (_fixedValueChanges != null) {
            result = new Object[_fixedValueChanges.length];
            for (int i = 0; i < _fixedValueChanges.length; i++) {
                if (_fixedValueChangedIndicator[i])
                    result[i] = _fixedValueChanges[i];
                    // handle partial update case where field value was not changed
                else if (_previousEntryData != null && _currentEntryData.getFixedPropertiesValues()[i] == null)
                    result[i] = _previousEntryData.getFixedPropertiesValues()[i];
                else
                    result[i] = _currentEntryData.getFixedPropertiesValues()[i];
            }
        }

        return result;
    }

    public Map<String, Object> getDynamicValues() {
        Map<String, Object> result = _currentEntryData.getDynamicProperties();

        if (_dynamicValueChanges != null) {
            result = new HashMap<String, Object>(_currentEntryData.getDynamicProperties());
            result.putAll(_dynamicValueChanges);
        }

        return result;
    }

    @Override
    public Object getFieldValue(String fieldName)
            throws IllegalArgumentException, IllegalStateException {
        int fixedPropertyPosition = _typeDesc.getFixedPropertyPosition(fieldName);
        if (fixedPropertyPosition == -1) {
            if (_dynamicValueChanges != null && _dynamicValueChanges.containsKey(fieldName))
                return _dynamicValueChanges.get(fieldName);
            else
                return _currentEntryData.getPropertyValue(fieldName);
        }

        return getFieldValue(fixedPropertyPosition);
    }

    @Override
    public Object getFieldValue(int position) throws IllegalArgumentException,
            IllegalStateException {
        if (_fixedValueChanges != null && _fixedValueChangedIndicator[position])
            return _fixedValueChanges[position];
        else if (_previousEntryData != null && _currentEntryData.getFixedPropertyValue(position) == null)
            return _previousEntryData.getFixedPropertyValue(position);
        else
            return _currentEntryData.getFixedPropertyValue(position);
    }

    @Override
    public Object setFieldValue(String fieldName, Object value)
            throws IllegalArgumentException, IllegalStateException {
        int fixedPropertyPosition = _typeDesc.getFixedPropertyPosition(fieldName);
        if (fixedPropertyPosition == -1) {
            Object previousValue = getFieldValue(fieldName);
            if (_dynamicValueChanges == null)
                _dynamicValueChanges = new HashMap<String, Object>();

            _dynamicValueChanges.put(fieldName, value);
            return previousValue;
        }

        return setFieldValue(fixedPropertyPosition, value);
    }

    @Override
    public Object setFieldValue(int position, Object value)
            throws IllegalArgumentException, IllegalStateException {
        Object previousValue = getFieldValue(position);

        if (_fixedValueChanges == null) {
            _fixedValueChanges = new Object[_typeDesc.getNumOfFixedProperties()];
            _fixedValueChangedIndicator = new boolean[_typeDesc.getNumOfFixedProperties()];
        }

        _fixedValueChanges[position] = value;
        _fixedValueChangedIndicator[position] = true;

        return previousValue;
    }

    @Override
    public boolean isTransient() {
        return _isTransient;
    }

    @Override
    public long getTimeToLive() {
        return _currentEntryData.getTimeToLive(true);
    }

    @Override
    public int getVersion() {
        return _currentEntryData.getVersion();
    }

    @Override
    public MarshalledObject getHandback() {
        return null;
    }

    @Override
    public int getNotifyType() {
        return NotifyModifiers.NOTIFY_NONE;
    }


}
