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

package com.j_spaces.core.cache;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.metadata.index.ISpaceCompoundIndexSegment;
import com.j_spaces.kernel.SystemProperties;

import java.util.Map;

/**
 * segment for a compound index structure
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class CompoundIndexSegmentTypeData {
    private final ISpaceCompoundIndexSegment _segment;

    private final TypeDataIndex _originatingIndex;

    private final ThreadLocal<IValueCloner> _valueCloner;
    private final boolean _considerValueClone;
    private final boolean _cloneableIndexValue;
    //is value type fixed & known ?
    private final boolean _valueTypeKnown;

    private Class<?> _valueType;
    private boolean _valueTypeFinallySet;

    private final int _segmentPosition;
    private final int _fixedPropertyPosition; //if the (root) is a fixed property

    public CompoundIndexSegmentTypeData(ISpaceCompoundIndexSegment segment, Map<String, TypeDataIndex<?>> indexTable, IServerTypeDesc serverTypeDesc) {
        _segment = segment;
        TypeDataIndex originatingIndex = indexTable.get(segment.getName());
        _segmentPosition = segment.getSegmentPosition();
        if (originatingIndex != null) {
            _originatingIndex = originatingIndex;
            _valueCloner = null;
            _considerValueClone = originatingIndex.isConsiderValueClone();
            _cloneableIndexValue = false; //NOTE- clonning done inside the other index, value not important
            _valueTypeKnown = false; //NOTE- clonning done inside the other index, value not important
            _valueTypeFinallySet = true;
        } else {//we need to handle value clonning here since there is no index
            _originatingIndex = null;
            _valueCloner = new ThreadLocal<IValueCloner>();
            String val = System.getProperty(SystemProperties.CACHE_MANAGER_EMBEDDED_INDEX_PROTECTION);

            Class<?> valueClass = null;
            boolean embeddedIndexProtection = new Boolean(val != null ? val : SystemProperties.CACHE_MANAGER_EMBEDDED_INDEX_PROTECTION_DEFAULT);
            if (segment.isPropertySegment()) {
                //try find the property class value
                int pos = serverTypeDesc.getTypeDesc().getFixedPropertyPosition(segment.getName());
                if (pos != -1) {
                    PropertyInfo property = serverTypeDesc.getTypeDesc().getFixedProperty(pos);
                    valueClass = property.getType();
                    _valueType = valueClass;
                } else {
                    valueClass = Object.class;
                }
                _valueTypeFinallySet = true;
            }
            if (!embeddedIndexProtection) {
                _considerValueClone = false;
                _cloneableIndexValue = false;
                _valueTypeKnown = false;
            } else {
                if (valueClass == null || valueClass.getName().equals("java.lang.Object")) {//
                    _valueTypeKnown = false;
                    _considerValueClone = true;
                    _cloneableIndexValue = false;
                } else {//check the type
                    _valueTypeKnown = true;
                    //check if immutable
                    if (TypeDataIndex.isImmutableIndexValue(valueClass)) {
                        _considerValueClone = false;
                        _cloneableIndexValue = false;
                    } else {
                        _considerValueClone = true;
                        _cloneableIndexValue = TypeDataIndex.isCloneableIndexValue(valueClass);
                    }
                }
            }

        }
        String name = segment.getName();
        if (segment.isPropertySegment() || (!segment.getName().contains(".")))
            _fixedPropertyPosition = serverTypeDesc.getTypeDesc().getFixedPropertyPosition(name);
        else
            _fixedPropertyPosition = serverTypeDesc.getTypeDesc().getFixedPropertyPosition(name.substring(0, name.indexOf(".")));
    }


    public ISpaceCompoundIndexSegment getDefinitionSegment() {
        return _segment;
    }

    public boolean isConsiderValueClone() {
        return _considerValueClone;
    }

    public <K> K cloneIndexValue(K fieldValue, IEntryHolder entryHolder) {
        if (_originatingIndex == null && _valueType == null && fieldValue != null)
            updateValueType(fieldValue);

        if (!isConsiderValueClone() || fieldValue == null)
            return fieldValue;
        if (_originatingIndex != null)
            return (K) _originatingIndex.cloneIndexValue(fieldValue, entryHolder);

        Class<?> clzz = !_valueTypeKnown ? fieldValue.getClass() : getValueType();
        if (!_valueTypeKnown && TypeDataIndex.isImmutableIndexValue(clzz))
            return fieldValue;
        if (_valueCloner.get() == null)
            _valueCloner.set(new DefaultValueCloner());

        K res = (K) _valueCloner.get().cloneValue(fieldValue, _cloneableIndexValue, clzz, entryHolder.getUID(), entryHolder.getClassName());

        //perfom an health check
        if (res != fieldValue && (fieldValue.hashCode() != res.hashCode() || !fieldValue.equals(res)))
            throw new RuntimeException("Entry Class: " + entryHolder.getClassName() +
                    " - Wrong hashCode() or equals() implementation of " +
                    fieldValue.getClass() + " class field");

        return res;
    }

    public Class<?> getValueType() {
        return _originatingIndex != null ? _originatingIndex.getValueType() : _valueType;

    }

    /**
     * set the value type if needed, returns true if the type is finally set
     */
    boolean updateValueType(Object value) {
        if (_valueTypeFinallySet || value == null)
            return _valueTypeFinallySet;
        Class<?> type = value.getClass();
        // If first time, just set the value:
        if (_valueType == null) {
            synchronized (this) {
                if (_valueType == null)
                    _valueType = type;
            }
        }
        // Otherwise, if not the same type or a parent type, look for a common super type:
        else if (_valueType != type && !_valueType.isAssignableFrom(type)) {
            synchronized (this) {
                _valueType = TypeDataIndex.getCommonSuperType(type, _valueType);
            }
        }
        if (_valueType == Object.class)
            _valueTypeFinallySet = true;
        return _valueTypeFinallySet;
    }

    public TypeDataIndex getOriginatingIndex() {
        return _originatingIndex;
    }

    public int getSegmentPoition() {
        return _segmentPosition;
    }

    public int getFixedPropertyPos() {
        return _fixedPropertyPosition;
    }

    public boolean isValueTypeFinallySet() {
        return _valueTypeFinallySet;
    }
}
