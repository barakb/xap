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

package com.gigaspaces.internal.document;

import com.gigaspaces.document.DocumentObjectConverter;
import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpaceStringProperty;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.ClassUtils;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 8.0.1
 */
@com.gigaspaces.api.InternalApi
public class DocumentObjectConverterInternal extends DocumentObjectConverter {
    private static final DocumentObjectConverterInternal _instance = new DocumentObjectConverterInternal();

    public static DocumentObjectConverterInternal instance() {
        return _instance;
    }

    protected DocumentObjectConverterInternal() {
        super();
    }

    @Override
    public Object toDocumentIfNeeded(Object object, SpaceDocumentSupport documentSupport) {
        return super.toDocumentIfNeeded(object, documentSupport);
    }

    @Override
    public Object fromDocumentIfNeeded(Object object, SpaceDocumentSupport documentSupport, Class<?> expectedType) {
        return super.fromDocumentIfNeeded(object, documentSupport, expectedType);
    }

    public void convertNonPrimitivePropertiesToStrings(IEntryPacket entryPacket) {
        Object[] fixedProperties = entryPacket.getFieldValues();
        Object[] convertedFixedProperties = convertNonPrimitiveFixedPropertiesToStrings(fixedProperties, entryPacket.getTypeDescriptor());
        if (convertedFixedProperties != fixedProperties)
            entryPacket.setFieldsValues(convertedFixedProperties);

        Map<String, Object> dynamicProperties = entryPacket.getDynamicProperties();
        Map<String, Object> convertedDynamicProperties = convertNonPrimitiveDynamicPropertiesToStrings(dynamicProperties);
        if (convertedDynamicProperties != dynamicProperties)
            entryPacket.setDynamicProperties(convertedDynamicProperties);

    }

    public void convertNonPrimitivePropertiesToDocuments(IEntryPacket entryPacket) {
        Object[] values = entryPacket.getFieldValues();
        Object[] convertedValues = convertNonPrimitiveFixedPropertiesToDocuments(values, entryPacket.getTypeDescriptor());
        if (convertedValues != values)
            entryPacket.setFieldsValues(convertedValues);

        Map<String, Object> dynamicProperties = entryPacket.getDynamicProperties();
        Map<String, Object> convertedDynamicProperties = convertNonPrimitiveDynamicPropertiesToDocuments(dynamicProperties);
        if (convertedDynamicProperties != dynamicProperties)
            entryPacket.setDynamicProperties(convertedDynamicProperties);
    }

    private Object[] convertNonPrimitiveFixedPropertiesToStrings(Object[] values, ITypeDesc typeDesc) {
        Object[] convertedValues = null;
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] == null || typeDesc.getFixedProperty(i).isCommonJavaType())
                    continue;

                Object convertedValue = toSpaceStringProperty(values[i]);
                if (convertedValue != values[i]) {
                    if (convertedValues == null)
                        convertedValues = values.clone();
                    convertedValues[i] = convertedValue;
                }
            }
        }

        return convertedValues != null ? convertedValues : values;
    }

    public Object[] convertNonPrimitiveFixedPropertiesToDocuments(Object[] values, ITypeDesc typeDesc) {
        Object[] convertedValues = null;
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] == null || typeDesc.getFixedProperty(i).isCommonJavaType())
                    continue;

                Object convertedValue = toDocumentIfNeeded(values[i]);
                if (convertedValue != values[i]) {
                    if (convertedValues == null)
                        convertedValues = values.clone();
                    convertedValues[i] = convertedValue;
                }
            }
        }

        return convertedValues != null ? convertedValues : values;
    }

    private Map<String, Object> convertNonPrimitiveDynamicPropertiesToStrings(Map<String, Object> values) {
        Map<String, Object> convertedValues = null;
        if (values != null && !values.isEmpty()) {
            for (java.util.Map.Entry<String, Object> dynamicProperty : values.entrySet()) {
                Object value = dynamicProperty.getValue();
                if (value == null || ReflectionUtils.isCommonJavaType(value.getClass()))
                    continue;

                Object convertedValue = toSpaceStringProperty(value);
                if (convertedValue != value) {
                    if (convertedValues == null)
                        convertedValues = new DocumentProperties(values);
                    convertedValues.put(dynamicProperty.getKey(), convertedValue);
                }
            }
        }
        return convertedValues != null ? convertedValues : values;
    }

    public Map<String, Object> convertNonPrimitiveDynamicPropertiesToDocuments(Map<String, Object> values) {
        Map<String, Object> convertedValues = null;
        if (values != null && !values.isEmpty()) {
            for (java.util.Map.Entry<String, Object> dynamicProperty : values.entrySet()) {
                Object value = dynamicProperty.getValue();
                if (value == null || ReflectionUtils.isCommonJavaType(value.getClass()))
                    continue;

                Object convertedValue = toDocumentIfNeeded(value);
                if (convertedValue != value) {
                    if (convertedValues == null)
                        convertedValues = new DocumentProperties(values);
                    convertedValues.put(dynamicProperty.getKey(), convertedValue);
                }
            }
        }
        return convertedValues != null ? convertedValues : values;
    }

    private Object toDocumentIfNeeded(Object value) {
        Class<? extends Object> type = value.getClass();
        if (type.isEnum())
            return new SpaceDocument(type.getName()).setProperty("value", value.toString());
        if (MarshObject.class.isAssignableFrom(type))
            return value;
        return toDocumentIfNeeded(value, SpaceDocumentSupport.DEFAULT);
    }

    private static SpaceStringProperty toSpaceStringProperty(Object propertyValue) {
        Class<?> propertyClass = propertyValue.getClass();

        String valueStr = null;
        if (propertyClass.isArray()) {
            if (propertyClass == byte[].class)
                valueStr = Arrays.toString((byte[]) propertyValue);
            else if (propertyClass == short[].class)
                valueStr = Arrays.toString((short[]) propertyValue);
            else if (propertyClass == int[].class)
                valueStr = Arrays.toString((int[]) propertyValue);
            else if (propertyClass == long[].class)
                valueStr = Arrays.toString((long[]) propertyValue);
            else if (propertyClass == char[].class)
                valueStr = Arrays.toString((char[]) propertyValue);
            else if (propertyClass == float[].class)
                valueStr = Arrays.toString((float[]) propertyValue);
            else if (propertyClass == double[].class)
                valueStr = Arrays.toString((double[]) propertyValue);
            else if (propertyClass == boolean[].class)
                valueStr = Arrays.toString((boolean[]) propertyValue);
            else { // element is an array of object references
                valueStr = Arrays.deepToString((Object[]) propertyValue);
            }
        } else {
            //object or collection
            valueStr = propertyValue.toString();
        }

        return new SpaceStringProperty(ClassUtils.getTypeDisplayName(propertyClass.getName()), valueStr);
    }
}
