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

package com.j_spaces.core.client;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ExternalEntryUtils {
    public static ExternalEntry createExternalEntry(String className, Object[] fieldsValues, ITypeDesc typeDesc) {
        ExternalEntry ee = new ExternalEntry();

        ee.m_ClassName = className;
        ee.m_FieldsValues = fieldsValues;
        updateProperties(ee, typeDesc);

        return ee;
    }

    public static void updateProperties(ExternalEntry ee, ITypeDesc typeDesc) {
        final int length = typeDesc.getNumOfFixedProperties();
        String[] fieldNames = new String[length];
        String[] fieldTypes = new String[length];
        boolean[] indices = new boolean[length];
        boolean[] primitives = new boolean[length];

        for (int i = 0; i < length; i++) {
            PropertyInfo property = typeDesc.getFixedProperty(i);
            fieldNames[i] = property.getName();
            fieldTypes[i] = property.getTypeName();
            indices[i] = typeDesc.getIndexType(property.getName()).isIndexed();
            primitives[i] = property.isSpacePrimitive();
        }

        ee.m_FieldsNames = fieldNames;
        ee.m_FieldsTypes = fieldTypes;
        ee.m_IndexIndicators = indices;
        ee._primitiveFields = primitives;
    }

    public static void updateTypeDescription(ExternalEntry ee, ITypeDesc typeDesc) {
        ee.setClassName(typeDesc.getTypeName());
        updateProperties(ee, typeDesc);
        ee.setPrimaryKeyName(typeDesc.getRoutingPropertyName());
        ee.setReplicatable(typeDesc.isReplicable());
        ee.setSuperClassesNames(typeDesc.getRestrictSuperClassesNames());
    }

}
