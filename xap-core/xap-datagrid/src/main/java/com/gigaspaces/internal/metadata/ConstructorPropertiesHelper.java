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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.metadata.SpaceMetadataValidationException;

import java.lang.reflect.Constructor;

/**
 * @author Dan Kilman
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class ConstructorPropertiesHelper {
    /**
     * @see {@link com.gigaspaces.internal.metadata.ConstructorPropertyNameExtractor#getParameterNames(Constructor)
     */
    public static String[] extractParameterNames(Constructor<?> constructor) {
        return ReflectionUtil.getConstructorParametersNames(constructor);
    }

    public static boolean possiblyGenericConstructorParameterName(String paramName) {
        if (paramName == null)
            return true;

        // at the minimum it will be arg and 1 digit
        if (paramName.length() < 4)
            return false;

        return paramName.startsWith("arg") && Character.isDigit(paramName.charAt(4));
    }

    public static ConstructorInstantiationDescriptor buildConstructorInstantiationDescriptor(
            Constructor<?> constructor,
            String[] parameterNames,
            Class<?>[] parameterTypes,
            SpaceTypeInfo typeInfo) {
        ConstructorInstantiationDescriptor result = new ConstructorInstantiationDescriptor();
        result.setConstructor(constructor);

        int[] propertyToConstructorIndex = new int[typeInfo.getNumOfSpaceProperties()];
        for (int i = 0; i < propertyToConstructorIndex.length; i++)
            propertyToConstructorIndex[i] = -1;
        boolean[] excludedConstructorIndexes = new boolean[parameterNames.length];

        for (int constructorIndex = 0; constructorIndex < parameterNames.length; constructorIndex++) {
            String parameterName = parameterNames[constructorIndex];
            SpacePropertyInfo property = typeInfo.getProperty(parameterName);
            int indexOfProperty = typeInfo.indexOf(property);
            if (indexOfProperty >= 0) {
                propertyToConstructorIndex[indexOfProperty] = constructorIndex;
                if (property == typeInfo.getIdProperty())
                    result.setIdPropertyConstructorIndex(constructorIndex);
            } else if (property != null) {
                boolean excluded = true;
                if (property == typeInfo.getVersionProperty()) {
                    result.setVersionConstructorIndex(constructorIndex);
                    excluded = false;
                }
                if (property == typeInfo.getPersistProperty()) {
                    result.setPersistConstructorIndex(constructorIndex);
                    excluded = false;
                }
                if (property == typeInfo.getLeaseExpirationProperty()) {
                    result.setLeaseConstructorIndex(constructorIndex);
                    excluded = false;
                }
                if (property == typeInfo.getDynamicPropertiesProperty()) {
                    result.setDynamicPropertiesConstructorIndex(constructorIndex);
                    excluded = false;
                }
                excludedConstructorIndexes[constructorIndex] = excluded;
            } else {
                throw new SpaceMetadataValidationException(typeInfo.getType(), "Missing getter for constructor based property: " + parameterName +
                        ". If this is not a space property, it needs to be explicitly excluded");
            }
        }

        result.setConstructorParameterTypes(parameterTypes);
        result.setConstructorParameterNames(parameterNames);
        result.setSpacePropertyToConstructorIndex(propertyToConstructorIndex);
        result.setExcludedIndexes(excludedConstructorIndexes);

        return result;
    }

}
