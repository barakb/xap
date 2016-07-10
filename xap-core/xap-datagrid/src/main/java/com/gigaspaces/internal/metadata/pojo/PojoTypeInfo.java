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

package com.gigaspaces.internal.metadata.pojo;

import com.gigaspaces.internal.utils.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Niv Ingberg
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class PojoTypeInfo {
    private static final String PREFIX_GET = "get";
    private static final String PREFIX_SET = "set";
    public static final String PREFIX_IS = "is";

    private final Class<?> _type;
    private final PojoTypeInfo _superTypeInfo;
    private final String _name;
    private final Map<String, PojoPropertyInfo> _properties;

    public PojoTypeInfo(Class<?> type, PojoTypeInfo superTypeInfo) {
        this._type = type;
        this._superTypeInfo = superTypeInfo;
        this._name = type.getName();
        this._properties = initProperties(type, superTypeInfo);
    }

    public String getName() {
        return _name;
    }

    public Class<?> getType() {
        return _type;
    }

    public PojoTypeInfo getSuperTypeInfo() {
        return _superTypeInfo;
    }

    public Map<String, PojoPropertyInfo> getProperties() {
        return _properties;
    }

    public int getNumOfProperties() {
        return _properties.size();
    }

    public PojoPropertyInfo getProperty(String propertyName) {
        return _properties.get(propertyName);
    }


    private static Map<String, PojoPropertyInfo> initProperties(Class<?> type, PojoTypeInfo superTypeInfo) {
        Map<String, PojoPropertyInfo> properties = new HashMap<String, PojoPropertyInfo>();

        Method[] methods = type.getDeclaredMethods();
        for (Method method : methods) {
            // Skip static | synthetic | bridge methods:
            final int modifiers = method.getModifiers();
            if (Modifier.isStatic(modifiers) || method.isSynthetic() || method.isBridge())
                continue;

            // Check if method is a property method, skip if not:
            final String propertyName = getPropertyName(method);
            if (propertyName == null)
                continue;

            // Check if property is already listed:
            PojoPropertyInfo property = properties.get(propertyName);
            if (property == null) {
                // Add new property:
                property = new PojoPropertyInfo(propertyName);
                properties.put(property.getName(), property);
            }
            // Add method to property:
            property.getMethods().add(method);
        }

        // Add methods from super type info:
        if (superTypeInfo != null)
            for (Entry<String, PojoPropertyInfo> entry : superTypeInfo._properties.entrySet()) {
                PojoPropertyInfo superProperty = entry.getValue();
                PojoPropertyInfo property = properties.get(superProperty.getName());
                if (property == null) {
                    property = new PojoPropertyInfo(superProperty.getName());
                    properties.put(property.getName(), property);
                }
                property.getMethods().addAll(superProperty.getMethods());
            }

        // Post-process each property to deduce type and accessors:
        for (Entry<String, PojoPropertyInfo> entry : properties.entrySet())
            entry.getValue().calculateAccessors();

        return properties;
    }

    private static String getPropertyName(Method method) {
        final String methodName = method.getName();
        final int numOfParameters = method.getParameterTypes().length;

        if (numOfParameters == 0) {
            final Class<?> returnType = method.getReturnType();
            if (returnType == void.class)
                return null;
            if (StringUtils.isStrictPrefix(methodName, PREFIX_GET))
                return extractPropertyName(methodName, PREFIX_GET);
            if (StringUtils.isStrictPrefix(methodName, PREFIX_IS) && (returnType == boolean.class || returnType == Boolean.class))
                return extractPropertyName(methodName, PREFIX_IS);
        } else if (numOfParameters == 1) {
            if (StringUtils.isStrictPrefix(methodName, PREFIX_SET))
                return extractPropertyName(methodName, PREFIX_SET);
        }

        return null;
    }

    /**
     * Utility method to take a string and convert it to normal Java variable name capitalization.
     * This normally means converting the first character from upper case to lower case, but in the
     * (unusual) special case when there is more than one character and both the first and second
     * characters are upper case, we leave it alone. <p> Thus "FooBah" becomes "fooBah" and "X"
     * becomes "x", but "URL" stays as "URL".
     */
    private static String extractPropertyName(String methodName, String prefix) {
        final int prefixLength = prefix.length();

        if (methodName.length() > prefixLength + 1 &&
                Character.isUpperCase(methodName.charAt(prefixLength + 1)) &&
                Character.isUpperCase(methodName.charAt(prefixLength)))
            return methodName.substring(prefixLength);

        return Character.toLowerCase(methodName.charAt(prefixLength)) + methodName.substring(prefixLength + 1);
    }
}
