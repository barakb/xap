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

package com.gigaspaces.persistency.metadata;

import com.gigaspaces.internal.metadata.pojo.PojoPropertyInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfoRepository;
import com.gigaspaces.persistency.error.SpaceMongoException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class PojoTypeDescriptor {

    private final Constructor<Object> constructor;
    private final Map<String, Method> getters = new HashMap<String, Method>();
    private final Map<String, Method> setters = new HashMap<String, Method>();

    @SuppressWarnings("unchecked")
    public PojoTypeDescriptor(Class<?> type) {

        if (type == null)
            throw new IllegalArgumentException("type can not be null");

        PojoTypeInfo typeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);

        try {
            constructor = (Constructor<Object>) type.getConstructor();
        } catch (SecurityException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        } catch (NoSuchMethodException e) {

            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        }

        for (PojoPropertyInfo property : typeInfo.getProperties().values()) {
            if ("class".equals(property.getName())) {
                continue;
            }

            if (property.getGetterMethod() == null
                    || property.getSetterMethod() == null) {
                continue;
            }

            getters.put(property.getName(), property.getGetterMethod());
            setters.put(property.getName(), property.getSetterMethod());
        }

    }

    public Constructor<Object> getConstructor() {
        return constructor;
    }

    public Map<String, Method> getGetters() {
        return getters;
    }

    public Map<String, Method> getSetters() {
        return setters;
    }
}
