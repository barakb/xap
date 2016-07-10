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

import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.ISetterMethod;

import org.openspaces.persistency.support.ProcedureCache;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * helper class for fast reflection
 *
 * @author Shadi Massalha
 */
public class PojoRepository {

    private static final Map<String, PojoTypeDescriptor> pojoTypeProcedure = new ConcurrentHashMap<String, PojoTypeDescriptor>();

    private static final ProcedureCache procedureCache = new ProcedureCache();

    public PojoTypeDescriptor introcpect(Class<?> type) {
        if (type == null)
            throw new IllegalArgumentException("type can not be null");

        PojoTypeDescriptor descriptor = new PojoTypeDescriptor(type);

        pojoTypeProcedure.put(type.getName(), descriptor);

        return descriptor;
    }

    public PojoTypeDescriptor getProcedureCache(Class<?> key) {
        return pojoTypeProcedure.get(key.getName());
    }

    public IConstructor<Object> getConstructor(Class<?> type) {
        Constructor<Object> constructor = getPojoDescriptor(type)
                .getConstructor();

        return procedureCache.constructorFor(constructor);
    }

    public IGetterMethod<Object> getGetter(Class<?> type, String property) {
        Method getter = getPojoDescriptor(type).getGetters().get(property);

        return procedureCache.getterMethodFor(getter);
    }

    public ISetterMethod<Object> getSetter(Class<?> type, String property) {
        Method setter = getPojoDescriptor(type).getSetters().get(property);

        return procedureCache.setterMethodFor(setter);
    }

    /**
     * @param type
     * @return
     */
    private PojoTypeDescriptor getPojoDescriptor(Class<?> type) {
        PojoTypeDescriptor descriptor = pojoTypeProcedure.get(type.getName());

        if (descriptor == null)
            descriptor = introcpect(type);

        return descriptor;
    }

    public boolean contains(Class<?> type) {
        return pojoTypeProcedure.containsKey(type.getName());
    }

    public Map<String, Method> getGetters(Class<?> type) {
        return getPojoDescriptor(type).getGetters();

    }

    public Map<String, Method> getSetters(Class<?> type) {
        return getPojoDescriptor(type).getSetters();

    }
}
