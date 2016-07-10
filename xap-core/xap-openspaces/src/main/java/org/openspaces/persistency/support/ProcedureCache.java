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

package org.openspaces.persistency.support;

import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache for holding fast reflection based getters/setters/constructors. Instance will be created on
 * demand and only once.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class ProcedureCache {

    private final ConcurrentHashMap<Method, IGetterMethod<Object>> getters;
    private final ConcurrentHashMap<Method, ISetterMethod<Object>> setters;
    private final ConcurrentHashMap<Constructor<Object>, IConstructor<Object>> constructors;

    public ProcedureCache() {
        getters = new ConcurrentHashMap<Method, IGetterMethod<Object>>();
        setters = new ConcurrentHashMap<Method, ISetterMethod<Object>>();
        constructors = new ConcurrentHashMap<Constructor<Object>, IConstructor<Object>>();
    }

    /**
     * @param getterMethod The {@link java.lang.reflect.Method} instance that matches a property
     *                     getter.
     * @return A matching {@link com.gigaspaces.internal.reflection.IGetterMethod} for this getter
     * method.
     */
    public IGetterMethod<Object> getterMethodFor(Method getterMethod) {
        try {
            IGetterMethod<Object> objectIGetterMethod = getters.get(getterMethod);
            if (objectIGetterMethod != null)
                return objectIGetterMethod;

            IGetterMethod<Object> possibleGetter = ReflectionUtil.createGetterMethod(getterMethod);
            IGetterMethod<Object> currentGetter = getters.putIfAbsent(getterMethod, possibleGetter);
            return currentGetter == null ? possibleGetter : currentGetter;
        } catch (Exception e) {
            throw new IllegalStateException("Reflection factory failed unexpectedly", e);
        }
    }

    /**
     * @param setterMethod The {@link java.lang.reflect.Method} instance that matches a property
     *                     setter.
     * @return A matching {@link com.gigaspaces.internal.reflection.ISetterMethod} for this setter
     * method.
     */
    public ISetterMethod<Object> setterMethodFor(Method setterMethod) {
        try {
            ISetterMethod<Object> objectISetterMethod = setters.get(setterMethod);
            if (objectISetterMethod != null)
                return objectISetterMethod;

            ISetterMethod<Object> possibleSetter = ReflectionUtil.createSetterMethod(setterMethod);
            ISetterMethod<Object> currentSetter = setters.putIfAbsent(setterMethod, possibleSetter);
            return currentSetter == null ? possibleSetter : currentSetter;
        } catch (Exception e) {
            throw new IllegalStateException("Reflection factory failed unexpectedly", e);
        }
    }

    /**
     * @param constructor The default no-args {@link java.lang.reflect.Constructor} matching a POJO
     *                    type
     * @return A matching {@link com.gigaspaces.internal.reflection.IConstructor} for this
     * contructor.
     */
    public IConstructor<Object> constructorFor(Constructor<Object> constructor) {
        try {
            IConstructor<Object> objectIConstructor = constructors.get(constructor);
            if (objectIConstructor != null)
                return objectIConstructor;

            IConstructor<Object> possibleConst = ReflectionUtil.createCtor(constructor);
            IConstructor<Object> currentConst = constructors.putIfAbsent(constructor, possibleConst);
            return currentConst == null ? possibleConst : currentConst;
        } catch (Exception e) {
            throw new IllegalStateException("Reflection factory failed unexpectedly", e);
        }
    }

}
