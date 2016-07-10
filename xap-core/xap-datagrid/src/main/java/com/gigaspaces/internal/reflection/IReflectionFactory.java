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

package com.gigaspaces.internal.reflection;

import com.gigaspaces.internal.metadata.SpaceTypeInfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Interface for reflection enhancement factory.
 *
 * @author Guy
 * @since 7.0
 */
public interface IReflectionFactory {
    <T> IConstructor<T> getConstructor(Constructor<T> ctor);

    <T> IParamsConstructor<T> getParamsConstructor(Constructor<T> ctor);

    <T> String[] getConstructorParametersNames(Constructor<T> ctor);

    <T> IMethod<T> getMethod(Method method);

    <T> IMethod<T> getMethod(ClassLoader classLoader, Method method);

    <T> IMethod<T>[] getMethods(Method[] methods);

    <T> IMethod<T>[] getMethods(ClassLoader classLoader, Method[] methods);

    <T> IGetterMethod<T> getGetterMethod(Method method);

    <T> IGetterMethod<T> getGetterMethod(ClassLoader classLoader, Method method);

    <T> ISetterMethod<T> getSetterMethod(Method method);

    <T> ISetterMethod<T> getSetterMethod(ClassLoader classLoader, Method method);

    <T, F> IField<T, F> getField(Field field);

    <T, F> IField<T, F>[] getFields(Class<T> clazz);

    <T> IProperties<T> getProperties(SpaceTypeInfo typeInfo);

    <T> IProperties<T> getFieldProperties(Class<T> declaringClass, Field[] fields);

    Object getProxy(ClassLoader loader, Class<?>[] interfaces, ProxyInvocationHandler handler, boolean allowCache);
}
