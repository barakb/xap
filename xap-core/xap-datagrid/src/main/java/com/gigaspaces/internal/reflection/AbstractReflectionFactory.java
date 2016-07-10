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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public abstract class AbstractReflectionFactory implements IReflectionFactory {
    public IMethod[] getMethods(ClassLoader classLoader, Method[] methods) {
        final int length = methods.length;
        IMethod[] result = new IMethod[length];

        for (int i = 0; i < length; ++i)
            result[i] = getMethod(classLoader, methods[i]);

        return result;
    }

    public <T, F> IField<T, F>[] getFields(Class<T> clazz) {
        final Field[] fields = clazz.getFields();
        final int length = fields.length;
        final IField[] result = new IField[length];

        for (int i = 0; i < length; ++i)
            result[i] = getField(fields[i]);

        return result;
    }

    public <T> IMethod<T> getMethod(Method method) {
        return getMethod(null, method);
    }

    public IMethod[] getMethods(Method[] methods) {
        return getMethods(null, methods);
    }

    public <T> ISetterMethod<T> getSetterMethod(Method method) {
        return getSetterMethod(null, method);
    }

    public <T> IGetterMethod<T> getGetterMethod(Method method) {
        return getGetterMethod(null, method);
    }
}
