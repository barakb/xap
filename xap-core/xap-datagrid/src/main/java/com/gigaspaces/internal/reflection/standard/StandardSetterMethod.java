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

package com.gigaspaces.internal.reflection.standard;

import com.gigaspaces.internal.reflection.ISetterMethod;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Provides a wrapper over the standard setter method reflection
 *
 * @author GuyK
 * @since 6.5.1
 */
@com.gigaspaces.api.InternalApi
public class StandardSetterMethod<T> extends StandardMethod<T> implements
        ISetterMethod<T> {

    public StandardSetterMethod(Method method) {
        super(method);
    }

    public void set(T obj, Object arg) throws IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        super.invoke(obj, arg);
    }
}
