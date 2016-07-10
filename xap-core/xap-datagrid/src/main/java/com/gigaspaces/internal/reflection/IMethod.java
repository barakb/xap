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

import java.lang.reflect.InvocationTargetException;


/**
 * Provides an abstraction over a method reflection.
 *
 * @param <T> Type of the class that contains this method.
 * @author Guy
 * @since 6.5
 */
public interface IMethod<T> extends IProcedure<T> {
    final public static String DESCRIPTOR_NAME = ReflectionUtil.getDescriptor(IMethod.class);
    final public static String ARRAY_DESCRIPTOR_NAME = ReflectionUtil.getDescriptor(IMethod[].class);

    /**
     * Invokes the underlying method represented by this <code>IMethod</code> object, on the
     * specified object with the specified parameters. Individual parameters are automatically
     * unwrapped to match primitive formal parameters, and both primitive and reference parameters
     * are subject to method invocation conversions as necessary.
     *
     * @param obj  the object the underlying method is invoked from
     * @param args the arguments used for the method call
     * @return the result of dispatching the method represented by this object on <code>obj</code>
     * with parameters <code>args</code>
     * @throws IllegalAccessException      if this <code>Method</code> object enforces Java language
     *                                     access control and the underlying method is
     *                                     inaccessible.
     * @throws IllegalArgumentException    if the method is an instance method and the specified
     *                                     object argument is not an instance of the class or
     *                                     interface declaring the underlying method (or of a
     *                                     subclass or implementor thereof); if the number of actual
     *                                     and formal parameters differ; if an unwrapping conversion
     *                                     for primitive arguments fails; or if, after possible
     *                                     unwrapping, a parameter value cannot be converted to the
     *                                     corresponding formal parameter type by a method
     *                                     invocation conversion.
     * @throws InvocationTargetException   if the underlying method throws an exception.
     * @throws NullPointerException        if the specified object is null and the method is an
     *                                     instance method.
     * @throws ExceptionInInitializerError if the initialization provoked by this method fails.
     */
    Object invoke(T obj, Object... args)
            throws IllegalAccessException, IllegalArgumentException,
            InvocationTargetException;
}
