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

import java.lang.reflect.Method;


/**
 * Provides an abstraction over a method reflection.
 *
 * @param <T> Type of the class that contains this method.
 * @author Guy
 * @since 6.5
 */
public interface IProcedure<T> extends IMember<T> {

    final public static String INTERNAL_NAME = ReflectionUtil.getInternalName(IProcedure.class);

    /**
     * Returns a <code>Class</code> object that represents the formal return type of the method
     * represented by this <code>IMethod</code> object.
     *
     * @return the return type for the method this object represents
     */
    Class<?> getReturnType();

    /**
     * Returns an array of <code>Class</code> objects that represent the formal parameter types, in
     * declaration order, of the method represented by this <code>Method</code> object.  Returns an
     * array of length 0 if the underlying method takes no parameters.
     *
     * @return the parameter types for the method this object represents
     */
    Class<?>[] getParameterTypes();

    /**
     * Returns an array of <code>Class</code> objects that represent the types of the exceptions
     * declared to be thrown by the underlying method represented by this <code>IMethod</code>
     * object.  Returns an array of length 0 if the method declares no exceptions in its
     * <code>throws</code> clause.
     *
     * @return the exception types declared as being thrown by the method this object represents
     */
    Class[] getExceptionTypes();


    /**
     * Return a reference to the {@link Method} that this {@link IProcedure} represents
     */
    Method getMethod();
}
