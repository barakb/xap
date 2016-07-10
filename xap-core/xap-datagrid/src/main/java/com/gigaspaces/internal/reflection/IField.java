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

import java.lang.reflect.Modifier;


/**
 * Provides an abstraction over a field reflection.
 *
 * @param <T> Type of the class that contains this field type
 * @param <F> Field type.
 * @author Guy
 */
public interface IField<T, F> extends IMember<T> {
    final public static String INTERNAL_NAME = ReflectionUtil.getInternalName(IField.class);

    F get(T obj) throws IllegalArgumentException, IllegalAccessException;

    void set(T obj, F value) throws IllegalArgumentException, IllegalAccessException;

    /**
     * Returns the name of the field represented by this <code>Field</code> object.
     */
    String getName();

    /**
     * Returns the <code>Class</code> object representing the class or interface that declares the
     * field represented by this <code>Field</code> object.
     */
    Class<T> getDeclaringClass();

    /**
     * Returns the Java language modifiers for the field represented by this <code>Field</code>
     * object, as an integer. The <code>Modifier</code> class should be used to decode the
     * modifiers.
     *
     * @see Modifier
     */
    public int getModifiers();

    /**
     * Returns a <code>Class</code> object that identifies the declared type for the field
     * represented by this <code>Field</code> object.
     *
     * @return a <code>Class</code> object identifying the declared type of the field represented by
     * this object
     */
    public Class<F> getType();

}
