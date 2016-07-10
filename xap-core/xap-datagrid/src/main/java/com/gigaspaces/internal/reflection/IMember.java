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

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;


/**
 * Provides an abstraction over a member reflection.
 *
 * @param <T> Type of the class that contains this method.
 * @author Guy
 * @since 6.6
 */
public interface IMember<T> extends AnnotatedElement {
    final public static String INTERNAL_NAME = ReflectionUtil.getInternalName(IMember.class);

    /**
     * @return underline reflection based {@link Member}
     */
    Member getMember();

    /**
     * Returns the Class object representing the class or interface that declares the member or
     * constructor represented by this Member.
     *
     * @return an object representing the declaring class of the underlying member
     */
    Class<?> getDeclaringClass();

    /**
     * Returns the simple name of the underlying member or constructor represented by this Member.
     *
     * @return the simple name of the underlying member
     */
    String getName();

    /**
     * Returns the Java language modifiers for the member or constructor represented by this Member,
     * as an integer.  The Modifier class should be used to decode the modifiers in the integer.
     *
     * @return the Java language modifiers for the underlying member
     * @see Modifier
     */
    int getModifiers();

    /**
     * Get the value of the <tt>accessible</tt> flag for this object.
     *
     * @return the value of the object's <tt>accessible</tt> flag
     */
    boolean isAccessible();

    /**
     * Set the <tt>accessible</tt> flag for this object to the indicated boolean value.  A value of
     * <tt>true</tt> indicates that the reflected object should suppress Java language access
     * checking when it is used.  A value of <tt>false</tt> indicates that the reflected object
     * should enforce Java language access checks.
     *
     * <p>First, if there is a security manager, its <code>checkPermission</code> method is called
     * with a <code>ReflectPermission("suppressAccessChecks")</code> permission.
     *
     * <p>A <code>SecurityException</code> is raised if <code>flag</code> is <code>true</code> but
     * accessibility of this object may not be changed (for example, if this element object is a
     * {@link Constructor} object for the class {@link java.lang.Class}).
     *
     * <p>A <code>SecurityException</code> is raised if this object is a {@link
     * java.lang.reflect.Constructor} object for the class <code>java.lang.Class</code>, and
     * <code>flag</code> is true.
     *
     * @param flag the new value for the <tt>accessible</tt> flag
     * @throws SecurityException if the request is denied.
     * @see SecurityManager#checkPermission
     * @see java.lang.RuntimePermission
     */
    void setAccessible(boolean flag) throws SecurityException;

}
