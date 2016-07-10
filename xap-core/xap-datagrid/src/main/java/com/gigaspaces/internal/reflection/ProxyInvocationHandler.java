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

import java.lang.reflect.UndeclaredThrowableException;


/**
 * <code>InvocationHandler</code> is the interface implemented by the <i>invocation handler</i> of a
 * proxy instance.
 *
 * <p>Each proxy instance has an associated invocation handler. When a method is invoked on a proxy
 * instance, the method invocation is encoded and dispatched to the <code>invoke</code> method of
 * its invocation handler.
 *
 * @author Guy
 * @since 7.0
 */
public interface ProxyInvocationHandler {

    final public static String INTERNAL_NAME = ReflectionUtil.getInternalName(ProxyInvocationHandler.class);

    /**
     * Processes a method invocation on a proxy instance and returns the result.  This method will
     * be invoked on an invocation handler when a method is invoked on a proxy instance that it is
     * associated with.
     *
     * @param proxy  the proxy instance that the method was invoked on
     * @param method the <code>IMethod</code> instance corresponding to the interface method invoked
     *               on the proxy instance.  The declaring class of the <code>IMethod</code> object
     *               will be the interface that the method was declared in, which may be a
     *               superinterface of the proxy interface that the proxy class inherits the method
     *               through.
     * @param args   an array of objects containing the values of the arguments passed in the method
     *               invocation on the proxy instance, or <code>null</code> if interface method
     *               takes no arguments. Arguments of primitive types are wrapped in instances of
     *               the appropriate primitive wrapper class, such as <code>java.lang.Integer</code>
     *               or <code>java.lang.Boolean</code>.
     * @return the value to return from the method invocation on the proxy instance.  If the
     * declared return type of the interface method is a primitive type, then the value returned by
     * this method must be an instance of the corresponding primitive wrapper class; otherwise, it
     * must be a type assignable to the declared return type.  If the value returned by this method
     * is <code>null</code> and the interface method's return type is primitive, then a
     * <code>NullPointerException</code> will be thrown by the method invocation on the proxy
     * instance.  If the value returned by this method is otherwise not compatible with the
     * interface method's declared return type as described above, a <code>ClassCastException</code>
     * will be thrown by the method invocation on the proxy instance.
     * @throws Throwable the exception to throw from the method invocation on the proxy instance.
     *                   The exception's type must be assignable either to any of the exception
     *                   types declared in the <code>throws</code> clause of the interface method or
     *                   to the unchecked exception types <code>java.lang.RuntimeException</code> or
     *                   <code>java.lang.Error</code>.  If a checked exception is thrown by this
     *                   method that is not assignable to any of the exception types declared in the
     *                   <code>throws</code> clause of the interface method, then an {@link
     *                   UndeclaredThrowableException} containing the exception that was thrown by
     *                   this method will be thrown by the method invocation on the proxy instance.
     * @see UndeclaredThrowableException
     */
    public Object invoke(Object proxy, IMethod method, Object[] args) throws Throwable;

}
