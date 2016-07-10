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


package org.openspaces.remoting;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A service execution callback allows to wrap the execution of "server side" service. If actual
 * execution of the service is needed, the <code>invoke</code> method will need to be called on the
 * passed <code>Method</code> using the service as the actual service to invoke it on, and {@link
 * SpaceRemotingInvocation#getArguments()} as the method arguments.
 *
 * <p>As an example: <code>method.invoke(service, invocation.getArguments())</code>.
 *
 * @author kimchy
 */
public interface ServiceExecutionAspect {

    /**
     * A service execution callback allows to wrap the execution of "server side" service. If actual
     * execution of the service is needed, the <code>invoke</code> method will need to be called on
     * the passed <code>Method</code> using the service as the actual service to invoke it on, and
     * {@link SpaceRemotingInvocation#getArguments()} as the method arguments.
     *
     * <p>As an example: <code>method.invoke(service, invocation.getArguments())</code>.
     */
    Object invoke(SpaceRemotingInvocation invocation, MethodInvocation method, Object service) throws InvocationTargetException, IllegalAccessException;

    /**
     * A wrapper for method invocation. Allows to execute the actual method invocation. Similar to
     * reflection based method invocation.
     */
    interface MethodInvocation {
        /**
         * Invokes this execution on the specified service using the supplied arguments.
         *
         * @param service the service to invoke the method on.
         * @param args    the arguments of the invocation.
         * @return invocation result.
         */
        Object invoke(Object service, Object... args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException;

        /**
         * Gets the underlying method of this invocation
         */
        Method getMethod();
    }
}
