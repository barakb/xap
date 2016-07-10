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

import org.aopalliance.intercept.MethodInvocation;

/**
 * A remoting aspect allows to wrap the remote invocation with specific "user" logic, for example,
 * to add retry in case of a failure, security, or something similar.
 *
 * @author kimchy
 * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean
 * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean
 */
public interface RemoteInvocationAspect<T> {

    /**
     * The aspect is called instead of the actual remote invocation. The methodInvocation can be
     * used to access any method related information. The remoting invoker passed can be used to
     * actually invoke the remote invocation.
     */
    T invoke(MethodInvocation methodInvocation, RemotingInvoker remotingInvoker) throws Throwable;
}
