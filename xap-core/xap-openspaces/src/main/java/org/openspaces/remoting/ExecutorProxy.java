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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation used to inject {@link ExecutorSpaceRemotingProxyFactoryBean} into a field.
 *
 * @author kimchy
 * @see ExecutorSpaceRemotingProxyFactoryBean
 * @see RemotingAnnotationBeanPostProcessor
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExecutorProxy {

    /**
     * The name of the {@link org.openspaces.core.GigaSpace} instance (representing the Space) that
     * this remote invocation will occur on.
     *
     * <p>If there is only one instance of {@link org.openspaces.core.GigaSpace}, will defualt to
     * it. If not, will throw an exception if not defined.
     */
    public abstract String gigaSpace() default "";

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setTimeout(long)
     */
    long timeout() default 60000;

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setBroadcast(boolean)
     */
    public abstract boolean broadcast() default false;

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    public abstract String remoteRoutingHandler() default "";

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    public abstract Class remoteRoutingHandlerType() default Object.class;

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)
     */
    public abstract String metaArgumentsHandler() default "";

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)
     */
    public abstract Class metaArgumentsHandlerType() default Object.class;

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setRemoteInvocationAspect(org.openspaces.remoting.RemoteInvocationAspect)
     */
    public abstract String remoteInvocationAspect() default "";

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setRemoteInvocationAspect(org.openspaces.remoting.RemoteInvocationAspect)
     */
    public abstract Class remoteInvocationAspectType() default Object.class;

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(org.openspaces.remoting.RemoteResultReducer)
     */
    public abstract String remoteResultReducer() default "";

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(org.openspaces.remoting.RemoteResultReducer)
     */
    public abstract Class remoteResultReducerType() default Object.class;

    /**
     * @see ExecutorSpaceRemotingProxyFactoryBean#setReturnFirstResult(boolean)
     */
    public abstract boolean returnFirstResult() default true;
}