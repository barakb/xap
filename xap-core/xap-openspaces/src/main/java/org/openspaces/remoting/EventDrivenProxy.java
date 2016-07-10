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
 * An annotation used to inject {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean}
 * into a field.
 *
 * @author kimchy
 * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean
 * @see RemotingAnnotationBeanPostProcessor
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventDrivenProxy {

    /**
     * The name of the {@link org.openspaces.core.GigaSpace} instance (representing the Space) that
     * this remote invocation will occur on.
     *
     * <p>If there is only one instance of {@link org.openspaces.core.GigaSpace}, will defualt to
     * it. If not, will throw an exception if not defined.
     */
    String gigaSpace() default "";

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setFifo(boolean)
     */
    boolean fifo() default false;

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setTimeout(long)
     */
    long timeout() default 60000;

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setGlobalOneWay(boolean)
     */
    boolean globalOneWay() default false;

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setVoidOneWay(boolean)
     */
    boolean voidOneWay() default false;

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setAsyncMethodPrefix(String)
     */
    String asyncMethodPrefix() default org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean.DEFAULT_ASYNC_METHOD_PREFIX;

    /**
     * The bean name that implements this interface.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    String remoteRoutingHandler() default "";

    /**
     * The class that implements this interface.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    Class remoteRoutingHandlerType() default Object.class;

    /**
     * The bean name that implements this interface.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)
     */
    String metaArgumentsHandler() default "";

    /**
     * The Class that implements this interface.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)
     */
    Class metaArgumentsHandlerType() default Object.class;

    /**
     * The bean name that implements this interface.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    String remoteInvocationAspect() default "";

    /**
     * The class that implements this interface.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    Class remoteInvocationAspectType() default Object.class;
}