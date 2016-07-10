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
 * An annotation used to configure method level behaviour for event driven remoting proxies, which
 * are eventually configured through {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean}.
 * Should be set on the method of the remote interface
 *
 * @author uri
 * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean
 * @see org.openspaces.remoting.RemotingAnnotationBeanPostProcessor
 * @since 8.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventDrivenRemotingMethod {

    /**
     * Specifies the fifo behaviour of this method. When the annotation is not set , will default to
     * {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setFifo(boolean)}.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setFifo(boolean)
     */
    boolean fifo() default false;

    /**
     * Specifies the globalOneWay behaviour of the annotated method. When the annotation is not set
     * , will default to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setGlobalOneWay(boolean)}.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setGlobalOneWay(boolean)
     */
    boolean globalOneWay() default false;

    /**
     * Specifies the voidOneWay behaviour of the annotated method. When the annotation is not set ,
     * will default to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setVoidOneWay(boolean)}.
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setVoidOneWay(boolean)
     */
    boolean voidOneWay() default false;

    /**
     * Sets the timeout that will be used to wait for the response from the remote invocation of the
     * annotated method. When the annotation is not set , will default to {@link
     * org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setTimeout(long)}
     *
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setTimeout(long)
     */
    long timeout() default 60000;

    /**
     * Specifies the name in the spring context of a {@link org.openspaces.remoting.RemoteRoutingHandler}
     * to be used for determining the routing key for the invocation. When the annotation is not set
     * , will default to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)}
     */
    String remoteRoutingHandler() default "";

    /**
     * Specifies the class name of a {@link org.openspaces.remoting.RemoteRoutingHandler} to be used
     * for for determining the routing key for the invocation. When using this attribute, each
     * invocation of the annotated method will result in creating a new instance of the specified
     * class. When the annotation is not set , will default to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)}
     */
    Class remoteRoutingHandlerType() default Object.class;

    /**
     * Specifies the name in the spring context of a {@link org.openspaces.remoting.RemoteInvocationAspect}
     * to be used when invoking the annotated method. When the annotation is not set , will default
     * to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteInvocationAspect(org.openspaces.remoting.RemoteInvocationAspect)}
     */
    String remoteInvocationAspect() default "";

    /**
     * Specifies the class name of a {@link org.openspaces.remoting.RemoteInvocationAspect} to be
     * used to be used when invoking the annotated method. When using this attribute, each
     * invocation of the annotated method will result in creating a new instance of the specified
     * class. When the annotation is not set , will default to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteInvocationAspect(org.openspaces.remoting.RemoteInvocationAspect)}
     */
    Class remoteInvocationAspectType() default Object.class;

    /**
     * Specifies the name in the spring context of a {@link org.openspaces.remoting.MetaArgumentsHandler}
     * to be used when invoking the annotated method. When the annotation is not set , will default
     * to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)}
     */
    String metaArgumentsHandler() default "";

    /**
     * Specifies the class name of a {@link org.openspaces.remoting.MetaArgumentsHandler} to be used
     * to be used when invoking the annotated method. When using this attribute, each invocation of
     * the annotated method will result in creating a new instance of the specified class. When the
     * annotation is not set , will default to {@link org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)}
     */
    Class metaArgumentsHandlerType() default Object.class;


}
