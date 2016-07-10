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
 * An annotation used to configure method level behaviour for executor remoting proxies. which are
 * eventually configured through {@link ExecutorSpaceRemotingProxyFactoryBean}. Should be set on the
 * method of the remote interface
 *
 * @author uri
 * @see ExecutorSpaceRemotingProxyFactoryBean
 * @see RemotingAnnotationBeanPostProcessor
 * @since 8.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExecutorRemotingMethod {
    /**
     * Determines Whether or not to broadcast the specific method call to all cluster members. When
     * set, a remote reducer must also be present. The reducer can be be configured specifically for
     * the method to which the annotation applies (using the #remoteResultReducer or
     * #remoteInvocationAspectType attributes) or at the proxy level by using {@link
     * ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(RemoteResultReducer)} When the
     * annotation is not set, will default to {@link ExecutorSpaceRemotingProxyFactoryBean#setBroadcast(boolean)}
     */
    boolean broadcast();

    /**
     * When broadcast is set to true, specifies the name in the spring context of a {@link
     * RemoteResultReducer} to be used for reducing the result of the invocation. When the
     * annotation is not set, will default to {@link ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(RemoteResultReducer)}
     */
    String remoteResultReducer() default "";

    /**
     * When broadcast is set to true, specifies the class name of a {@link RemoteResultReducer} to
     * be used for reducing the result of the invocation. When using this attribute, each invocation
     * of the annotated method will result in creating a new instance of the specified class. When
     * the annotation is not set, will default to {@link ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(RemoteResultReducer)}
     */
    Class remoteResultReducerType() default Object.class;

    /**
     * When broadcast is set to false, specifies the name in the spring context of a {@link
     * RemoteRoutingHandler} to be used for determining the routing key for the invocation. When the
     * annotation is not set, will default to {@link ExecutorSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(RemoteRoutingHandler)}
     */
    String remoteRoutingHandler() default "";

    /**
     * When broadcast is set to false, specifies the class name of a {@link RemoteRoutingHandler} to
     * be used for for determining the routing key for the invocation. When using this attribute,
     * each invocation of the annotated method will result in creating a new instance of the
     * specified class. When the annotation is not set, will default to {@link
     * ExecutorSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(RemoteRoutingHandler)}
     */
    Class remoteRoutingHandlerType() default Object.class;

    /**
     * Specifies the name in the spring context of a {@link RemoteInvocationAspect} to be used when
     * invoking the annotated method. When the annotation is not set, will default to {@link
     * ExecutorSpaceRemotingProxyFactoryBean#setRemoteInvocationAspect(RemoteInvocationAspect)}
     */
    String remoteInvocationAspect() default "";

    /**
     * Specifies the class name of a {@link RemoteInvocationAspect} to be used to be used when
     * invoking the annotated method. When using this attribute, each invocation of the annotated
     * method will result in creating a new instance of the specified class. When the annotation is
     * not set, will default to {@link ExecutorSpaceRemotingProxyFactoryBean#setRemoteInvocationAspect(RemoteInvocationAspect)}
     */
    Class remoteInvocationAspectType() default Object.class;

    /**
     * Specifies the name in the spring context of a {@link MetaArgumentsHandler} to be used when
     * invoking the annotated method. When the annotation is not set, will default to {@link
     * ExecutorSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(MetaArgumentsHandler)}
     */
    String metaArgumentsHandler() default "";

    /**
     * Specifies the class name of a {@link MetaArgumentsHandler} to be used to be used when
     * invoking the annotated method. When using this attribute, each invocation of the annotated
     * method will result in creating a new instance of the specified class. When the annotation is
     * not set, will default to {@link ExecutorSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(MetaArgumentsHandler)}
     */
    Class metaArgumentsHandlerType() default Object.class;


}
