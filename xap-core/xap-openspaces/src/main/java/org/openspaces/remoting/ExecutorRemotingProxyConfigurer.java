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

import org.openspaces.core.GigaSpace;

/**
 * A simple programmatic configurer creating a remote executor based proxy
 *
 * <p>Usage example:
 * <pre>
 * IJSpace space = new UrlSpaceConfigurer("jini://&#42;/&#42;/mySpace")
 *                        .space();
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(space).gigaSpace();
 * MyBusinessInterface proxy = new ExecutorRemotingProxyConfigurer&lt;MyBusinessInterface&gt;(gigaSpace,
 * MyBusinessInterface.class)
 *                                         .broadcast(true)
 *                                         .proxy();
 * proxy.businessMethod(...);
 * </pre>
 *
 * @author kimchy
 */
public class ExecutorRemotingProxyConfigurer<T> {

    private ExecutorSpaceRemotingProxyFactoryBean executorFactoryBean;

    public ExecutorRemotingProxyConfigurer(GigaSpace gigaSpace, Class<T> serviceInterface) {
        executorFactoryBean = new ExecutorSpaceRemotingProxyFactoryBean();
        executorFactoryBean.setGigaSpace(gigaSpace);
        executorFactoryBean.setServiceInterface(serviceInterface);
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setTimeout(long)
     */
    public ExecutorRemotingProxyConfigurer<T> timeout(long timeout) {
        executorFactoryBean.setTimeout(timeout);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setBroadcast(boolean)
     */
    public ExecutorRemotingProxyConfigurer<T> broadcast(boolean broadcast) {
        executorFactoryBean.setBroadcast(broadcast);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setBroadcast(boolean)
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(org.openspaces.remoting.RemoteResultReducer)
     */
    public <X, Y> ExecutorRemotingProxyConfigurer<T> broadcast(RemoteResultReducer<X, Y> remoteResultReducer) {
        executorFactoryBean.setRemoteResultReducer(remoteResultReducer);
        executorFactoryBean.setBroadcast(true);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setRemoteResultReducer(org.openspaces.remoting.RemoteResultReducer)
     */
    public ExecutorRemotingProxyConfigurer<T> remoteResultReducer(RemoteResultReducer remoteResultReducer) {
        executorFactoryBean.setRemoteResultReducer(remoteResultReducer);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setReturnFirstResult(boolean)
     */
    public ExecutorRemotingProxyConfigurer<T> returnFirstResult(boolean returnFirstResult) {
        executorFactoryBean.setReturnFirstResult(returnFirstResult);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(MetaArgumentsHandler)
     */
    public ExecutorRemotingProxyConfigurer<T> metaArgumentsHandler(MetaArgumentsHandler metaArgumentsHandler) {
        executorFactoryBean.setMetaArgumentsHandler(metaArgumentsHandler);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#remoteInvocationAspect
     */
    public ExecutorRemotingProxyConfigurer<T> remoteInvocationAspect(RemoteInvocationAspect remoteInvocationAspect) {
        executorFactoryBean.setRemoteInvocationAspect(remoteInvocationAspect);
        return this;
    }

    /**
     * @see org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(RemoteRoutingHandler)
     */
    public ExecutorRemotingProxyConfigurer<T> remoteRoutingHandler(RemoteRoutingHandler remoteRoutingHandler) {
        executorFactoryBean.setRemoteRoutingHandler(remoteRoutingHandler);
        return this;
    }

    /**
     * Creates a new executor proxy of type T
     */
    public T proxy() {
        executorFactoryBean.afterPropertiesSet();
        return (T) executorFactoryBean.getObject();
    }
}