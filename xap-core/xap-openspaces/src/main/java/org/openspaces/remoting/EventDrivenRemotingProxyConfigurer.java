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
 * A simple programmatic configurer creating a remote asyncronous proxy
 *
 * <p>Usage example:
 * <pre>
 * IJSpace space = new UrlSpaceConfigurer("jini://&#42;/&#42;/mySpace")
 *                        .space();
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(space).gigaSpace();
 * MyBusinessInterface proxy = new EventDrivenRemotingProxyConfigurer&lt;MyBusinessInterface&gt;(gigaSpace,
 * MyBusinessInterface.class)
 *                                  .timeout(15000)
 *                                  .proxy();
 * proxy.businessMethod(...);
 * </pre>
 *
 * @author Uri Cohen
 */
public class EventDrivenRemotingProxyConfigurer<T> {

    private EventDrivenSpaceRemotingProxyFactoryBean eventDrivenFactoryBean;

    public EventDrivenRemotingProxyConfigurer(GigaSpace gigaSpace, Class<T> serviceInterface) {
        eventDrivenFactoryBean = new EventDrivenSpaceRemotingProxyFactoryBean();
        eventDrivenFactoryBean.setGigaSpace(gigaSpace);
        eventDrivenFactoryBean.setServiceInterface(serviceInterface);
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setGlobalOneWay(boolean)
     */
    public EventDrivenRemotingProxyConfigurer<T> globalOneWay(boolean globalOneWay) {
        eventDrivenFactoryBean.setGlobalOneWay(globalOneWay);
        return this;
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setFifo(boolean)
     */
    public EventDrivenRemotingProxyConfigurer<T> fifo(boolean fifo) {
        eventDrivenFactoryBean.setFifo(fifo);
        return this;
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setMetaArgumentsHandler(org.openspaces.remoting.MetaArgumentsHandler)
     */
    public EventDrivenRemotingProxyConfigurer<T> metaArgumentsHandler(MetaArgumentsHandler metaArgumentsHandler) {
        eventDrivenFactoryBean.setMetaArgumentsHandler(metaArgumentsHandler);
        return this;
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#remoteInvocationAspect
     */
    public EventDrivenRemotingProxyConfigurer<T> remoteInvocationAspect(RemoteInvocationAspect remoteInvocationAspect) {
        eventDrivenFactoryBean.setRemoteInvocationAspect(remoteInvocationAspect);
        return this;
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setVoidOneWay(boolean)
     */
    public EventDrivenRemotingProxyConfigurer<T> voidOneWay(boolean voidOneWay) {
        eventDrivenFactoryBean.setVoidOneWay(voidOneWay);
        return this;
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setTimeout(long)
     */
    public EventDrivenRemotingProxyConfigurer<T> timeout(long timeout) {
        eventDrivenFactoryBean.setTimeout(timeout);
        return this;
    }

    /**
     * @see org.openspaces.remoting.EventDrivenSpaceRemotingProxyFactoryBean#setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)
     */
    public EventDrivenRemotingProxyConfigurer<T> remoteRoutingHandler(RemoteRoutingHandler remoteRoutingHandler) {
        eventDrivenFactoryBean.setRemoteRoutingHandler(remoteRoutingHandler);
        return this;
    }

    /**
     * Creates a new event driven proxy of type T
     */
    public T proxy() {
        eventDrivenFactoryBean.afterPropertiesSet();
        return (T) eventDrivenFactoryBean.getObject();
    }
}