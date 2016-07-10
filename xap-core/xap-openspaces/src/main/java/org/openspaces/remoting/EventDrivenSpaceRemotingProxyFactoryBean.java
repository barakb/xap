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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.openspaces.core.GigaSpace;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.remoting.support.RemoteAccessor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Future;

import static org.openspaces.remoting.RemotingUtils.createByClassOrFindByName;

/**
 * A space <b>event driven</b> remoting proxy that forward the service execution to a remote service
 * with the space as the transport layer. Services are remotely exported in the "server side" using
 * the {@link SpaceRemotingServiceExporter} which in turn is registered with an event container.
 * This proxy builds a representation of the remote invocation using {@link SpaceRemotingEntry} and
 * waits for a remoting response represented by {@link SpaceRemotingEntry}.
 *
 * <p>The proxy requires a {@link #setGigaSpace(org.openspaces.core.GigaSpace)} interface to be set
 * in order to write the remote invocation and wait for a response using the space API. It also
 * requires a {@link #setServiceInterface(Class)} which represents the interface that will be
 * proxied.
 *
 * <p>Allows for one way invocations (i.e. not waiting for a response). The one way invocation can
 * be set globally for all of the service methods by setting {@link #setGlobalOneWay(boolean)} or
 * can be enabled only for methods that return <code>void</code> by setting {@link
 * #setVoidOneWay(boolean)}. Note, if using one way invocation and an exception is raised by the
 * remote service, it won't be raised by this proxy.
 *
 * <p>A timeout which controls how long the proxy will wait for the response can be set using {@link
 * #setTimeout(long)}. The timeout value if in <b>milliseconds</b>.
 *
 * <p>The space remote proxy supports a future based invocation. This means that if, on the client
 * side, one of the service interface methods returns {@link java.util.concurrent.Future}, it can be
 * used for async execution. Note, this means that in terms of interfaces there will have to be two
 * different service interfaces (under the same package and with the same name). One for the server
 * side service that returns the actual value, and one on the client side that for the same method
 * simply returns the future. Another option is not having two different interfaces, but having the
 * same interface with async methods (returning <code>Future</code>). The async methods should start
 * with a specified prefix (defaults to <code>async</code>) and should have no implementation on the
 * server side (simply return <code>null</code>).
 *
 * <p>In case of remote invocation over a partitioned space the default partitioned routing index
 * will be random (the hashCode of the newly created {@link SpaceRemotingEntry} class). The proxy
 * allows for a pluggable routing handler implementation by setting {@link
 * #setRemoteRoutingHandler(RemoteRoutingHandler)}.
 *
 * <p>The actual remote invocation can be replaced with an aspect implementing {@link
 * org.openspaces.remoting.RemoteInvocationAspect} which can be set using {@link
 * #setRemoteInvocationAspect(RemoteInvocationAspect)}. It is up the aspect to then call the actual
 * remote invocation.
 *
 * <p>Note that it is also possible to configure method level fifo behavior, one way behavior,
 * {@link RemoteResultReducer}, {@link RemoteRoutingHandler}, {@link RemoteInvocationAspect} and
 * {@link MetaArgumentsHandler} using the {@link EventDrivenRemotingMethod} annotation.
 *
 * @author kimchy
 * @see SpaceRemotingServiceExporter
 */
public class EventDrivenSpaceRemotingProxyFactoryBean extends RemoteAccessor implements FactoryBean, InitializingBean,
        MethodInterceptor, RemotingInvoker, ApplicationContextAware {

    public static final String DEFAULT_ASYNC_METHOD_PREFIX = "async";

    private GigaSpace gigaSpace;

    private long timeout = 60000;

    private final SpaceRemotingEntryFactory remotingEntryFactory = new SpaceRemotingEntryMetadataFactory();

    private RemoteRoutingHandler remoteRoutingHandler;

    private MetaArgumentsHandler metaArgumentsHandler;

    private boolean globalOneWay = false;

    private boolean voidOneWay = false;

    private boolean fifo = false;

    private String asyncMethodPrefix = DEFAULT_ASYNC_METHOD_PREFIX;

    private RemoteInvocationAspect remoteInvocationAspect;

    private Object serviceProxy;

    private Map<Method, RemotingUtils.MethodHash> methodHashLookup;

    private ApplicationContext applicationContext;

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Sets the GigaSpace interface that will be used to work with the space as the transport layer
     * for both writing and taking {@link SpaceRemotingEntry}.
     */
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    /**
     * Sets the timeout that will be used to wait for the remote invocation response. The timeout
     * value is in <b>milliseconds</b> and defaults to <code>60000</code> (60 seconds).
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * In case of remote invocation over a partitioned space the default partitioned routing index
     * will be random (the hashCode of the newly created {@link SpaceRemotingEntry} class). This
     * {@link RemoteRoutingHandler} allows for custom routing computation (for example, based on one
     * of the service method parameters).
     */
    public void setRemoteRoutingHandler(RemoteRoutingHandler remoteRoutingHandler) {
        this.remoteRoutingHandler = remoteRoutingHandler;
    }

    /**
     * Allows to set a meta argument handler that will control {@link SpaceRemotingInvocation#getMetaArguments()}.
     */
    public void setMetaArgumentsHandler(MetaArgumentsHandler metaArgumentsHandler) {
        this.metaArgumentsHandler = metaArgumentsHandler;
    }

    /**
     * If set to <code>true</code> (defaults to <code>false</code>) all of the service methods will
     * be invoked and the proxy will not wait for a return value. Note, any exception raised by the
     * remote service will be logged on the server side and not propagated to the client.
     */
    public void setGlobalOneWay(boolean globalOneWay) {
        this.globalOneWay = globalOneWay;
    }

    /**
     * If set to <code>true</code> (defaults to <code>false</code>) service methods that return void
     * will be invoked and the proxy will not wait for a return value. Note, any exception raised by
     * the remote service will be logged on the server side and not propagated to the client.
     */
    public void setVoidOneWay(boolean voidOneWay) {
        this.voidOneWay = voidOneWay;
    }

    /**
     * Sets the remote invocation objects written to the Space to work in FIFO. Note, when setting
     * this setting make sure to set it on the {@link org.openspaces.remoting.SpaceRemotingServiceExporter}.
     * This allows for remoting to work in fifo mode without needing to set the whole Space to work
     * in fifo mode.
     */
    public void setFifo(boolean fifo) {
        this.fifo = fifo;
    }

    /**
     * Sets the async method prefix. Defaults to {@link #DEFAULT_ASYNC_METHOD_PREFIX}.
     */
    public void setAsyncMethodPrefix(String asyncMethodPrefix) {
        this.asyncMethodPrefix = asyncMethodPrefix;
    }

    /**
     * The actual remote invocation can be replaced with an aspect implementing {@link
     * org.openspaces.remoting.RemoteInvocationAspect} which can be set using {@link
     * #setRemoteInvocationAspect(RemoteInvocationAspect)}. It is up the aspect to then call the
     * actual remote invocation.
     */
    public void setRemoteInvocationAspect(RemoteInvocationAspect remoteInvocationAspect) {
        this.remoteInvocationAspect = remoteInvocationAspect;
    }

    public void afterPropertiesSet() {
        Assert.notNull(getServiceInterface(), "serviceInterface property is required");
        Assert.notNull(gigaSpace, "gigaSpace property is required");
        this.serviceProxy = ProxyFactory.getProxy(getServiceInterface(), this);
        this.methodHashLookup = RemotingUtils.buildMethodToHashLookupForInterface(getServiceInterface(), asyncMethodPrefix);
    }

    public Object getObject() {
        return this.serviceProxy;
    }

    public Class<?> getObjectType() {
        return getServiceInterface();
    }

    public boolean isSingleton() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
        RemoteInvocationAspect localRemoteInvocationAspect = null;
        Annotation[] methodAnnotations = methodInvocation.getMethod().getAnnotations();
        for (Annotation methodAnnotation : methodAnnotations) {
            if (methodAnnotation instanceof EventDrivenRemotingMethod) {
                EventDrivenRemotingMethod remotingMethodAnnotation = (EventDrivenRemotingMethod) methodAnnotation;
                localRemoteInvocationAspect = (RemoteInvocationAspect) createByClassOrFindByName(applicationContext, remotingMethodAnnotation.remoteInvocationAspect(),
                        remotingMethodAnnotation.remoteInvocationAspectType());
            }
        }
        if (localRemoteInvocationAspect == null) {
            localRemoteInvocationAspect = remoteInvocationAspect;
        }

        if (localRemoteInvocationAspect != null) {
            return localRemoteInvocationAspect.invoke(methodInvocation, this);
        }
        return invokeRemote(methodInvocation);
    }

    public Object invokeRemote(MethodInvocation methodInvocation) throws Throwable {
        String lookupName = getServiceInterface().getName();
        String methodName = methodInvocation.getMethod().getName();

        boolean localFifo = false;
        boolean localVoidOneWay = false;
        boolean localGlobalOneWay = false;
        RemoteRoutingHandler localRoutingHandler = null;
        MetaArgumentsHandler localMetaArgumentsHandler = null;
        long localTimeout = -1;

        Annotation[] methodAnnotations = methodInvocation.getMethod().getAnnotations();
        boolean hasMethodLevelRemotingAnnotation = false;
        for (Annotation methodAnnotation : methodAnnotations) {
            if (methodAnnotation instanceof EventDrivenRemotingMethod) {
                hasMethodLevelRemotingAnnotation = true;
                EventDrivenRemotingMethod remotingMethodAnnotation = (EventDrivenRemotingMethod) methodAnnotation;
                localFifo = remotingMethodAnnotation.fifo();
                localGlobalOneWay = remotingMethodAnnotation.globalOneWay();
                localVoidOneWay = remotingMethodAnnotation.voidOneWay();
                localMetaArgumentsHandler = (MetaArgumentsHandler) createByClassOrFindByName(applicationContext, remotingMethodAnnotation.metaArgumentsHandler(),
                        remotingMethodAnnotation.metaArgumentsHandlerType());
                localTimeout = remotingMethodAnnotation.timeout();
                localRoutingHandler = (RemoteRoutingHandler) createByClassOrFindByName(applicationContext, remotingMethodAnnotation.remoteRoutingHandler(),
                        remotingMethodAnnotation.remoteRoutingHandlerType());
            }
        }

        if (!hasMethodLevelRemotingAnnotation) {
            localFifo = fifo;
            localVoidOneWay = voidOneWay;
            localGlobalOneWay = globalOneWay;
            localRoutingHandler = remoteRoutingHandler;
            localMetaArgumentsHandler = metaArgumentsHandler;
            localTimeout = timeout;
        }


        boolean asyncExecution = false;
        if (Future.class.isAssignableFrom(methodInvocation.getMethod().getReturnType())) {
            asyncExecution = true;
            if (methodName.startsWith(asyncMethodPrefix)) {
                methodName = StringUtils.uncapitalize(methodName.substring(asyncMethodPrefix.length()));
            }
        }

        SpaceRemotingEntry remotingEntry = remotingEntryFactory.createHashEntry().buildInvocation(lookupName, methodName,
                methodHashLookup.get(methodInvocation.getMethod()), methodInvocation.getArguments());

        remotingEntry.setRouting(RemotingProxyUtils.computeRouting(remotingEntry, localRoutingHandler, methodInvocation));

        if (localMetaArgumentsHandler != null) {
            remotingEntry.setMetaArguments(localMetaArgumentsHandler.obtainMetaArguments(remotingEntry));
        }

        // check if this invocation will be a one way invocation
        if (localGlobalOneWay) {
            remotingEntry.setOneWay(Boolean.TRUE);
        } else {
            if (localVoidOneWay && methodInvocation.getMethod().getReturnType() == void.class) {
                remotingEntry.setOneWay(Boolean.TRUE);
            }
        }
        remotingEntry.setFifo(localFifo);

        gigaSpace.write(remotingEntry);

        // if this is a one way invocation, simply return null
        if (remotingEntry.getOneWay() != null && remotingEntry.getOneWay()) {
            return null;
        }

        // if the return value is a future, return the future
        if (asyncExecution) {
            return new EventDrivenRemoteFuture(gigaSpace, remotingEntry);
        }

        SpaceRemotingEntry invokeResult = gigaSpace.take(remotingEntry.buildResultTemplate(), localTimeout);
        if (invokeResult == null) {
            throw new RemoteTimeoutException("Timeout waiting for result for [" + lookupName +
                    "] and method [" + methodName + "]", localTimeout);
        }
        if (invokeResult.getException() != null) {
            throw invokeResult.getException();
        }
        return invokeResult.getResult();
    }
}