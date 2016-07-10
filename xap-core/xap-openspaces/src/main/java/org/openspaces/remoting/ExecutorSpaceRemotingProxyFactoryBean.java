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

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.internal.DefaultAsyncResult;

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

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.openspaces.remoting.RemotingUtils.buildMethodToHashLookupForInterface;
import static org.openspaces.remoting.RemotingUtils.createByClassOrFindByName;

/**
 * A space <b>executor</b> remoting proxy that forward the service execution to a remote service
 * with the space as the transport layer. Services are remotely exported in the "server side" using
 * the {@link org.openspaces.remoting.SpaceRemotingServiceExporter}. The actual invocations uses the
 * space executors mechanism to perform the actual method invocation, allowing to perform async
 * invocations (invocations that return {@link java.util.concurrent.Future} (which is better than
 * the sync remoting option). In general, the executor based remoting should be used over the sync
 * remoting option. <p/> <p>The proxy requires a {@link #setGigaSpace(org.openspaces.core.GigaSpace)}
 * interface to be set in order to write execute. <p/> <p>A timeout which controls how long the
 * proxy will wait for the response can be set using {@link #setTimeout(long)}. The timeout value if
 * in <b>milliseconds</b>. <p/> <p>The space remote proxy supports a future based invocation. This
 * means that if, on the client side, one of the service interface methods returns {@link
 * java.util.concurrent.Future}, it can be used for async execution. Note, this means that in terms
 * of interfaces there will have to be two different service interfaces (under the same package and
 * with the same name). One for the server side service that returns the actual value, and one on
 * the client side that for the same method simply returns the future. Another option is not having
 * two different interfaces, but having the same interface with async methods (returning
 * <code>Future</code>). The async methods should start with a specified prefix (defaults to
 * <code>async</code>) and should have no implementation on the server side (simply return
 * <code>null</code>). <p/> <p>In case of remote invocation over a partitioned space the default
 * partitioned routing index will be random (the hashCode of the newly created {@link
 * org.openspaces.remoting.ExecutorRemotingTask} class). The proxy allows for a pluggable routing
 * handler implementation by setting {@link #setRemoteRoutingHandler(org.openspaces.remoting.RemoteRoutingHandler)}.
 * <p/> <p>The proxy allows to perform broadcast the remote invocation to all different cluster
 * members (partitions for example) by setting the {@link #setBroadcast(boolean) broadcast} flag to
 * <code>true</code>. In such cases, a custom {@link #setRemoteResultReducer(org.openspaces.remoting.RemoteResultReducer)}
 * can be plugged to reduce the results of all different services into a single response (assuming
 * that the service has a return value). <p/> <p>The actual remote invocation can be replaced with
 * an aspect implementing {@link RemoteInvocationAspect} which can be set using {@link
 * #setRemoteInvocationAspect(org.openspaces.remoting.RemoteInvocationAspect)}. It is up the aspect
 * to then call the actual remote invocation.</p> <p>Note that it is also possible to configure
 * method level broadcasting, {@link RemoteResultReducer}, {@link RemoteRoutingHandler}, {@link
 * RemoteInvocationAspect} and {@link MetaArgumentsHandler} using the {@link ExecutorRemotingMethod}
 * annotation.
 *
 * @author kimchy
 * @see SpaceRemotingServiceExporter
 */
public class ExecutorSpaceRemotingProxyFactoryBean extends RemoteAccessor implements FactoryBean, InitializingBean,
        MethodInterceptor, RemotingInvoker, ApplicationContextAware {

    public static final String DEFAULT_ASYNC_METHOD_PREFIX = "async";


    private GigaSpace gigaSpace;

    private long timeout = 60000;

    private RemoteRoutingHandler remoteRoutingHandler;

    private MetaArgumentsHandler metaArgumentsHandler;

    private final String asyncMethodPrefix = DEFAULT_ASYNC_METHOD_PREFIX;

    private boolean broadcast = false;

    private boolean returnFirstResult = true;

    private RemoteResultReducer remoteResultReducer;

    private RemoteInvocationAspect remoteInvocationAspect;

    private Object serviceProxy;

    private Map<Method, RemotingUtils.MethodHash> methodHashLookup;

    private ApplicationContext applicationContext;

    /**
     * Sets the GigaSpace interface that will be used to work with the space as the transport
     * layer.
     */
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
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
     * will be random. This {@link org.openspaces.remoting.RemoteRoutingHandler} allows for custom
     * routing computation (for example, based on one of the service method parameters).
     */
    public void setRemoteRoutingHandler(RemoteRoutingHandler remoteRoutingHandler) {
        this.remoteRoutingHandler = remoteRoutingHandler;
    }

    /**
     * If set the <code>true</code> (defaults to <code>false</code>) causes the remote invocation to
     * be called on all active (primary) cluster members.
     */
    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    /**
     * When using broadcast set to <code>true</code>, allows to plug a custom reducer that can
     * reduce the array of result objects into another response object.
     */
    public void setRemoteResultReducer(RemoteResultReducer remoteResultReducer) {
        this.remoteResultReducer = remoteResultReducer;
    }

    /**
     * Allows to set a meta argument handler that will control {@link org.openspaces.remoting.SpaceRemotingInvocation#getMetaArguments()}.
     */
    public void setMetaArgumentsHandler(MetaArgumentsHandler metaArgumentsHandler) {
        this.metaArgumentsHandler = metaArgumentsHandler;
    }

    /**
     * When set to <code>true</code> (defaults to <code>true</code>) will return the first result
     * when using broadcast. If set to <code>false</code>, an array of results will be retuned. <p/>
     * <p>Note, this only applies if no reducer is provided.
     */
    public void setReturnFirstResult(boolean returnFirstResult) {
        this.returnFirstResult = returnFirstResult;
    }

    /**
     * The actual remote invocation can be replaced with an aspect implementing {@link
     * RemoteInvocationAspect} which can be set using {@link #setRemoteInvocationAspect(org.openspaces.remoting.RemoteInvocationAspect)}.
     * It is up the aspect to then call the actual remote invocation.
     */
    public void setRemoteInvocationAspect(RemoteInvocationAspect remoteInvocationAspect) {
        this.remoteInvocationAspect = remoteInvocationAspect;
    }

    public void afterPropertiesSet() {
        Assert.notNull(getServiceInterface(), "serviceInterface property is required");
        Assert.notNull(gigaSpace, "gigaSpace property is required");
        this.serviceProxy = ProxyFactory.getProxy(getServiceInterface(), this);
        this.methodHashLookup = buildMethodToHashLookupForInterface(getServiceInterface(), asyncMethodPrefix);
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

    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
        RemoteInvocationAspect localRemoteInvocationAspect = null;
        Annotation[] methodAnnotations = methodInvocation.getMethod().getAnnotations();
        for (Annotation methodAnnotation : methodAnnotations) {
            if (methodAnnotation instanceof ExecutorRemotingMethod) {
                ExecutorRemotingMethod remotingMethodAnnotation = (ExecutorRemotingMethod) methodAnnotation;
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

        boolean asyncExecution = false;
        if (Future.class.isAssignableFrom(methodInvocation.getMethod().getReturnType())) {
            asyncExecution = true;
            if (methodName.startsWith(asyncMethodPrefix)) {
                methodName = StringUtils.uncapitalize(methodName.substring(asyncMethodPrefix.length()));
            }
        }

        ExecutorRemotingTask task = new ExecutorRemotingTask(lookupName, methodName, methodHashLookup.get(methodInvocation.getMethod()), methodInvocation.getArguments());

        BroadcastIndicator broadcastIndicator = null;
        RemoteResultReducer localRemoteResultReducer = null;
        RemoteRoutingHandler localRoutingHandler = null;
        MetaArgumentsHandler localMetaArgumentsHandler = null;
        Boolean localShouldBroadcast = null;

        //compute broadcast related meta data
        if (methodInvocation.getArguments() != null && methodInvocation.getArguments().length > 0 && methodInvocation.getArguments()[0] instanceof BroadcastIndicator) {
            broadcastIndicator = (BroadcastIndicator) methodInvocation.getArguments()[0];
            if (broadcastIndicator.shouldBroadcast() != null) {
                localShouldBroadcast = broadcastIndicator.shouldBroadcast();
                localRemoteResultReducer = broadcastIndicator.getReducer();
            }
        } else {
            Annotation[] methodAnnotations = methodInvocation.getMethod().getAnnotations();
            for (Annotation methodAnnotation : methodAnnotations) {
                if (methodAnnotation instanceof ExecutorRemotingMethod) {
                    ExecutorRemotingMethod remotingMethodAnnotation = (ExecutorRemotingMethod) methodAnnotation;
                    if (remotingMethodAnnotation.broadcast()) {
                        localShouldBroadcast = true;
                        localRemoteResultReducer = (RemoteResultReducer) createByClassOrFindByName(applicationContext, remotingMethodAnnotation.remoteResultReducer(),
                                remotingMethodAnnotation.remoteResultReducerType());
                    } else {
                        localShouldBroadcast = false;
                        RemoteRoutingHandler methodRoutingHandler = (RemoteRoutingHandler) createByClassOrFindByName(applicationContext, remotingMethodAnnotation.remoteRoutingHandler(),
                                remotingMethodAnnotation.remoteRoutingHandlerType());
                        if (methodRoutingHandler != null) {
                            localRoutingHandler = methodRoutingHandler;
                        }
                    }
                    localMetaArgumentsHandler = (MetaArgumentsHandler) createByClassOrFindByName(applicationContext, remotingMethodAnnotation.metaArgumentsHandler(),
                            remotingMethodAnnotation.metaArgumentsHandlerType());

                }
            }
        }


        if (localRemoteResultReducer == null) {
            localRemoteResultReducer = remoteResultReducer;
        }
        if (localRoutingHandler == null) {
            localRoutingHandler = remoteRoutingHandler;
        }
        if (localMetaArgumentsHandler == null) {
            localMetaArgumentsHandler = metaArgumentsHandler;
        }
        if (localShouldBroadcast == null) {
            localShouldBroadcast = broadcast;
        }

        if (!localShouldBroadcast) {
            task.setRouting(RemotingProxyUtils.computeRouting(task, localRoutingHandler, methodInvocation));
        }

        if (localMetaArgumentsHandler != null) {
            task.setMetaArguments(localMetaArgumentsHandler.obtainMetaArguments(task));
        }

        if (localShouldBroadcast) {
            DistributedExecutorAsyncFuture future = new DistributedExecutorAsyncFuture(gigaSpace.execute(task), localRemoteResultReducer, task);
            if (asyncExecution)
                return future;
            try {
                return future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            } catch (TimeoutException e) {
                throw new RemoteTimeoutException("Timeout waiting for result for [" + lookupName +
                        "] and method [" + methodName + "]", timeout);
            }
        }

        ExecutorAsyncFuture future = new ExecutorAsyncFuture(gigaSpace.execute(task, task.getRouting()), task);
        if (asyncExecution)
            return future;

        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        } catch (TimeoutException e) {
            throw new RemoteTimeoutException("Timeout waiting for result for [" + lookupName +
                    "] and method [" + methodName + "]", timeout);
        }
    }

    private class DistributedExecutorAsyncFuture implements AsyncFuture {

        private final AsyncFuture<List<AsyncResult<ExecutorRemotingTask.InternalExecutorResult>>> future;

        private final RemoteResultReducer remoteResultReducer;

        private final ExecutorRemotingTask task;

        public DistributedExecutorAsyncFuture(AsyncFuture<List<AsyncResult<ExecutorRemotingTask.InternalExecutorResult>>> future, RemoteResultReducer remoteResultReducer, ExecutorRemotingTask task) {
            this.future = future;
            this.remoteResultReducer = remoteResultReducer;
            this.task = task;
        }

        public void setListener(AsyncFutureListener listener) {
            future.setListener(new ExecutorAsyncFutureListener(listener));
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        public boolean isCancelled() {
            return future.isCancelled();
        }

        public boolean isDone() {
            return future.isDone();
        }

        public Object get() throws InterruptedException, ExecutionException {
            try {
                return get(-1, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // should not happen
                throw new ExecutionException("Timeout exception", e);
            }
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            Object retVal;
            try {
                List<AsyncResult<ExecutorRemotingTask.InternalExecutorResult>> results;
                if (timeout == -1) {
                    results = future.get();
                } else {
                    results = future.get(timeout, unit);
                }
                if (remoteResultReducer != null) {
                    SpaceRemotingResult[] ret = new SpaceRemotingResult[results.size()];
                    int i = 0;
                    for (AsyncResult<ExecutorRemotingTask.InternalExecutorResult> result : results) {
                        ret[i++] = new ExecutorSpaceRemotingResult(result, null);
                    }
                    retVal = remoteResultReducer.reduce(ret, task);
                } else if (returnFirstResult) {
                    AsyncResult<ExecutorRemotingTask.InternalExecutorResult> result = results.iterator().next();
                    if (result.getException() != null) {
                        throw result.getException();
                    }
                    return result.getResult();
                } else {
                    Object[] retVals = new Object[results.size()];
                    int i = 0;
                    for (AsyncResult<ExecutorRemotingTask.InternalExecutorResult> result : results) {
                        if (result.getException() != null) {
                            throw result.getException();
                        }
                        retVals[i++] = result.getResult().getResult();
                    }
                    retVal = retVals;
                }
                return retVal;
            } catch (InterruptedException e) {
                throw e;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ExecutorRemotingTask.InternalExecutorException) {
                    throw new ExecutionException("Failed to invoke service [" + task.getLookupName() + "] with method [" + task.getMethodName() + "]", ((ExecutorRemotingTask.InternalExecutorException) e.getCause()).getException());
                }
                throw e;
            } catch (Throwable e) {
                Throwable actualException = e;
                if (e instanceof ExecutorRemotingTask.InternalExecutorException) {
                    actualException = ((ExecutorRemotingTask.InternalExecutorException) e).getException();
                }
                throw new ExecutionException("Failed to invoke service [" + task.getLookupName() + "] with method [" + task.getMethodName() + "]", actualException);
            }
        }
    }

    private static class ExecutorAsyncFuture implements AsyncFuture {

        private final AsyncFuture<ExecutorRemotingTask.InternalExecutorResult> future;

        private final ExecutorRemotingTask task;

        private ExecutorAsyncFuture(AsyncFuture<ExecutorRemotingTask.InternalExecutorResult> future, ExecutorRemotingTask task) {
            this.future = future;
            this.task = task;
        }

        public void setListener(AsyncFutureListener listener) {
            future.setListener(new ExecutorAsyncFutureListener(listener));
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        public boolean isCancelled() {
            return future.isCancelled();
        }

        public boolean isDone() {
            return future.isDone();
        }

        public Object get() throws InterruptedException, ExecutionException {
            try {
                ExecutorRemotingTask.InternalExecutorResult result = future.get();
                return result.getResult();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ExecutorRemotingTask.InternalExecutorException) {
                    throw new ExecutionException("Failed to invoke service [" + task.getLookupName() + "] with method [" + task.getMethodName() + "]", ((ExecutorRemotingTask.InternalExecutorException) e.getCause()).getException());
                }
                throw e;
            }
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            try {
                ExecutorRemotingTask.InternalExecutorResult result = future.get(timeout, unit);
                return result.getResult();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ExecutorRemotingTask.InternalExecutorException) {
                    throw new ExecutionException("Failed to invoke service [" + task.getLookupName() + "] with method [" + task.getMethodName() + "]", ((ExecutorRemotingTask.InternalExecutorException) e.getCause()).getException());
                }
                throw e;
            }
        }
    }

    private static class ExecutorAsyncFutureListener implements AsyncFutureListener {

        private final AsyncFutureListener listener;

        private ExecutorAsyncFutureListener(AsyncFutureListener listener) {
            this.listener = listener;
        }

        public void onResult(AsyncResult result) {
            if (result.getException() != null) {
                Exception e = result.getException();
                if (e instanceof ExecutorRemotingTask.InternalExecutorException) {
                    e = (Exception) ((ExecutorRemotingTask.InternalExecutorException) e).getException();
                }
                listener.onResult(new DefaultAsyncResult(null, e));
            } else {
                Object res = ((ExecutorRemotingTask.InternalExecutorResult) result.getResult()).getResult();
                listener.onResult(new DefaultAsyncResult(res, null));
            }
        }
    }

    private static class ExecutorSpaceRemotingResult<T extends Serializable> implements SpaceRemotingResult<T> {

        private final AsyncResult<ExecutorRemotingTask.InternalExecutorResult<T>> asyncResult;

        private final Integer routing;

        public ExecutorSpaceRemotingResult(AsyncResult<ExecutorRemotingTask.InternalExecutorResult<T>> asyncResult, Integer routing) {
            this.asyncResult = asyncResult;
            this.routing = routing;
        }

        public Integer getRouting() {
            return routing;
        }

        public T getResult() {
            if (asyncResult.getException() == null) {
                return asyncResult.getResult().getResult();
            }
            return null;
        }

        public Throwable getException() {
            if (asyncResult.getException() != null) {
                if (asyncResult.getException() instanceof ExecutorRemotingTask.InternalExecutorException) {
                    return ((ExecutorRemotingTask.InternalExecutorException) asyncResult.getException()).getException();
                }
                return asyncResult.getException();
            }
            return null;
        }

        public Integer getInstanceId() {
            if (asyncResult.getException() != null) {
                if (asyncResult.getException() instanceof ExecutorRemotingTask.InternalExecutorException) {
                    return ((ExecutorRemotingTask.InternalExecutorException) asyncResult.getException()).getInstanceId();
                }
                return null;
            }
            return asyncResult.getResult().getInstanceId();
        }
    }
}