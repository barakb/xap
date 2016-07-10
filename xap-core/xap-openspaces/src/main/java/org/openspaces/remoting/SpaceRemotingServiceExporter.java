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

import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.reflection.standard.StandardMethod;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jini.rio.boot.ServiceClassLoader;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.events.EventTemplateProvider;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.remoting.RemoteAccessException;
import org.springframework.remoting.RemoteLookupFailureException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.Assert;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exports a list of services (beans) as remote services with the Space as the transport layer. All
 * the interfaces each service implements are registered as lookup names (matching {@link
 * SpaceRemotingInvocation#getLookupName()} which are then used to lookup the actual service when a
 * remote invocation is received. The correct service and its method are then executed and a {@link
 * org.openspaces.remoting.SpaceRemotingResult} is written back to the space. The remote result can
 * either hold the return value (or <code>null</code> in case of void return value) or an exception
 * that was thrown by the service.
 *
 * <p>The exporter implements {@link org.openspaces.events.SpaceDataEventListener} which means that
 * it acts as a listener to data events and should be used with the different event containers such
 * as {@link org.openspaces.events.polling.SimplePollingEventListenerContainer}. This method of
 * execution is called <b>event driven</b> remote execution ({@link EventDrivenSpaceRemotingProxyFactoryBean}).
 *
 * <p>It also implements {@link org.openspaces.events.EventTemplateProvider} which means that within
 * the event container configuration there is no need to configure the template, as it uses the one
 * provided by this exported.
 *
 * <p>Last, the exporter provides services to executor based remoting ({@link
 * org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean}).
 *
 * <p>By default, the exporter will also autowire and post process all the arguments passed,
 * allowing to inject them with "server" side beans using Spring {@link
 * org.springframework.beans.factory.annotation.Autowired} annotation for example. Note, this
 * variables must be defined as <code>transient</code> so they won't be passed back to the client.
 * This can be disabled by setting {@link #setDisableAutowiredArguments(boolean)} to
 * <code>true</code>.
 *
 * @author kimchy
 * @see org.openspaces.events.polling.SimplePollingEventListenerContainer
 * @see SpaceRemotingEntry
 * @see EventDrivenSpaceRemotingProxyFactoryBean
 */
public class SpaceRemotingServiceExporter implements SpaceDataEventListener<SpaceRemotingEntry>, InitializingBean, ApplicationContextAware, BeanNameAware,
        EventTemplateProvider, ClusterInfoAware, ApplicationListener, ServiceDetailsProvider, ServiceMonitorsProvider {

    public static final String DEFAULT_ASYNC_INTERFACE_SUFFIX = "Async";

    private static final Log logger = LogFactory.getLog(SpaceRemotingServiceExporter.class);

    final private SpaceRemotingEntryFactory remotingEntryFactory = new SpaceRemotingEntryMetadataFactory();

    private List<Object> services = new ArrayList<Object>();

    final private List<ServiceInfo> servicesInfo = new ArrayList<ServiceInfo>();

    private boolean useFastReflection = true;

    final private IdentityHashMap<Object, ServiceInfo> serviceToServiceInfoMap = new IdentityHashMap<Object, ServiceInfo>();

    final private AtomicLong processed = new AtomicLong();

    final private AtomicLong failed = new AtomicLong();

    private ApplicationContext applicationContext;

    private String beanName;

    final private Map<String, Object> interfaceToService = new HashMap<String, Object>();

    private String asyncInterfaceSuffix = DEFAULT_ASYNC_INTERFACE_SUFFIX;

    private boolean fifo = false;

    private boolean disableAutowiredArguments = false;

    private ServiceExecutionAspect serviceExecutionAspect;

    private String templateLookupName;

    private ClusterInfo clusterInfo;

    private Map<String, Map<RemotingUtils.MethodHash, IMethod>> methodInvocationLookup;

    // for backward comp
    final private MethodInvocationCache methodInvocationCache = new MethodInvocationCache();

    private volatile boolean initialized = false;

    final private CountDownLatch initializationLatch = new CountDownLatch(1);

    /**
     * Sets the list of services that will be exported as remote services. Each service will have
     * all of its interfaces registered as lookups (mapping to {@link SpaceRemotingEntry#getLookupName()}
     * which will then be used to invoke the correct service.
     */
    public void setServices(List<Object> services) {
        this.services = services;
    }

    /**
     * For async based execution of remote services, this is one of the options to enable this by
     * using two different interfaces. The first is the actual "server side" interface (sync), and
     * the other has the same interface name just with an "async suffix" to it. The exporter will
     * identify the async suffix, and will perform the invocation on the actual interface.
     *
     * <p>This setter allows to set the async suffix which by default is <code>Async</code>.
     */
    public void setAsyncInterfaceSuffix(String asyncInterfaceSuffix) {
        this.asyncInterfaceSuffix = asyncInterfaceSuffix;
    }

    /**
     * Sets the template used to read async invocation (the {@link SpaceRemotingEntry}) to be fifo.
     * Works in with setting the {@link EventDrivenSpaceRemotingProxyFactoryBean} fifo flag to
     * <code>true</code> and allows for remoting to work in fifo mode without needing to set the
     * whole Space to work in fifo mode.
     */
    public void setFifo(boolean fifo) {
        this.fifo = fifo;
    }

    /**
     * Controls if executing the service should use fast reflection or not.
     */
    public void setUseFastReflection(boolean userFastReflection) {
        this.useFastReflection = userFastReflection;
    }

    /**
     * Allows to disable (by default it is enabled) the autowiring of method arguments with beans
     * that exists within the server side context.
     */
    public void setDisableAutowiredArguments(boolean disableAutowiredArguments) {
        this.disableAutowiredArguments = disableAutowiredArguments;
    }

    /**
     * Allows to inject a service execution callback.
     */
    public void setServiceExecutionAspect(ServiceExecutionAspect serviceExecutionAspect) {
        this.serviceExecutionAspect = serviceExecutionAspect;
    }

    /**
     * Allows to narrow down the async polling container to perform a lookup only on specific lookup
     * name (which is usually the interface that will be used to proxy it on the client side).
     * Defaults to match on all async remoting invocations.
     *
     * <p>This option allows to create several polling container, each for different service that
     * will perform the actual invocation.
     */
    public void setTemplateLookupName(String templateLookupName) {
        this.templateLookupName = templateLookupName;
    }

    /**
     * Application context injected by Spring
     */
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public void setBeanName(String name) {
        this.beanName = name;
    }

    /**
     * Cluster Info injected
     */
    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public void addService(String beanId, Object service) throws IllegalStateException {
        if (initialized) {
            throw new IllegalStateException("Can't add a service once the exporter has initialized");
        }
        this.servicesInfo.add(new ServiceInfo(beanId, service.getClass().getName(), service));
    }

    public void afterPropertiesSet() throws Exception {
        if (beanName == null) {
            beanName = "serviceExporter";
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (applicationEvent instanceof ContextRefreshedEvent) {
            Assert.notNull(services, "services property is required");
            ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
            if (origClassLoader instanceof ServiceClassLoader && origClassLoader.getParent() instanceof ServiceClassLoader) {
                // Since the refreshable context causes the ContextRefreshEvent as well, under its own new class loader
                Thread.currentThread().setContextClassLoader(origClassLoader.getParent());
            }
            try {
                // go over the services and create the interface to service lookup
                int naCounter = 0;
                for (Object service : services) {
                    if (service instanceof ServiceRef) {
                        String ref = ((ServiceRef) service).getRef();
                        service = applicationContext.getBean(ref);
                        this.servicesInfo.add(new ServiceInfo(ref, service.getClass().getName(), service));
                    } else {
                        this.servicesInfo.add(new ServiceInfo("NA" + (++naCounter), service.getClass().getName(), service));
                    }
                }
                methodInvocationLookup = new HashMap<String, Map<RemotingUtils.MethodHash, IMethod>>();
                for (ServiceInfo serviceInfo : servicesInfo) {
                    Set<Class> interfaces = ReflectionUtil.getAllInterfacesForClassAsSet(serviceInfo.getService().getClass());
                    for (Class<?> anInterface : interfaces) {
                        interfaceToService.put(anInterface.getName(), serviceInfo.getService());
                        methodInvocationLookup.put(anInterface.getName(), RemotingUtils.buildHashToMethodLookupForInterface(anInterface, useFastReflection));
                        // for backward comp
                        methodInvocationCache.addService(anInterface, serviceInfo.getService(), useFastReflection);
                    }

                    serviceToServiceInfoMap.put(serviceInfo.getService(), serviceInfo);
                }
                initialized = true;
                initializationLatch.countDown();
            } finally {
                Thread.currentThread().setContextClassLoader(origClassLoader);
            }
        }
    }

    /**
     * The template used for receiving events. Defaults to all objects that are of type {@link
     * SpaceRemotingEntry}.
     */
    public Object getTemplate() {
        SpaceRemotingEntry remotingEntry = remotingEntryFactory.createEntry();
        remotingEntry.setInvocation(Boolean.TRUE);
        remotingEntry.setFifo(fifo);
        remotingEntry.setLookupName(templateLookupName);
        if (logger.isDebugEnabled()) {
            logger.debug("Registering async remoting service template [" + remotingEntry + "]");
        }
        return remotingEntry;
    }

    public ServiceDetails[] getServicesDetails() {
        ArrayList<RemotingServiceDetails.RemoteService> remoteServices = new ArrayList<RemotingServiceDetails.RemoteService>();
        for (ServiceInfo serviceInfo : servicesInfo) {
            remoteServices.add(new RemotingServiceDetails.RemoteService(serviceInfo.getBeanId(), serviceInfo.getClassName()));
        }
        return new ServiceDetails[]{new RemotingServiceDetails(beanName, remoteServices.toArray(new RemotingServiceDetails.RemoteService[remoteServices.size()]))};
    }

    public ServiceMonitors[] getServicesMonitors() {
        ArrayList<RemotingServiceMonitors.RemoteServiceStats> remoteServiceStats = new ArrayList<RemotingServiceMonitors.RemoteServiceStats>();
        for (ServiceInfo serviceInfo : servicesInfo) {
            remoteServiceStats.add(new RemotingServiceMonitors.RemoteServiceStats(serviceInfo.getBeanId(), serviceInfo.getProcessed().get(), serviceInfo.getFailures().get()));
        }
        return new ServiceMonitors[]{new RemotingServiceMonitors(beanName, processed.get(), failed.get(), remoteServiceStats.toArray(new RemotingServiceMonitors.RemoteServiceStats[remoteServiceStats.size()]))};
    }

    /**
     * Receives a {@link SpaceRemotingEntry} which holds all the relevant invocation information.
     * Looks up (based on {@link SpaceRemotingEntry#getLookupName()} the interface the service is
     * registered against (which is the interface the service implements) and then invokes the
     * relevant method within it using the provided method name and arguments. Write the result
     * value or invocation exception back to the space using {@link SpaceRemotingEntry}.
     *
     * @param remotingEntry The remote entry object
     * @param gigaSpace     The GigaSpace interface
     * @param txStatus      A transactional status
     * @param source        An optional source event information
     */
    public void onEvent(SpaceRemotingEntry remotingEntry, GigaSpace gigaSpace, TransactionStatus txStatus, Object source)
            throws RemoteAccessException {

        waitTillInitialized();

        String lookupName = remotingEntry.getLookupName();
        if (lookupName.endsWith(asyncInterfaceSuffix)) {
            lookupName = lookupName.substring(0, lookupName.length() - asyncInterfaceSuffix.length());
        }

        Object service = interfaceToService.get(lookupName);
        if (service == null) {
            // we did not get an interface, maybe it is a bean name?
            try {
                service = applicationContext.getBean(lookupName);
            } catch (NoSuchBeanDefinitionException e) {
                // do nothing, write back a proper exception
            }
            if (service == null) {
                writeResponse(gigaSpace, remotingEntry, new RemoteLookupFailureException(
                        "Failed to find service for lookup [" + remotingEntry.getLookupName() + "]"));
                return;
            }
        }

        autowireArguments(service, remotingEntry.getArguments());

        IMethod method = null;
        try {
            if (remotingEntry instanceof HashedSpaceRemotingEntry && ((HashedSpaceRemotingEntry) remotingEntry).getMethodHash() != null) {
                method = methodInvocationLookup.get(lookupName).get(((HashedSpaceRemotingEntry) remotingEntry).getMethodHash());
            }
            if (method == null) {
                method = methodInvocationCache.findMethod(lookupName, service, remotingEntry.getMethodName(), remotingEntry.getArguments());
            }
        } catch (Exception e) {
            failedExecution(service);
            writeResponse(gigaSpace, remotingEntry, new RemoteLookupFailureException("Failed to find method ["
                    + remotingEntry.getMethodName() + "] for lookup [" + remotingEntry.getLookupName() + "]", e));
            return;
        }
        try {
            Object retVal;
            if (serviceExecutionAspect != null) {
                retVal = serviceExecutionAspect.invoke(remotingEntry, new InternalMethodInvocation(method), service);
            } else {
                retVal = method.invoke(service, remotingEntry.getArguments());
            }
            writeResponse(gigaSpace, remotingEntry, retVal);
            processedExecution(service);
        } catch (InvocationTargetException e) {
            failedExecution(service);
            writeResponse(gigaSpace, remotingEntry, e.getTargetException());
        } catch (IllegalAccessException e) {
            failedExecution(service);
            writeResponse(gigaSpace, remotingEntry, new RemoteLookupFailureException("Failed to access method ["
                    + remotingEntry.getMethodName() + "] for lookup [" + remotingEntry.getLookupName() + "]", e));
        } catch (Throwable e) {
            failedExecution(service);
            writeResponse(gigaSpace, remotingEntry, e);
        }
    }

    private void writeResponse(GigaSpace gigaSpace, SpaceRemotingEntry remotingEntry, Throwable e) {
        if (remotingEntry.getOneWay() == null || !remotingEntry.getOneWay()) {
            SpaceRemotingEntry result = remotingEntry.buildResult(e);
            if (clusterInfo != null) {
                result.setInstanceId(clusterInfo.getInstanceId());
            }
            gigaSpace.write(result);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Remoting execution is configured as one way and an exception was thrown", e);
            }
        }
    }

    private void writeResponse(GigaSpace gigaSpace, SpaceRemotingEntry remotingEntry, Object retVal) {
        if (remotingEntry.getOneWay() == null || !remotingEntry.getOneWay()) {
            SpaceRemotingEntry result = remotingEntry.buildResult(retVal);
            if (clusterInfo != null) {
                result.setInstanceId(clusterInfo.getInstanceId());
            }
            gigaSpace.write(result);
        }
    }

    private void autowireArguments(Object service, Object[] args) {
        if (disableAutowiredArguments) {
            return;
        }
        if (args == null) {
            return;
        }
        if (shouldAutowire(service)) {
            for (Object arg : args) {
                if (arg == null) {
                    continue;
                }
                AutowireCapableBeanFactory beanFactory = applicationContext.getAutowireCapableBeanFactory();
                beanFactory.autowireBeanProperties(arg, AutowireCapableBeanFactory.AUTOWIRE_NO, false);
                beanFactory.initializeBean(arg, arg.getClass().getName());
            }
        }
    }

    private boolean shouldAutowire(Object service) {
        if (service instanceof AutowireArgumentsMarker) {
            return true;
        }
        if (service.getClass().isAnnotationPresent(AutowireArguments.class)) {
            return true;
        }
        for (Class clazz : service.getClass().getInterfaces()) {
            if (clazz.isAnnotationPresent(AutowireArguments.class)) {
                return true;
            }
        }
        return false;
    }

    // Executor execution

    public Object invokeExecutor(ExecutorRemotingTask task) throws Throwable {
        waitTillInitialized();
        String lookupName = task.getLookupName();
        if (lookupName.endsWith(asyncInterfaceSuffix)) {
            lookupName = lookupName.substring(0, lookupName.length() - asyncInterfaceSuffix.length());
        }

        Object service = interfaceToService.get(lookupName);
        if (service == null) {
            // we did not get an interface, maybe it is a bean name?
            try {
                service = applicationContext.getBean(lookupName);
            } catch (NoSuchBeanDefinitionException e) {
                // do nothing, write back a proper exception
                throw new RemoteLookupFailureException("Failed to find service for lookup [" + lookupName + "]", e);
            }
            if (service == null) {
                throw new RemoteLookupFailureException("Failed to find service for lookup [" + lookupName + "]");
            }
        }

        autowireArguments(service, task.getArguments());

        IMethod method = null;
        try {
            if (task.getMethodHash() != null) {
                method = methodInvocationLookup.get(lookupName).get(task.getMethodHash());
            }
            if (method == null) {
                method = methodInvocationCache.findMethod(lookupName, service, task.getMethodName(), task.getArguments());
            }
        } catch (Exception e) {
            failedExecution(service);
            throw new RemoteLookupFailureException("Failed to find method [" + task.getMethodName() + "] for lookup [" + task.getLookupName() + "]", e);
        }
        try {
            Object retVal;
            if (serviceExecutionAspect != null) {
                retVal = serviceExecutionAspect.invoke(task, new InternalMethodInvocation(method), service);
            } else {
                retVal = method.invoke(service, task.getArguments());
            }
            processedExecution(service);
            return retVal;
        } catch (InvocationTargetException e) {
            failedExecution(service);
            throw e.getTargetException();
        } catch (IllegalAccessException e) {
            failedExecution(service);
            throw new RemoteLookupFailureException("Failed to access method [" + task.getMethodName() + "] for lookup [" + task.getLookupName() + "]");
        }
    }

    private void processedExecution(Object service) {
        processed.incrementAndGet();
        serviceToServiceInfoMap.get(service).getProcessed().incrementAndGet();
    }

    private void failedExecution(Object service) {
        failed.incrementAndGet();
        serviceToServiceInfoMap.get(service).getFailures().incrementAndGet();
    }

    private void waitTillInitialized() throws RemoteLookupFailureException {
        if (initialized) {
            return;
        }
        try {
            initializationLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RemoteLookupFailureException("Space remoting service exporter interrupted while waiting for initialization", e);
        }
        if (!initialized) {
            throw new RemoteLookupFailureException("Space remoting service exporter not initialized yet");
        }
    }

    /**
     * Holds a cache of method reflection information per service (interface). If there is a single
     * method within the interface with the same name and number of parameters, then the cached
     * version will be used. If there are more than one method with the name and number of
     * parameteres, then the Java reflection <code>getMethod</code> will be used.
     *
     * <p>Note, as a side effect, if we are using cached methods, we support executing interfaces
     * that declare a super type as a parameter, and invocation will be done using a sub type. This
     * does not work with Java reflection getMethod as it only returns exact match for argument
     * types.
     *
     * <p>Also note, this cache is *not* thread safe. The idea here is that this cache is initlaized
     * at startup and then never updated.
     */
    private static class MethodInvocationCache {

        private final Map<String, MethodsCacheEntry> serviceToMethodCacheMap = new HashMap<String, MethodsCacheEntry>();

        public IMethod findMethod(String lookupName, Object service, String methodName, Object[] arguments) throws NoSuchMethodException {
            int numberOfParameters = 0;
            if (arguments != null) {
                numberOfParameters = arguments.length;
            }
            IMethod invocationMethod;
            IMethod[] methods = serviceToMethodCacheMap.get(lookupName).getMethodCacheEntry(methodName).getMethod(numberOfParameters);
            if (methods != null && methods.length == 1) {
                //we can do caching
                invocationMethod = methods[0];
            } else {
                if (arguments == null) {
                    arguments = new Object[0];
                }
                Class<?>[] argumentTypes = new Class<?>[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    argumentTypes[i] = (arguments[i] != null ? arguments[i].getClass() : Object.class);
                }

                invocationMethod = new StandardMethod(service.getClass().getMethod(methodName, argumentTypes));
            }
            return invocationMethod;
        }

        public void addService(Class serviceInterface, Object service, boolean useFastReflection) {
            MethodsCacheEntry methodsCacheEntry = new MethodsCacheEntry();
            serviceToMethodCacheMap.put(serviceInterface.getName(), methodsCacheEntry);
            methodsCacheEntry.addService(service.getClass(), useFastReflection);
        }

        private static class MethodsCacheEntry {

            private Map<String, MethodCacheEntry> methodNameMap = new HashMap<String, MethodCacheEntry>();

            public MethodCacheEntry getMethodCacheEntry(String methodName) {
                return methodNameMap.get(methodName);
            }

            public void addService(Class service, boolean useFastReflection) {
                Method[] methods = service.getMethods();
                for (Method method : methods) {
                    MethodCacheEntry methodCacheEntry = methodNameMap.get(method.getName());
                    if (methodCacheEntry == null) {
                        methodCacheEntry = new MethodCacheEntry();
                        methodNameMap.put(method.getName(), methodCacheEntry);
                    }
                    methodCacheEntry.addMethod(method, useFastReflection);
                }
            }
        }

        private static class MethodCacheEntry {

            private Map<Integer, IMethod[]> parametersPerMethodMap = new HashMap<Integer, IMethod[]>();

            public IMethod[] getMethod(int numberOfParams) {
                return parametersPerMethodMap.get(numberOfParams);
            }

            public void addMethod(Method method, boolean useFastReflection) {
                IMethod fastMethod;
                if (useFastReflection) {
                    fastMethod = ReflectionUtil.createMethod(method);
                } else {
                    fastMethod = new StandardMethod(method);
                }
                IMethod[] list = parametersPerMethodMap.get(method.getParameterTypes().length);
                if (list == null) {
                    list = new IMethod[]{fastMethod};
                } else {
                    IMethod[] tempList = new IMethod[list.length + 1];
                    System.arraycopy(list, 0, tempList, 0, list.length);
                    tempList[list.length] = fastMethod;
                    list = tempList;
                }
                parametersPerMethodMap.put(method.getParameterTypes().length, list);
            }
        }
    }

    private static class ServiceInfo {
        private final String beanId;
        private final String className;
        private final Object service;
        private final AtomicLong processed = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();

        private ServiceInfo(String beanId, String className, Object service) {
            this.beanId = beanId;
            this.className = className;
            this.service = service;
        }

        public String getBeanId() {
            return beanId;
        }

        public String getClassName() {
            return className;
        }

        public Object getService() {
            return service;
        }

        public AtomicLong getProcessed() {
            return processed;
        }

        public AtomicLong getFailures() {
            return failures;
        }
    }

    private static class InternalMethodInvocation implements ServiceExecutionAspect.MethodInvocation {
        private final IMethod method;

        private InternalMethodInvocation(IMethod method) {
            this.method = method;
        }

        public Object invoke(Object obj, Object... args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            return method.invoke(obj, args);
        }

        @Override
        public Method getMethod() {
            return method.getMethod();
        }
    }
}
