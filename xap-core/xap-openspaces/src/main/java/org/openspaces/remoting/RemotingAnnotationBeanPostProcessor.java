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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.util.AnnotationUtils;
import org.openspaces.remoting.scripting.EventDrivenScriptingExecutor;
import org.openspaces.remoting.scripting.ExecutorScriptingExecutor;
import org.openspaces.remoting.scripting.LazyLoadingRemoteInvocationAspect;
import org.openspaces.remoting.scripting.ScriptingExecutor;
import org.openspaces.remoting.scripting.ScriptingMetaArgumentsHandler;
import org.openspaces.remoting.scripting.ScriptingRemoteRoutingHandler;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessorAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.openspaces.remoting.RemotingUtils.createByClassOrFindByName;

/**
 * @author kimchy
 */
public class RemotingAnnotationBeanPostProcessor extends InstantiationAwareBeanPostProcessorAdapter implements ApplicationContextAware, Ordered {

    private static final Log logger = LogFactory.getLog(RemotingAnnotationBeanPostProcessor.class);

    private ApplicationContext applicationContext;

    private Map<String, GigaSpace> gsByName;

    private GigaSpace uniqueGs;

    private int order = Ordered.LOWEST_PRECEDENCE;

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean == null) {
            return bean;
        }
        Class beanClass = AopUtils.getTargetClass(bean);
        if (beanClass == null) {
            return bean;
        }
        RemotingService remotingService = AnnotationUtils.findAnnotation(beanClass, RemotingService.class);
        if (remotingService != null) {
            SpaceRemotingServiceExporter exporter;
            if (StringUtils.hasLength(remotingService.exporter())) {
                exporter = (SpaceRemotingServiceExporter) applicationContext.getBean(remotingService.exporter());
                if (exporter == null) {
                    throw new IllegalArgumentException("Failed to find exporter under name [" + remotingService.exporter() + "] for bean [" + beanName + "]");
                }
            } else {
                Map exporters = applicationContext.getBeansOfType(SpaceRemotingServiceExporter.class);
                if (exporters.isEmpty()) {
                    throw new IllegalArgumentException("No service exporters are defined within the context, can't register remote service bean [" + beanName + "]");
                }
                if (exporters.size() > 1) {
                    throw new IllegalStateException("More than one service exporter are defined within the context, please specify the exact service exported to register with");
                }
                exporter = (SpaceRemotingServiceExporter) exporters.values().iterator().next();
            }
            exporter.addService(beanName, bean);
        }
        return bean;
    }

    @Override
    public boolean postProcessAfterInstantiation(final Object bean, String beanName) throws BeansException {
        Class beanClass = AopUtils.getTargetClass(bean);
        if (beanClass == null) {
            return true;
        }
        ReflectionUtils.doWithFields(beanClass, new ReflectionUtils.FieldCallback() {
            public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                ExecutorScriptingExecutor executorScriptingExecutor = field.getAnnotation(ExecutorScriptingExecutor.class);
                if (executorScriptingExecutor != null) {
                    ExecutorSpaceRemotingProxyFactoryBean factoryBean = new ExecutorSpaceRemotingProxyFactoryBean();
                    factoryBean.setGigaSpace(findGigaSpaceByName(executorScriptingExecutor.gigaSpace()));
                    factoryBean.setTimeout(executorScriptingExecutor.timeout());
                    factoryBean.setMetaArgumentsHandler(new ScriptingMetaArgumentsHandler());
                    factoryBean.setRemoteInvocationAspect(new LazyLoadingRemoteInvocationAspect());
                    factoryBean.setRemoteRoutingHandler(new ScriptingRemoteRoutingHandler());
                    factoryBean.setServiceInterface(ScriptingExecutor.class);
                    factoryBean.afterPropertiesSet();
                    field.setAccessible(true);
                    field.set(bean, factoryBean.getObject());
                }
                EventDrivenScriptingExecutor eventDrivenScriptingExecutor = field.getAnnotation(EventDrivenScriptingExecutor.class);
                if (eventDrivenScriptingExecutor != null) {
                    EventDrivenSpaceRemotingProxyFactoryBean factoryBean = new EventDrivenSpaceRemotingProxyFactoryBean();
                    factoryBean.setTimeout(eventDrivenScriptingExecutor.timeout());
                    factoryBean.setFifo(eventDrivenScriptingExecutor.fifo());
                    factoryBean.setGigaSpace(findGigaSpaceByName(eventDrivenScriptingExecutor.gigaSpace()));
                    factoryBean.setMetaArgumentsHandler(new ScriptingMetaArgumentsHandler());
                    factoryBean.setRemoteInvocationAspect(new LazyLoadingRemoteInvocationAspect());
                    factoryBean.setRemoteRoutingHandler(new ScriptingRemoteRoutingHandler());
                    factoryBean.setServiceInterface(ScriptingExecutor.class);
                    factoryBean.afterPropertiesSet();
                    field.setAccessible(true);
                    field.set(bean, factoryBean.getObject());
                }
                EventDrivenProxy eventDrivenProxy = field.getAnnotation(EventDrivenProxy.class);
                if (eventDrivenProxy != null) {
                    EventDrivenSpaceRemotingProxyFactoryBean factoryBean = new EventDrivenSpaceRemotingProxyFactoryBean();
                    factoryBean.setTimeout(eventDrivenProxy.timeout());
                    factoryBean.setFifo(eventDrivenProxy.fifo());
                    factoryBean.setGigaSpace(findGigaSpaceByName(eventDrivenProxy.gigaSpace()));
                    factoryBean.setAsyncMethodPrefix(eventDrivenProxy.asyncMethodPrefix());
                    factoryBean.setMetaArgumentsHandler((MetaArgumentsHandler) createByClassOrFindByName(applicationContext, eventDrivenProxy.metaArgumentsHandler(), eventDrivenProxy.metaArgumentsHandlerType()));
                    factoryBean.setRemoteInvocationAspect((RemoteInvocationAspect) createByClassOrFindByName(applicationContext, eventDrivenProxy.remoteInvocationAspect(), eventDrivenProxy.remoteInvocationAspectType()));
                    factoryBean.setRemoteRoutingHandler((RemoteRoutingHandler) createByClassOrFindByName(applicationContext, eventDrivenProxy.remoteRoutingHandler(), eventDrivenProxy.remoteRoutingHandlerType()));
                    factoryBean.setServiceInterface(field.getType());
                    factoryBean.afterPropertiesSet();
                    field.setAccessible(true);
                    field.set(bean, factoryBean.getObject());
                }
                ExecutorProxy executorProxy = field.getAnnotation(ExecutorProxy.class);
                if (executorProxy != null) {
                    ExecutorSpaceRemotingProxyFactoryBean factoryBean = new ExecutorSpaceRemotingProxyFactoryBean();
                    factoryBean.setGigaSpace(findGigaSpaceByName(executorProxy.gigaSpace()));
                    factoryBean.setTimeout(executorProxy.timeout());
                    factoryBean.setBroadcast(executorProxy.broadcast());
                    factoryBean.setMetaArgumentsHandler((MetaArgumentsHandler) createByClassOrFindByName(applicationContext, executorProxy.metaArgumentsHandler(), executorProxy.metaArgumentsHandlerType()));
                    factoryBean.setRemoteInvocationAspect((RemoteInvocationAspect) createByClassOrFindByName(applicationContext, executorProxy.remoteInvocationAspect(), executorProxy.remoteInvocationAspectType()));
                    factoryBean.setRemoteRoutingHandler((RemoteRoutingHandler) createByClassOrFindByName(applicationContext, executorProxy.remoteRoutingHandler(), executorProxy.remoteRoutingHandlerType()));
                    factoryBean.setRemoteResultReducer((RemoteResultReducer) createByClassOrFindByName(applicationContext, executorProxy.remoteResultReducer(), executorProxy.remoteResultReducerType()));
                    factoryBean.setReturnFirstResult(executorProxy.returnFirstResult());
                    factoryBean.setServiceInterface(field.getType());
                    factoryBean.afterPropertiesSet();
                    field.setAccessible(true);
                    field.set(bean, factoryBean.getObject());
                }
            }
        });

        return true;
    }

    protected GigaSpace findGigaSpaceByName(String gsName) throws NoSuchBeanDefinitionException {
        initMapsIfNecessary();
        if (gsName == null || "".equals(gsName)) {
            if (this.uniqueGs != null) {
                return this.uniqueGs;
            } else {
                throw new NoSuchBeanDefinitionException("No GigaSpaces name given and factory contains several");
            }
        }
        GigaSpace namedGs = this.gsByName.get(gsName);
        if (namedGs == null) {
            throw new NoSuchBeanDefinitionException("No GigaSpaces found for name [" + gsName + "]");
        }
        return namedGs;
    }

    /**
     * Lazily initialize gs map.
     */
    private synchronized void initMapsIfNecessary() {
        if (this.gsByName == null) {
            this.gsByName = new HashMap<String, GigaSpace>();
            // Look for named GigaSpaces

            for (String gsName : BeanFactoryUtils.beanNamesForTypeIncludingAncestors(this.applicationContext, GigaSpace.class)) {

                GigaSpace gs = (GigaSpace) this.applicationContext.getBean(gsName);
                gsByName.put(gsName, gs);
            }

            if (this.gsByName.isEmpty()) {
                // Try to find a unique GigaSpaces.
                String[] gsNames = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(this.applicationContext, GigaSpace.class);
                if (gsNames.length == 1) {
                    this.uniqueGs = (GigaSpace) this.applicationContext.getBean(gsNames[0]);
                }
            } else if (this.gsByName.size() == 1) {
                this.uniqueGs = this.gsByName.values().iterator().next();
            }

            if (this.gsByName.isEmpty() && this.uniqueGs == null) {
                logger.warn("No named gs instances defined and not exactly one anonymous one: cannot inject");
            }
        }
    }
}
