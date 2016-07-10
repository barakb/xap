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

package org.openspaces.events.support;

import org.openspaces.archive.ArchiveOperationHandler;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.DynamicEventTemplateProvider;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.EventTemplateProvider;
import org.openspaces.events.adapter.AnnotationDynamicEventTemplateProviderAdapter;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author kimchy
 */
public class AnnotationProcessorUtils {

    private AnnotationProcessorUtils() {

    }

    public static EventContainersBus findBus(ApplicationContext applicationContext) {

        EventContainersBus ecb = null;
        Map<String, EventContainersBus> eventContainerBuses = applicationContext.getBeansOfType(EventContainersBus.class);

        //resolve <os-events:annotation-support>
        ecb = eventContainerBuses.get(org.openspaces.events.config.AnnotationSupportBeanDefinitionParser.PRIMARY_EVENT_CONTAINER_BUS_BEAN_NAME);
        if (ecb == null) {
            //resolve <os-archive:annotation-support>
            ecb = eventContainerBuses.get(org.openspaces.archive.config.AnnotationSupportBeanDefinitionParser.SECONDARY_EVENT_CONTAINER_BUS_BEAN_NAME);
        }
        if (ecb == null) {
            throw new IllegalArgumentException("No event container bus found, can't register polling container. Add <os-events:annotation-support> or <os-archive:annotation-support>");
        }
        return ecb;
    }

    public static GigaSpace findGigaSpace(final Object bean, String gigaSpaceName, ApplicationContext applicationContext, String beanName) throws IllegalArgumentException {
        return findRef(bean, gigaSpaceName, applicationContext, beanName, GigaSpace.class);
    }

    public static ArchiveOperationHandler findArchiveHandler(final Object bean, String archiveHandlerName, ApplicationContext applicationContext, String beanName) throws IllegalArgumentException {
        return findRef(bean, archiveHandlerName, applicationContext, beanName, ArchiveOperationHandler.class);
    }

    public static <T> T findRef(final Object bean, String refName, ApplicationContext applicationContext, String beanName, Class<T> refClass) throws IllegalArgumentException {
        T ref = null;
        if (StringUtils.hasLength(refName)) {
            ref = (T) applicationContext.getBean(refName, refClass);
            if (ref == null) {
                throw new IllegalArgumentException("Failed to lookup " + refClass.getName() + " using name [" + refName + "] in bean [" + beanName + "]");
            }
            return ref;
        }

        Map<String, T> refs = applicationContext.getBeansOfType(refClass);
        if (refs.size() == 1) {
            return (T) refs.values().iterator().next();
        }
        throw new IllegalArgumentException("Failed to resolve " + refClass.getName() + " to use with event container, " +
                "[" + beanName + "] does not specify one, has no fields of that type, and there are more than one " + refClass.getName() + " beans within the context");
    }

    public static PlatformTransactionManager findTxManager(String txManagerName, ApplicationContext applicationContext, String beanName) {
        PlatformTransactionManager txManager;
        if (StringUtils.hasLength(txManagerName)) {
            txManager = (PlatformTransactionManager) applicationContext.getBean(txManagerName);
            if (txManager == null) {
                throw new IllegalArgumentException("Failed to find transaction manager for name [" + txManagerName + "] in bean [" + beanName + "]");
            }
        } else {
            Map txManagers = applicationContext.getBeansOfType(PlatformTransactionManager.class);
            if (txManagers.size() == 1) {
                txManager = (PlatformTransactionManager) txManagers.values().iterator().next();
            } else {
                throw new IllegalArgumentException("More than one transaction manager defined in context, and no transactionManager specified, can't detect one");
            }
        }
        return txManager;
    }


    /**
     * @return a {@link DynamicEventTemplateProvider} based on interface or annotation.
     */
    public static DynamicEventTemplateProvider findDynamicEventTemplateProvider(final Object bean) {

        DynamicEventTemplateProvider provider = null;
        if (bean instanceof DynamicEventTemplateProvider) {
            provider = (DynamicEventTemplateProvider) bean;
        } else {
            AnnotationDynamicEventTemplateProviderAdapter adapter =
                    new AnnotationDynamicEventTemplateProviderAdapter();
            adapter.setDelegate(bean);
            adapter.setFailSilentlyIfMethodNotFound(true);
            adapter.afterPropertiesSet();
            if (adapter.isMethodFound()) {
                provider = adapter;
            }
        }
        return provider;
    }

    public static Object findTemplateFromProvider(Object listener) {
        Object newTemplate = null;
        // check if it implements an interface to provide the template
        if (EventTemplateProvider.class.isAssignableFrom(listener.getClass())) {
            newTemplate = ((EventTemplateProvider) listener).getTemplate();
        } else {
            // check if there is an annotation for it
            final AtomicReference<Method> ref = new AtomicReference<Method>();
            ReflectionUtils.doWithMethods(AopUtils.getTargetClass(listener), new ReflectionUtils.MethodCallback() {
                public void doWith(final Method method) throws IllegalArgumentException, IllegalAccessException {
                    if (method.isAnnotationPresent(EventTemplate.class)) {
                        ref.set(method);
                    }
                }
            });
            if (ref.get() != null) {
                ref.get().setAccessible(true);
                try {
                    newTemplate = ref.get().invoke(listener);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Failed to get template from method [" + ref.get().getName() + "]", e);
                }
            }
        }
        return newTemplate;
    }
}
