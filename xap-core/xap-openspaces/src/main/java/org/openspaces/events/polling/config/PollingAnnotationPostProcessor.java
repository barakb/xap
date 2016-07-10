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


package org.openspaces.events.polling.config;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.util.AnnotationUtils;
import org.openspaces.events.DynamicEventTemplateProvider;
import org.openspaces.events.EventDriven;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.asyncpolling.AsyncPolling;
import org.openspaces.events.notify.Notify;
import org.openspaces.events.polling.Polling;
import org.openspaces.events.polling.SimplePollingContainerConfigurer;
import org.openspaces.events.support.AnnotationProcessorUtils;
import org.openspaces.events.support.EventContainersBus;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * A {@link org.openspaces.events.polling.Polling} annotation post processor. Creates an internal
 * instance of {@link org.openspaces.events.polling.SimplePollingEventListenerContainer} that wraps
 * the given bean (if annotated) listener.
 *
 * @author kimchy
 */
public class PollingAnnotationPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private ApplicationContext applicationContext;

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    public Object postProcessAfterInitialization(final Object bean, String beanName) throws BeansException {
        if (bean == null) {
            return bean;
        }
        Class<?> beanClass = this.getBeanClass(bean);
        if (beanClass == null) {
            return bean;
        }

        Polling polling = AnnotationUtils.findAnnotation(beanClass, Polling.class);
        if (polling == null) {
            EventDriven eventDriven = AnnotationUtils.findAnnotation(beanClass, EventDriven.class);
            if (eventDriven == null) {
                return bean;
            }
            Notify notify = AnnotationUtils.findAnnotation(beanClass, Notify.class);
            if (notify != null) {
                return bean;
            }
            AsyncPolling asyncPolling = AnnotationUtils.findAnnotation(beanClass, AsyncPolling.class);
            if (asyncPolling != null) {
                return bean;
            }
            return bean;
        }

        GigaSpace gigaSpace = AnnotationProcessorUtils.findGigaSpace(bean, polling.gigaSpace(), applicationContext, beanName);

        EventContainersBus eventContainersBus = AnnotationProcessorUtils.findBus(applicationContext);

        SimplePollingContainerConfigurer pollingContainerConfigurer = new SimplePollingContainerConfigurer(gigaSpace);

        pollingContainerConfigurer.name(beanName);

        if (bean instanceof SpaceDataEventListener) {
            pollingContainerConfigurer.eventListener((SpaceDataEventListener) bean);
        } else {
            pollingContainerConfigurer.eventListenerAnnotation(bean);
        }

        DynamicEventTemplateProvider templateProvider = AnnotationProcessorUtils.findDynamicEventTemplateProvider(bean);
        if (templateProvider != null) {
            pollingContainerConfigurer.dynamicTemplate(templateProvider);
        }

        pollingContainerConfigurer.concurrentConsumers(polling.concurrentConsumers());
        pollingContainerConfigurer.maxConcurrentConsumers(polling.maxConcurrentConsumers());
        pollingContainerConfigurer.receiveTimeout(polling.receiveTimeout());
        pollingContainerConfigurer.performSnapshot(polling.performSnapshot());
        pollingContainerConfigurer.passArrayAsIs(polling.passArrayAsIs());
        pollingContainerConfigurer.recoveryInterval(polling.recoveryInterval());
        pollingContainerConfigurer.autoStart(polling.autoStart());

        // handle transactions (we support using either @Transactional or @TransactionalEvent or both)
        TransactionalEvent transactionalEvent = AnnotationUtils.findAnnotation(beanClass, TransactionalEvent.class);
        Transactional transactional = AnnotationUtils.findAnnotation(beanClass, Transactional.class);
        if (transactionalEvent != null || transactional != null) {
            if (transactionalEvent != null) {
                pollingContainerConfigurer.transactionManager(AnnotationProcessorUtils.findTxManager(transactionalEvent.transactionManager(), applicationContext, beanName));
            } else {
                pollingContainerConfigurer.transactionManager(AnnotationProcessorUtils.findTxManager("", applicationContext, beanName));
            }
            Isolation isolation = Isolation.DEFAULT;
            if (transactional != null && transactional.isolation() != Isolation.DEFAULT) {
                isolation = transactional.isolation();
            }
            if (transactionalEvent != null && transactionalEvent.isolation() != Isolation.DEFAULT) {
                isolation = transactionalEvent.isolation();
            }
            pollingContainerConfigurer.transactionIsolationLevel(isolation.value());

            int timeout = TransactionDefinition.TIMEOUT_DEFAULT;
            if (transactional != null && transactional.timeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                timeout = transactional.timeout();
            }
            if (transactionalEvent != null && transactionalEvent.timeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                timeout = transactionalEvent.timeout();
            }
            pollingContainerConfigurer.transactionTimeout(timeout);
        }

        eventContainersBus.registerContainer(beanName, pollingContainerConfigurer.pollingContainer());

        return bean;
    }

    private Class<?> getBeanClass(Object bean) {
        return AopUtils.getTargetClass(bean);
    }
}
