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

package org.openspaces.archive;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.util.AnnotationUtils;
import org.openspaces.events.DynamicEventTemplateProvider;
import org.openspaces.events.TransactionalEvent;
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
 * A {@link org.openspaces.archive.Archive} annotation post processor. Creates an internal instance
 * of {@link org.openspaces.archive.ArchivePollingContainer} that wraps the given bean (if
 * annotated) listener.
 *
 * @author Itai Frenkel
 * @since 9.1.1
 */
public class ArchivePollingAnnotationPostProcessor implements BeanPostProcessor, ApplicationContextAware {

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

        Archive archive = AnnotationUtils.findAnnotation(beanClass, Archive.class);
        if (archive == null) {
            return bean;
        }

        GigaSpace gigaSpace = AnnotationProcessorUtils.findGigaSpace(bean, archive.gigaSpace(), applicationContext, beanName);
        ArchiveOperationHandler archiveHandler = AnnotationProcessorUtils.findArchiveHandler(bean, archive.archiveHandler(), applicationContext, beanName);

        EventContainersBus eventContainersBus = AnnotationProcessorUtils.findBus(applicationContext);

        ArchivePollingContainerConfigurer archiveContainerConfigurer =
                new ArchivePollingContainerConfigurer(gigaSpace)
                        .archiveHandler(archiveHandler)
                        .name(beanName)
                        .concurrentConsumers(archive.concurrentConsumers())
                        .maxConcurrentConsumers(archive.maxConcurrentConsumers())
                        .receiveTimeout(archive.receiveTimeout())
                        .performSnapshot(archive.performSnapshot())
                        .recoveryInterval(archive.recoveryInterval())
                        .autoStart(archive.autoStart())
                        .batchSize(archive.batchSize())
                        .useFifoGrouping(archive.useFifoGrouping());

        Object staticTemplateProvider = AnnotationProcessorUtils.findTemplateFromProvider(bean);
        if (staticTemplateProvider != null)
            archiveContainerConfigurer.template(staticTemplateProvider);
        else {
            DynamicEventTemplateProvider templateProvider = AnnotationProcessorUtils.findDynamicEventTemplateProvider(bean);
            if (templateProvider != null) {
                archiveContainerConfigurer.dynamicTemplate(templateProvider);
            }
        }

        // handle transactions (we support using either @Transactional or @TransactionalEvent or both)
        TransactionalEvent transactionalEvent = AnnotationUtils.findAnnotation(beanClass, TransactionalEvent.class);
        Transactional transactional = AnnotationUtils.findAnnotation(beanClass, Transactional.class);
        if (transactionalEvent != null || transactional != null) {
            if (transactionalEvent != null) {
                archiveContainerConfigurer.transactionManager(AnnotationProcessorUtils.findTxManager(transactionalEvent.transactionManager(), applicationContext, beanName));
            } else {
                archiveContainerConfigurer.transactionManager(AnnotationProcessorUtils.findTxManager("", applicationContext, beanName));
            }
            Isolation isolation = Isolation.DEFAULT;
            if (transactional != null && transactional.isolation() != Isolation.DEFAULT) {
                isolation = transactional.isolation();
            }
            if (transactionalEvent != null && transactionalEvent.isolation() != Isolation.DEFAULT) {
                isolation = transactionalEvent.isolation();
            }
            archiveContainerConfigurer.transactionIsolationLevel(isolation.value());

            int timeout = TransactionDefinition.TIMEOUT_DEFAULT;
            if (transactional != null && transactional.timeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                timeout = transactional.timeout();
            }
            if (transactionalEvent != null && transactionalEvent.timeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                timeout = transactionalEvent.timeout();
            }
            archiveContainerConfigurer.transactionTimeout(timeout);
        }

        eventContainersBus.registerContainer(beanName, archiveContainerConfigurer.create());

        return bean;
    }

    private Class<?> getBeanClass(Object bean) {
        return AopUtils.getTargetClass(bean);
    }
}

