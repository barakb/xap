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

package org.openspaces.pu.container;

import com.gigaspaces.metrics.BeanMetricManager;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.Metric;
import com.gigaspaces.metrics.ServiceMetric;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Niv Ingberg
 * @since 10.1.1
 */
public class ProcessingUnitContainerContextBeanPostProcessor implements BeanPostProcessor {
    private static final Log logger = LogFactory.getLog(ProcessingUnitContainerContextBeanPostProcessor.class);
    private final ProcessingUnitContainerContext processingUnitContainerContext;

    public ProcessingUnitContainerContextBeanPostProcessor(ProcessingUnitContainerContext processingUnitContainerContext) {
        this.processingUnitContainerContext = processingUnitContainerContext;
    }

    @Override
    public Object postProcessBeforeInitialization(final Object bean, String beanName) throws BeansException {
        processProcessingUnitContainerContextAware(bean);
        processMetrics(bean, beanName);
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    private void processProcessingUnitContainerContextAware(final Object bean) {
        if (bean instanceof ProcessingUnitContainerContextAware)
            ((ProcessingUnitContainerContextAware) bean).setProcessingUnitContainerContext(processingUnitContainerContext);
    }

    private void processMetrics(final Object bean, final String beanName) {
        final AtomicReference<BeanMetricManager> metricManagerHolder = new AtomicReference<BeanMetricManager>();
        ReflectionUtils.doWithMethods(bean.getClass(), new ReflectionUtils.MethodCallback() {
            @Override
            public void doWith(Method method) {
                ServiceMetric annotation = method.getAnnotation(ServiceMetric.class);
                if (annotation != null) {
                    Metric metric = getMetricFromMethod(method, bean);
                    if (metric != null) {
                        if (logger.isDebugEnabled())
                            logger.debug("Registering custom metric " + annotation.name());
                        if (metricManagerHolder.get() == null)
                            metricManagerHolder.set(processingUnitContainerContext.createBeanMetricManager(beanName));
                        metricManagerHolder.get().register(annotation.name(), metric);
                    }
                }
            }
        });
    }

    private static Metric getMetricFromMethod(final Method method, final Object bean) {
        if (method.getParameterTypes().length != 0) {
            if (logger.isWarnEnabled())
                logger.warn("Metric registration of method " + method.getName() + " in " + bean.getClass().getName() +
                        " is skipped - metric method cannot have parameters");
            return null;
        }
        if (method.getReturnType().equals(Void.TYPE)) {
            if (logger.isWarnEnabled())
                logger.warn("Metric registration of method " + method.getName() + " in " + bean.getClass().getName() +
                        " is skipped - metric method cannot return void");
            return null;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            if (logger.isWarnEnabled())
                logger.warn("Metric registration of method " + method.getName() + " in " + bean.getClass().getName() +
                        " is skipped - metric method cannot be static");
            return null;
        }

        if (!method.isAccessible())
            method.setAccessible(true);

        if (Metric.class.isAssignableFrom(method.getReturnType())) {
            try {
                return (Metric) method.invoke(bean);
            } catch (IllegalAccessException e) {
                if (logger.isWarnEnabled())
                    logger.warn("Metric registration of method " + method.getName() + " in " + bean.getClass().getName() +
                            " is skipped - failed to get metric - " + e.getMessage());
                return null;
            } catch (InvocationTargetException e) {
                if (logger.isWarnEnabled())
                    logger.warn("Metric registration of method " + method.getName() + " in " + bean.getClass().getName() +
                            " is skipped - failed to get metric - " + e.getMessage());
                return null;
            }
        }

        return new Gauge<Object>() {
            @Override
            public Object getValue() throws Exception {
                return method.invoke(bean);
            }
        };
    }
}
