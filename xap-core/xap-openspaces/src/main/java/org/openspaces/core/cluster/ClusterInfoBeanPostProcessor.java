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


package org.openspaces.core.cluster;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * A Spring {@link BeanPostProcessor} that takes a {@link ClusterInfo} and injects it to all the
 * beans that implements {@link ClusterInfoAware} interface and to those that contain a field that
 * has the annotation {@link ClusterInfoContext}}.
 *
 * @author kimchy
 */
public class ClusterInfoBeanPostProcessor implements BeanPostProcessor {

    private ClusterInfo clusterInfo;

    /**
     * Constructs a new cluster info bean post processor based on the provided cluster info.
     */
    public ClusterInfoBeanPostProcessor(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public Object postProcessBeforeInitialization(final Object bean, String beanName) throws BeansException {
        if (bean instanceof ClusterInfoAware) {
            ((ClusterInfoAware) bean).setClusterInfo(clusterInfo);
        }
        if (bean == null) {
            return bean;
        }

        ReflectionUtils.doWithFields(bean.getClass(), new ReflectionUtils.FieldCallback() {
            public void doWith(Field field) {
                if (field.isAnnotationPresent(ClusterInfoContext.class)) {
                    if (!field.isAccessible()) {
                        field.setAccessible(true);
                    }
                    try {
                        field.set(bean, clusterInfo);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Failed to inject ClusterInfo", e);
                    }
                }
            }
        });

        return bean;
    }

    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

}
