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


package org.openspaces.pu.container.jee.context;

import org.openspaces.core.cluster.ClusterInfoBeanPostProcessor;
import org.openspaces.core.cluster.ClusterInfoPropertyPlaceholderConfigurer;
import org.openspaces.core.properties.BeanLevelPropertyBeanPostProcessor;
import org.openspaces.core.properties.BeanLevelPropertyPlaceholderConfigurer;
import org.openspaces.pu.container.ProcessingUnitContainerConfig;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.web.context.support.XmlWebApplicationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends Spring {@link org.springframework.web.context.support.XmlWebApplicationContext} allowing
 * to dynamically add (during construction) a {@link org.springframework.beans.factory.config.BeanPostProcessor}.
 *
 * @author kimchy
 */
public class ProcessingUnitWebApplicationContext extends XmlWebApplicationContext {

    private List<BeanPostProcessor> beanPostProcessors = new ArrayList<BeanPostProcessor>();

    public ProcessingUnitWebApplicationContext(ProcessingUnitContainerConfig config) {
        if (config.getBeanLevelProperties() != null) {
            addBeanFactoryPostProcessor(new BeanLevelPropertyPlaceholderConfigurer(config.getBeanLevelProperties(), config.getClusterInfo()));
            addBeanPostProcessor(new BeanLevelPropertyBeanPostProcessor(config.getBeanLevelProperties()));
        }
        if (config.getClusterInfo() != null) {
            addBeanPostProcessor(new ClusterInfoBeanPostProcessor(config.getClusterInfo()));
            addBeanFactoryPostProcessor(new ClusterInfoPropertyPlaceholderConfigurer(config.getClusterInfo()));
        }
    }

    /**
     * Adds Spring bean post processor. Note, this method should be called before the {@link
     * #refresh()} is called on this application context for the bean post processor to take
     * affect.
     *
     * @param beanPostProcessor The bean post processor to add
     */
    public void addBeanPostProcessor(BeanPostProcessor beanPostProcessor) {
        this.beanPostProcessors.add(beanPostProcessor);
    }

    /**
     * Creates a new bean factory by delegating to the super bean factory creation and then adding
     * all the registered {@link BeanPostProcessor}s.
     */
    protected DefaultListableBeanFactory createBeanFactory() {
        DefaultListableBeanFactory beanFactory = super.createBeanFactory();
        for (BeanPostProcessor beanPostProcessor : beanPostProcessors) {
            beanFactory.addBeanPostProcessor(beanPostProcessor);
        }
        return beanFactory;
    }
}
