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


package org.openspaces.pu.container.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.cluster.ClusterInfoBeanPostProcessor;
import org.openspaces.core.cluster.ClusterInfoPropertyPlaceholderConfigurer;
import org.openspaces.core.properties.BeanLevelPropertyBeanPostProcessor;
import org.openspaces.core.properties.BeanLevelPropertyPlaceholderConfigurer;
import org.openspaces.pu.container.ProcessingUnitContainerConfig;
import org.openspaces.pu.container.ProcessingUnitContainerContext;
import org.openspaces.pu.container.ProcessingUnitContainerContextBeanPostProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A Spring {@link org.springframework.context.ApplicationContext} implementation that works with
 * Spring {@link org.springframework.core.io.Resource} for config locations.
 *
 * <p> By default this application does not "start" and requires explicit call to {@link
 * #refresh()}.
 *
 * @author kimchy
 */
public class ResourceApplicationContext extends AbstractXmlApplicationContext {

    private static final Log logger = LogFactory.getLog(ResourceApplicationContext.class);
    private Resource[] resources;
    private final CompoundBeanPostProcessor compoundBeanPostProcessor = new CompoundBeanPostProcessor();

    /**
     * Create this application context with a list of resources for configuration and an optional
     * parent application context (can be <code>null</code>).
     *
     * @param resources List of xml config resources
     * @param parent    An optional parent application context
     */
    public ResourceApplicationContext(Resource[] resources, ApplicationContext parent) {
        super(parent);
        this.resources = resources;
    }

    public ResourceApplicationContext(Resource[] resources, ApplicationContext parent, ProcessingUnitContainerConfig config) {
        this(resources, parent);
        if (config.getBeanLevelProperties() != null) {
            addBeanFactoryPostProcessor(new BeanLevelPropertyPlaceholderConfigurer(config.getBeanLevelProperties(), config.getClusterInfo()));
            addBeanPostProcessor(new BeanLevelPropertyBeanPostProcessor(config.getBeanLevelProperties()));
        }
        if (config.getClusterInfo() != null) {
            addBeanPostProcessor(new ClusterInfoBeanPostProcessor(config.getClusterInfo()));
        }
        addBeanFactoryPostProcessor(new ClusterInfoPropertyPlaceholderConfigurer(config.getClusterInfo()));
        addBeanPostProcessor(new ProcessingUnitContainerContextBeanPostProcessor(new ProcessingUnitContainerContext(config)));
    }

    /**
     * Returns the config resources this application context uses.
     */
    protected Resource[] getConfigResources() {
        return this.resources;
    }

    /**
     * Adds Spring bean post processor. Note, this method should be called before the {@link
     * #refresh()} is called on this application context for the bean post processor to take
     * affect.
     *
     * @param beanPostProcessor The bean post processor to add
     */
    public void addBeanPostProcessor(BeanPostProcessor beanPostProcessor) {
        this.compoundBeanPostProcessor.add(beanPostProcessor);
    }

    /**
     * Creates a new bean factory by delegating to the super bean factory creation and then adding
     * all the registered {@link BeanPostProcessor}s.
     */
    protected DefaultListableBeanFactory createBeanFactory() {
        DefaultListableBeanFactory beanFactory = super.createBeanFactory();
        beanFactory.addBeanPostProcessor(compoundBeanPostProcessor);
        return beanFactory;
    }

    /**
     * Overrides in order to return {@link org.openspaces.pu.container.support.PUPathMatchingResourcePatternResolver}
     * which allows to perform path mathcing over a remote processing unit.
     */
    protected ResourcePatternResolver getResourcePatternResolver() {
        return new PUPathMatchingResourcePatternResolver();
    }

    private static class CompoundBeanPostProcessor implements BeanPostProcessor {

        private final List<BeanPostProcessor> beanPostProcessors = new CopyOnWriteArrayList<BeanPostProcessor>();

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
            if (logger.isDebugEnabled())
                logger.debug("postProcessBeforeInitialization(" + beanName + ")");
            for (BeanPostProcessor beanPostProcessor : beanPostProcessors)
                bean = beanPostProcessor.postProcessBeforeInitialization(bean, beanName);
            return bean;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
            if (logger.isDebugEnabled())
                logger.debug("postProcessAfterInitialization(" + beanName + ")");
            for (BeanPostProcessor beanPostProcessor : beanPostProcessors)
                bean = beanPostProcessor.postProcessAfterInitialization(bean, beanName);
            return bean;
        }

        public void add(BeanPostProcessor beanPostProcessor) {
            this.beanPostProcessors.add(beanPostProcessor);
        }
    }
}
