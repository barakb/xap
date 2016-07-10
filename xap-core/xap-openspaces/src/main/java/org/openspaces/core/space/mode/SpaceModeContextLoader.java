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


package org.openspaces.core.space.mode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.core.properties.BeanLevelPropertiesAware;
import org.openspaces.core.util.SpaceUtils;
import org.openspaces.pu.container.ProcessingUnitContainerConfig;
import org.openspaces.pu.container.support.ResourceApplicationContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.util.MethodInvoker;

/**
 * A Space mode based Spring context loader allows to load Spring application context if the Space
 * is in <code>PRIMARY</code> mode.
 *
 * <p>The space mode context loader allows to assemble beans that only operate when a space is in a
 * <code>PRIMARY</code> mode which basically applies when directly working with cluster members and
 * not a clustered space proxy (since in such cases it will always be <code>PRIMARY</code>).
 *
 * <p>The new Spring application context created will have the current context as its parent,
 * allowing to use any beans defined within the current context within the loaded context.
 *
 * <p>The context loader accepts a Spring {@link org.springframework.core.io.Resource} as the
 * location. A flag called {@link #setActiveWhenPrimary(boolean)} which defaults to
 * <code>true</code> allows to control if the context will be loaded only when the cluster member
 * moves to <code>PRIMARY</code> mode.
 *
 * @author kimchy
 */
public class SpaceModeContextLoader implements ApplicationContextAware, InitializingBean, DisposableBean,
        ApplicationListener, BeanLevelPropertiesAware, ClusterInfoAware, BeanNameAware {

    protected final Log logger = LogFactory.getLog(getClass());

    private Resource location;

    private GigaSpace gigaSpace;

    protected String beanName;

    private boolean activeWhenPrimary = true;

    private ApplicationContext parentApplicationContext;

    private ProcessingUnitContainerConfig config = new ProcessingUnitContainerConfig();

    protected volatile ResourceApplicationContext applicationContext;

    /**
     * The location of the Spring xml context application to be loaded.
     */
    public void setLocation(Resource location) {
        this.location = location;
    }

    /**
     * Allows to set the GigaSpace instance that will control (based on its Space mode - PRIMARY or
     * BACKUP) if the context will be loaded or not. Useful when more than one space is defined
     * within a Spring context.
     */
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    /**
     * Controls if the Spring context will be loaded when the space cluster member moves to
     * <code>PRIMARY</code> mode. Defaults to <code>true</code>.
     */
    public void setActiveWhenPrimary(boolean activeWhenPrimary) {
        this.activeWhenPrimary = activeWhenPrimary;
    }

    /**
     * Used to pass the {@link BeanLevelProperties} to the newly created application context.
     */
    @Override
    public void setBeanLevelProperties(BeanLevelProperties beanLevelProperties) {
        config.setBeanLevelProperties(beanLevelProperties);
    }

    /**
     * Used to pass {@link ClusterInfo} to the newly created application context.
     */
    @Override
    public void setClusterInfo(ClusterInfo clusterInfo) {
        config.setClusterInfo(clusterInfo);
    }

    /**
     * Injected by Spring and used as the parent application context for the newly created
     * application context.
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.parentApplicationContext = applicationContext;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!activeWhenPrimary) {
            loadApplicationContext();
        }
    }

    @Override
    public void destroy() throws Exception {
        closeApplicationContext();
    }

    /**
     * If {@link #setActiveWhenPrimary(boolean)} is set to <code>true</code> (the default) will
     * listens for {@link AfterSpaceModeChangeEvent} and load an application context if received.
     */
    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (activeWhenPrimary) {
            if (applicationEvent instanceof AfterSpaceModeChangeEvent) {
                AfterSpaceModeChangeEvent spEvent = (AfterSpaceModeChangeEvent) applicationEvent;
                if (spEvent.isPrimary()) {
                    if (gigaSpace != null) {
                        if (SpaceUtils.isSameSpace(spEvent.getSpace(), gigaSpace.getSpace())) {
                            try {
                                loadApplicationContext();
                            } catch (Exception e) {
                                logger.error("Failed to load context [" + location + "] when moving to primary mode", e);
                            }
                        }
                    } else {
                        try {
                            loadApplicationContext();
                        } catch (Exception e) {
                            logger.error("Failed to load context [" + location + "] when moving to primary mode", e);
                        }
                    }
                }
                publishEvent(applicationEvent);
            } else if (applicationEvent instanceof BeforeSpaceModeChangeEvent) {
                publishEvent(applicationEvent);
                BeforeSpaceModeChangeEvent spEvent = (BeforeSpaceModeChangeEvent) applicationEvent;
                if (!spEvent.isPrimary()) {
                    if (gigaSpace != null) {
                        if (SpaceUtils.isSameSpace(spEvent.getSpace(), gigaSpace.getSpace())) {
                            closeApplicationContext();
                        }
                    } else {
                        closeApplicationContext();
                    }
                }
            }
        } else {
            if (applicationEvent instanceof AbstractSpaceModeChangeEvent) {
                publishEvent(applicationEvent);
            }
        }
    }

    /**
     * Loads the application context and adding specific bean factory and bean post processors.
     * Won't load an application context if one is already defined.
     */
    protected void loadApplicationContext() throws Exception {
        if (applicationContext != null) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Loading application context [" + location + "]");
        }
        applicationContext = new ResourceApplicationContext(new Resource[]{location}, parentApplicationContext, config);
        try {
            applicationContext.refresh();
        } catch (Exception e) {
            applicationContext = null;
            throw e;
        }
    }

    /**
     * Closes the application context. Won't close that application context if one is not defined.
     */
    protected void closeApplicationContext() {
        if (applicationContext != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Closing application context [" + location + "]");
            }
            try {
                // null the parent, so the close event won't be sent to it as well...
                applicationContext.setParent(null);
                applicationContext.close();
            } finally {
                applicationContext = null;
            }
        }
    }

    /**
     * A hack to only send the application event on the child application context we are loading,
     * without propogating it to the parent application context (when using {@link
     * ApplicationContext#publishEvent(org.springframework.context.ApplicationEvent)} which will
     * create a recursive event calling.
     */
    protected void publishEvent(ApplicationEvent applicationEvent) {
        if (applicationContext == null) {
            return;
        }
        ApplicationEventMulticaster eventMulticaster;
        try {
            MethodInvoker methodInvoker = new MethodInvoker();
            methodInvoker.setTargetClass(AbstractApplicationContext.class);
            methodInvoker.setTargetMethod("getApplicationEventMulticaster");
            methodInvoker.setTargetObject(applicationContext);
            methodInvoker.setArguments(null);
            methodInvoker.prepare();
            eventMulticaster = (ApplicationEventMulticaster) methodInvoker.invoke();
        } catch (Exception e) {
            logger.warn("Failed to get application event multicaster to publish event to child application context", e);
            return;
        }
        eventMulticaster.multicastEvent(applicationEvent);
    }
}
