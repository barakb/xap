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


package org.openspaces.pu.container.standalone;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.pu.container.CannotCreateContainerException;
import org.openspaces.pu.container.ProcessingUnitContainerConfig;
import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider;
import org.openspaces.pu.container.support.ResourceApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A standalone container runnable allowing to start a Spring based application context based on the
 * provided parameters ({@link org.openspaces.core.properties.BeanLevelProperties}, {@link
 * org.openspaces.core.cluster.ClusterInfo}, and a list of config locations).
 *
 * <p> This runnable allows to start a processing unit container within a running thread, mainly
 * allowing for custom class loader created based on the processing unit structure to be scoped only
 * within the running thread. When using {@link StandaloneProcessingUnitContainer#main(String[])}
 * this feature is not required but when integrating the standalone container within another
 * application this allows not to corrupt the external environment thread context class loader.
 *
 * @author kimchy
 */
public class StandaloneContainerRunnable implements Runnable {

    private final ProcessingUnitContainerConfig config = new ProcessingUnitContainerConfig();

    private List<String> configLocations;

    private volatile boolean running;

    private volatile boolean initialized;

    private ResourceApplicationContext applicationContext;

    private volatile Throwable exception;

    private volatile Thread runningThread;

    /**
     * Constructs a new standalone container runnable based on the provided configuration set
     * parameters.
     *
     * @param beanLevelProperties The properties based configuration for Spring context
     * @param clusterInfo         The cluster info configuration
     * @param configLocations     List of config locations (string based)
     */
    public StandaloneContainerRunnable(BeanLevelProperties beanLevelProperties, ClusterInfo clusterInfo,
                                       List<String> configLocations) {
        this.config.setBeanLevelProperties(beanLevelProperties);
        this.config.setClusterInfo(clusterInfo);
        this.configLocations = configLocations;
    }

    /**
     * Constructs a new Spring {@link org.springframework.context.ApplicationContext} based on the
     * configured list of config locations. Also uses the provided {@link
     * org.openspaces.core.cluster.ClusterInfo} and {@link org.openspaces.core.properties.BeanLevelProperties}
     * in order to further config the application context.
     */
    public void run() {
        try {
            runningThread = Thread.currentThread();
            Resource[] resources;
            PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
            if (configLocations.size() == 0) {
                try {
                    resources = pathMatchingResourcePatternResolver.getResources(ApplicationContextProcessingUnitContainerProvider.DEFAULT_PU_CONTEXT_LOCATION);
                    if (resources.length == 0) {
                        throw new CannotCreateContainerException("Failed to find " + ApplicationContextProcessingUnitContainerProvider.DEFAULT_PU_CONTEXT_LOCATION);
                    }
                } catch (IOException e) {
                    throw new CannotCreateContainerException("Failed to parse pu.xml", e);
                }
            } else {
                List<Resource> tempResourcesList = new ArrayList<Resource>();
                for (String configLocation : configLocations) {
                    try {
                        Resource[] tempResources = pathMatchingResourcePatternResolver.getResources(configLocation);
                        for (Resource tempResource : tempResources) {
                            tempResourcesList.add(tempResource);
                        }
                    } catch (IOException e) {
                        throw new CannotCreateContainerException("Failed to parse pu.xml from location [" + configLocation + "]", e);
                    }
                }
                resources = tempResourcesList.toArray(new Resource[tempResourcesList.size()]);
            }
            // create the Spring application context
            applicationContext = new ResourceApplicationContext(resources, null, config);
            // "start" the application context
            applicationContext.refresh();
            this.running = true;
        } catch (Throwable t) {
            this.exception = t;
            return;
        } finally {
            initialized = true;
        }

        // Just hang in there until we get stopped
        while (isRunning() && !Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // do nothing, the while clause will simply exit
            }
        }
        applicationContext.destroy();
    }

    /**
     * Returns <code>true</code> if the application context initialized successfully.
     */
    public synchronized boolean isInitialized() {
        return initialized;
    }

    /**
     * Return <code>true</code> if this runnable is currently running.
     */
    public synchronized boolean isRunning() {
        return this.running;
    }

    /**
     * Stop this currently running container runnable.
     */
    public synchronized void stop() {
        this.running = false;
        if (runningThread != null)
            runningThread.interrupt();
    }

    /**
     * Returns Spring application context used as the backend for this container.
     */
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public synchronized boolean hasException() {
        return this.exception != null;
    }

    public synchronized Throwable getException() {
        return this.exception;
    }
}
