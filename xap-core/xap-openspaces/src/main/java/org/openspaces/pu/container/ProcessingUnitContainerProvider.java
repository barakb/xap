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

import com.gigaspaces.metrics.MetricRegistrator;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.core.properties.BeanLevelPropertiesAware;

import java.io.File;
import java.net.URL;

/**
 * A processing unit container provider is responsible for creating {@link
 * org.openspaces.pu.container.ProcessingUnitContainer}. Usually concrete implementation will have
 * additional parameters controlling the nature of how specific container will be created.
 *
 * @author kimchy
 */
public abstract class ProcessingUnitContainerProvider implements ClusterInfoAware, BeanLevelPropertiesAware {

    public static final String CONTAINER_CLASS_PROP = "pu.container.class";
    public static final String CONTEXT_PROPERTY_DEPLOY_PATH = "deployPath";
    public static final String SPACE_NAME_PROPERTY_KEY = "dataGridName";

    private final ProcessingUnitContainerConfig config = new ProcessingUnitContainerConfig();

    /**
     * Creates a processing unit container.
     *
     * @return A newly created processing unit container.
     */
    public abstract ProcessingUnitContainer createContainer() throws CannotCreateContainerException;

    /**
     * Sets the path where the processing unit deployment was extracted to.
     */
    public void setDeployPath(File deployPath) {
    }

    public void setManifestUrls(Iterable<URL> libs) {
    }

    public void setClassLoader(ClassLoader classLoader) {
    }

    public ClusterInfo getClusterInfo() {
        return config.getClusterInfo();
    }

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo) {
        config.setClusterInfo(clusterInfo);
    }

    public BeanLevelProperties getBeanLevelProperties() {
        return config.getBeanLevelProperties();
    }

    @Override
    public void setBeanLevelProperties(BeanLevelProperties beanLevelProperties) {
        config.setBeanLevelProperties(beanLevelProperties);
    }

    public void setMetricRegistrator(MetricRegistrator metricRegistrator) {
        config.setMetricRegistrator(metricRegistrator);
    }

    public ProcessingUnitContainerConfig getConfig() {
        return config;
    }

    public void setSpaceName(String spaceName) {
        if (spaceName != null) {
            getBeanLevelProperties().getContextProperties().setProperty(SPACE_NAME_PROPERTY_KEY, spaceName);
        }
    }
}
