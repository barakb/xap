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
import com.gigaspaces.metrics.MetricRegistrator;

import org.openspaces.core.cluster.ClusterInfo;

/**
 * Provides context for a processing unit container instance.
 *
 * @author Niv Ingberg
 * @since 11.0
 */
public class ProcessingUnitContainerContext {
    private final ClusterInfo clusterInfo;
    private final MetricRegistrator metricRegistrator;

    public ProcessingUnitContainerContext(ProcessingUnitContainerConfig config) {
        this.clusterInfo = config.getClusterInfo();
        this.metricRegistrator = config.getMetricRegistrator();
    }

    /**
     * Gets the cluster information injected with this processing unit container instance.
     *
     * @return The cluster information injected with this processing unit container instance.
     */
    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    /**
     * Creates a bean metric manager using the specified name.
     *
     * @param name Serves as prefix for all metrics which will be registered via this bean metric
     *             manager
     * @return A bean metric manager instance
     */
    public BeanMetricManager createBeanMetricManager(String name) {
        return metricRegistrator.extend(name);
    }
}
