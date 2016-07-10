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

/**
 * Allows for beans implementing this interface to be injected with {@link ClusterInfo}.
 *
 * <p>Note, the cluster information is obtained externally from the application context which means
 * that this feature need to be supported by specific containers (and is not supported by plain
 * Spring application context). This means that beans that implementations of {@link
 * ClusterInfoAware} should take into account the fact that the cluster info provided might be
 * null.
 *
 * @author kimchy
 */
public interface ClusterInfoAware {

    /**
     * Sets the cluster information.
     *
     * <p>Note, the cluster information is obtained externally from the application context which
     * means that this feature need to be supported by specific containers (and is not supported by
     * plain Spring application context). This means that beans that implement {@link
     * ClusterInfoAware} should take into account the fact that the cluster info provided might be
     * null.
     *
     * @param clusterInfo The cluster information to be injected
     */
    void setClusterInfo(ClusterInfo clusterInfo);
}
