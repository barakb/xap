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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.DummyUrlConverter;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IConnectionMonitor;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.ISpaceProxyProvider;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.ISpaceUrlConverter;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.SpaceFinderSpaceProxyProvider;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.SpaceProxyReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.SpaceUrlConverter;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.failuredetector.DummyFailureDetector;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.failuredetector.FailureDetectorConnectionMonitor;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.failuredetector.IFailureDetector;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.replication.SpaceServiceExporter;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.cluster.ReplicationPolicy;

import java.util.concurrent.TimeUnit;

@com.gigaspaces.api.InternalApi
public class ReplicationRouterBuilderFactory {

    public ReplicationRouterBuilderFactory() {

    }

    public ReplicationRouterBuilder create(SpaceImpl spaceImpl) {
        return createSpaceProxyReplicationRouterBuilder(spaceImpl, false);
    }

    public ReplicationRouterBuilder createSpaceProxyReplicationRouterBuilder(SpaceImpl spaceImpl, boolean dummy) {
        final ClusterPolicy clusterPolicy = spaceImpl.getClusterPolicy();
        final boolean isReplicated = clusterPolicy != null && clusterPolicy.m_Replicated;
        ISpaceUrlConverter spaceUrlConverter = dummy
                ? new DummyUrlConverter()
                : new SpaceUrlConverter(clusterPolicy.getReplicationPolicy());
        ISpaceProxyProvider spaceProxyProvider = new SpaceFinderSpaceProxyProvider();
        final int CORE_POOL_SIZE = isReplicated ? clusterPolicy.getReplicationPolicy().getConnectionMonitorThreadPoolSize() : ReplicationPolicy.DEFAULT_CONNECTION_MONITOR_THREAD_POOL_SIZE;
        final long MONITOR_DISCONNECTED_DELAY = 1;

        final String ownMemberName = spaceImpl.getConfigReader().getFullSpaceName();
        IFailureDetector failureDetector = spaceImpl.getClusterFailureDetector() != null ? spaceImpl.getClusterFailureDetector() : new DummyFailureDetector();
        IConnectionMonitor connectionMonitor = new FailureDetectorConnectionMonitor(ownMemberName,
                CORE_POOL_SIZE, MONITOR_DISCONNECTED_DELAY, TimeUnit.SECONDS, spaceProxyProvider, failureDetector);


        return new SpaceProxyReplicationRouter.Builder(ownMemberName,
                spaceImpl.getUuid(),
                spaceProxyProvider,
                spaceUrlConverter,
                connectionMonitor,
                new SpaceServiceExporter(spaceImpl),
                CORE_POOL_SIZE);
    }
}
