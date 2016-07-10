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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencySyncHandler;
import com.gigaspaces.internal.cluster.node.impl.groups.ISpaceItemGroupsExtractor;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaDataProducerBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.IIncomingReplicationHandler;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationRouterBuilder;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;


/**
 * Builds all the components needs to construct a {@link ReplicationNode}
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationNodeBuilder {
    IReplicationBacklogBuilder getReplicationBacklogBuilder();

    IReplicationProcessLogBuilder getReplicationProcessLogBuilder();

    ISpaceItemGroupsExtractor createSpaceItemGroupsExtractor();

    IReplicationRouter createReplicationRouter(
            IIncomingReplicationHandler listener);

    ISpaceReplicaDataConsumer<?, ?> createReplicaDataConsumer();

    ISpaceReplicaDataProducerBuilder createReplicaDataGenerator();

    IAsyncHandlerProvider getAsyncHandlerProvider();

    ReplicationRouterBuilder getReplicationRouterBuilder();

    IDirectPersistencySyncHandler createDirectPersistencySyncHandler();
}
