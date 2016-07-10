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
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;

@com.gigaspaces.api.InternalApi
public class ReplicationNodeBuilder
        implements IReplicationNodeBuilder {

    private IReplicationBacklogBuilder _replicationBacklogBuilder;
    private IReplicationProcessLogBuilder _replicationProcessLogBuilder;
    private ISpaceItemGroupsExtractor _spaceItemGroupsExtractor;
    private ISpaceReplicaDataConsumer _replicaDataConsumer;
    private ISpaceReplicaDataProducerBuilder _replicaDataProducerBuilder;
    private ReplicationRouterBuilder _routerBuilder;
    private IAsyncHandlerProvider _asyncHandlerProvider;
    private IDirectPersistencySyncHandler _directPersistencySyncHandler;

    public IDirectPersistencySyncHandler createDirectPersistencySyncHandler() {
        return _directPersistencySyncHandler;
    }

    public void setDirectPersistencySyncHandler(IDirectPersistencySyncHandler _directPersistencySyncHandler) {
        this._directPersistencySyncHandler = _directPersistencySyncHandler;
    }

    public void setReplicationBacklogBuilder(
            IReplicationBacklogBuilder replicationBacklogBuilder) {
        _replicationBacklogBuilder = replicationBacklogBuilder;
    }

    public void setReplicationProcessLogBuilder(
            IReplicationProcessLogBuilder replicationProcessLogBuilder) {
        _replicationProcessLogBuilder = replicationProcessLogBuilder;
    }

    public void setSpaceItemGroupsExtractor(
            ISpaceItemGroupsExtractor spaceItemGroupsExtractor) {
        _spaceItemGroupsExtractor = spaceItemGroupsExtractor;
    }

    public void setReplicaDataConsumer(ISpaceReplicaDataConsumer replicaDataConsumer) {
        _replicaDataConsumer = replicaDataConsumer;
    }

    public void setReplicaDataProducerBuilder(
            ISpaceReplicaDataProducerBuilder replicaDataProducerBuilder) {
        _replicaDataProducerBuilder = replicaDataProducerBuilder;
    }

    public void setAsyncHandlerProvider(IAsyncHandlerProvider asyncHandlerProvider) {
        _asyncHandlerProvider = asyncHandlerProvider;
    }

    public IAsyncHandlerProvider getAsyncHandlerProvider() {
        return _asyncHandlerProvider;
    }

    public ISpaceReplicaDataProducerBuilder createReplicaDataGenerator() {
        return _replicaDataProducerBuilder;
    }

    public ISpaceReplicaDataConsumer createReplicaDataConsumer() {
        return _replicaDataConsumer;
    }

    public IReplicationBacklogBuilder getReplicationBacklogBuilder() {
        return _replicationBacklogBuilder;
    }

    public IReplicationProcessLogBuilder getReplicationProcessLogBuilder() {
        return _replicationProcessLogBuilder;
    }

    public IReplicationRouter createReplicationRouter(
            IIncomingReplicationHandler listener) {
        return _routerBuilder.create(listener);
    }

    public ISpaceItemGroupsExtractor createSpaceItemGroupsExtractor() {
        return _spaceItemGroupsExtractor;
    }

    @Override
    public ReplicationRouterBuilder getReplicationRouterBuilder() {
        return _routerBuilder;
    }

    public void setReplicationRouterBuilder(
            ReplicationRouterBuilder routerBuilder) {
        _routerBuilder = routerBuilder;
    }

    @Override
    public String toString() {
        return "ReplicationNodeBuilder [" + StringUtils.NEW_LINE +
                "\t_replicationBacklogBuilder=" + _replicationBacklogBuilder + StringUtils.NEW_LINE +
                "\t_replicationProcessLogBuilder=" + _replicationProcessLogBuilder + StringUtils.NEW_LINE +
                "\t_spaceItemGroupsExtractor=" + _spaceItemGroupsExtractor + StringUtils.NEW_LINE +
                "\t_replicaDataConsumer=" + _replicaDataConsumer + StringUtils.NEW_LINE +
                "\t_replicaDataProducerBuilder=" + _replicaDataProducerBuilder + StringUtils.NEW_LINE +
                "\t_routerBuilder=" + _routerBuilder + StringUtils.NEW_LINE +
                "\t_asyncHandlerProvider=" + _asyncHandlerProvider + StringUtils.NEW_LINE +
                "]";
    }


}
