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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IReplicationSyncBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IReplicationSyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;


@com.gigaspaces.api.InternalApi
public class SyncReplicationSourceGroupBuilder
        extends AbstractReplicationSourceGroupBuilder<SourceGroupConfig> {

    private IAsyncHandlerProvider _asyncHandlerProvider;
    private int _asyncStateBatchSize;
    private IReplicationSyncBacklogBuilder _backlogBuilder;
    private long _asyncStateIdleDelayMilis;
    private IReplicationThrottleControllerBuilder _throttleControllerBuilder;
    private ReplicationMode _channelType;

    public SyncReplicationSourceGroupBuilder(
            DynamicSourceGroupConfigHolder groupConfig) {
        super(groupConfig);
    }

    public void setBacklogBuilder(IReplicationSyncBacklogBuilder backlogBuilder) {
        _backlogBuilder = backlogBuilder;
    }

    public void setChannelType(ReplicationMode channelType) {
        _channelType = channelType;
    }

    public void setThrottleController(
            IReplicationThrottleControllerBuilder throttleControllerBuilder) {
        _throttleControllerBuilder = throttleControllerBuilder;
    }

    public void setAsyncHandlerProvider(
            IAsyncHandlerProvider asyncStateHandlerProvider) {
        _asyncHandlerProvider = asyncStateHandlerProvider;
    }

    public void setAsyncStateBatchSize(int asyncStateBatchSize) {
        _asyncStateBatchSize = asyncStateBatchSize;
    }

    public void setAsyncStateIdleDelay(long asyncStateIdleDelayMilis) {
        _asyncStateIdleDelayMilis = asyncStateIdleDelayMilis;
    }

    public IReplicationSourceGroup createGroupImpl(
            DynamicSourceGroupConfigHolder groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationOutFilter outFilter,
            IReplicationSourceGroupStateListener stateListener) {
        IReplicationSyncGroupBacklog groupBacklog = _backlogBuilder.createSyncGroupBacklog(groupConfig);
        return new SyncReplicationSourceGroup(groupConfig,
                replicationRouter,
                outFilter,
                _throttleControllerBuilder,
                _asyncHandlerProvider,
                _asyncStateBatchSize,
                _asyncStateIdleDelayMilis,
                groupBacklog,
                replicationRouter.getMyLookupName(),
                stateListener,
                _channelType);
    }

    @Override
    public String toString() {
        return "SyncReplicationSourceGroupBuilder [_groupConfig="
                + getGroupConfig() + ", _asyncHandlerProvider="
                + _asyncHandlerProvider + ", _asyncStateBatchSize="
                + _asyncStateBatchSize + ", _backlogBuilder=" + _backlogBuilder
                + ", _asyncStateIdleDelayMilis=" + _asyncStateIdleDelayMilis
                + ", _throttleControllerBuilder=" + _throttleControllerBuilder + "]";
    }

}
