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

package com.gigaspaces.internal.cluster.node.impl.groups.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicReliableAsyncSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.IReplicationThrottleControllerBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;


@com.gigaspaces.api.InternalApi
public class ReliableAsyncReplicationSourceGroupBuilder
        extends AbstractReplicationSourceGroupBuilder<ReliableAsyncSourceGroupConfig> {

    private IReplicationReliableAsyncBacklogBuilder _backlogBuilder;
    private IReplicationThrottleControllerBuilder _throttleControllerBuilder;
    private IAsyncHandlerProvider _asyncHandlerProvider;
    private int _syncChannelAsyncStateBatchSize;
    private long _syncChannelIdleDelayMilis;
    private int _asyncChannelBatchSize;
    private long _asyncChannelIntervalMilis;
    private int _asyncChannelIntervalOperations;

    public ReliableAsyncReplicationSourceGroupBuilder(
            DynamicSourceGroupConfigHolder groupConfig) {
        super(groupConfig);
    }

    public void setBacklogBuilder(
            IReplicationReliableAsyncBacklogBuilder backlogBuilder) {
        _backlogBuilder = backlogBuilder;
    }

    public void setThrottleController(
            IReplicationThrottleControllerBuilder throttleController) {
        _throttleControllerBuilder = throttleController;
    }

    public void setAsyncHandlerProvider(
            IAsyncHandlerProvider asyncHandlerProvider) {
        _asyncHandlerProvider = asyncHandlerProvider;
    }

    public void setSyncChannelAsyncStateBatchSize(
            int syncChannelAsyncStateBatchSize) {
        _syncChannelAsyncStateBatchSize = syncChannelAsyncStateBatchSize;
    }

    public void setSyncChannelIdleDelayMilis(long syncChannelIdleDelayMilis) {
        _syncChannelIdleDelayMilis = syncChannelIdleDelayMilis;
    }

    public void setAsyncChannelBatchSize(int asyncChannelBatchSize) {
        _asyncChannelBatchSize = asyncChannelBatchSize;
    }

    public void setAsyncChannelIntervalMilis(long asyncChannelInterval) {
        _asyncChannelIntervalMilis = asyncChannelInterval;
    }

    public void setAsyncChannelIntervalOperations(
            int asyncChannelIntervalOperations) {
        _asyncChannelIntervalOperations = asyncChannelIntervalOperations;
    }

    @Override
    protected IReplicationSourceGroup createGroupImpl(
            DynamicSourceGroupConfigHolder groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationOutFilter outFilter,
            IReplicationSourceGroupStateListener stateListener) {
        IReplicationReliableAsyncGroupBacklog groupBacklog = _backlogBuilder.createReliableAsyncGroupBacklog((DynamicReliableAsyncSourceGroupConfigHolder) groupConfig);
        return new ReliableAsyncReplicationSourceGroup((DynamicReliableAsyncSourceGroupConfigHolder) groupConfig,
                replicationRouter,
                groupBacklog,
                replicationRouter.getMyLookupName(),
                outFilter,
                _throttleControllerBuilder,
                _asyncHandlerProvider,
                _syncChannelAsyncStateBatchSize,
                _syncChannelIdleDelayMilis,
                _asyncChannelBatchSize,
                _asyncChannelIntervalMilis,
                _asyncChannelIntervalOperations,
                stateListener);
    }

    @Override
    public String toString() {
        return "ReliableAsyncReplicationSourceGroupBuilder [_groupConfig="
                + getGroupConfig() + ", _backlogBuilder=" + _backlogBuilder
                + ", _throttleControllerBuilder=" + _throttleControllerBuilder
                + ", _asyncHandlerProvider=" + _asyncHandlerProvider
                + ", _syncChannelAsyncStateBatchSize="
                + _syncChannelAsyncStateBatchSize
                + ", _syncChannelIdleDelayMilis=" + _syncChannelIdleDelayMilis
                + ", _asyncChannelBatchSize=" + _asyncChannelBatchSize
                + ", _asyncChannelInterval=" + _asyncChannelIntervalMilis + "]";
    }

}
