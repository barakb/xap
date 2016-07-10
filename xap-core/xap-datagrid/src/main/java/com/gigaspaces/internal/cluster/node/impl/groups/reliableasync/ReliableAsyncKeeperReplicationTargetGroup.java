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

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicReliableAsyncSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilterBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.AbstractSyncSingleOriginReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncReplicationTargetChannel;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncKeeperProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;


@com.gigaspaces.api.InternalApi
public class ReliableAsyncKeeperReplicationTargetGroup
        extends AbstractSyncSingleOriginReplicationTargetGroup {

    private final IReplicationReliableAsyncKeeperProcessLogBuilder _processLogBuilder;
    private final IReplicationChannelDataFilterBuilder _filterBuilder;
    private final IDynamicSourceGroupMemberLifeCycleBuilder _lifeCycleBuilder;

    public ReliableAsyncKeeperReplicationTargetGroup(
            TargetGroupConfig groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationInFilter inFilter,
            IReplicationReliableAsyncKeeperProcessLogBuilder processLogBuilder,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationTargetGroupStateListener stateListener,
            IReplicationChannelDataFilterBuilder filterBuilder,
            IDynamicSourceGroupMemberLifeCycleBuilder lifeCycleBuilder) {
        super(groupConfig,
                replicationRouter,
                replicationInFacade,
                exceptionHandlerBuilder,
                inFilter,
                stateListener);
        _processLogBuilder = processLogBuilder;
        _filterBuilder = filterBuilder;
        _lifeCycleBuilder = lifeCycleBuilder;
    }

    @Override
    protected IReplicationSyncTargetProcessLog createProcessLog(
            TargetGroupConfig groupConfig, String sourceLookupName,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationGroupHistory groupHistory,
            IReplicationConnection sourceConnection) {
        // The reliable async keeper target group name should be identical to
        // the reliable async source group name
        IReplicationSourceGroup sourceGroup = replicationInFacade.getReplicationSourceGroup(getGroupName());
        IReplicationReliableAsyncGroupBacklog reliableAsyncGroupBacklog = (IReplicationReliableAsyncGroupBacklog) sourceGroup.getGroupBacklog();
        IReplicationReliableAsyncMediator mediator = new SourceDelegationReplicationReliableAsyncMediator((DynamicReliableAsyncSourceGroupConfigHolder) sourceGroup.getConfigHolder(),
                sourceConnection,
                reliableAsyncGroupBacklog,
                sourceLookupName,
                getReplicationRouter().getMyLookupName(),
                getReplicationRouter(),
                _filterBuilder,
                _lifeCycleBuilder);

        SyncReplicationTargetChannel activeChannel = _activeChannel;

        IReplicationSyncTargetProcessLog activeChannelProcessLog = (IReplicationSyncTargetProcessLog) (activeChannel != null ? activeChannel.getProcessLog() : null);

        return _processLogBuilder.createReliableAsyncKeeperProcessLog(groupConfig,
                replicationInFacade,
                exceptionHandler,
                getReplicationRouter().getMyLookupName(),
                getGroupName(),
                sourceLookupName,
                groupHistory,
                mediator,
                activeChannelProcessLog);
    }
}
