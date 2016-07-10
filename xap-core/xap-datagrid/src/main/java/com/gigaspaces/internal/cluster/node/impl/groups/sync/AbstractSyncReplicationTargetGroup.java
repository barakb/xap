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

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;


public abstract class AbstractSyncReplicationTargetGroup
        extends AbstractReplicationTargetGroup {

    private final IReplicationProcessLogExceptionHandlerBuilder _exceptionHandlerBuilder;

    public AbstractSyncReplicationTargetGroup(
            TargetGroupConfig groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationInFilter inFilter,
            IReplicationTargetGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                replicationInFacade,
                inFilter,
                stateListener);
        _exceptionHandlerBuilder = exceptionHandlerBuilder;
    }

    protected SyncReplicationTargetChannel createChannel(
            ReplicationEndpointDetails sourceEndpointDetails,
            IReplicationMonitoredConnection sourceConnection,
            IBacklogHandshakeRequest handshakeRequest,
            IReplicationGroupHistory groupHistory) {
        IReplicationProcessLogExceptionHandler exceptionHandler = _exceptionHandlerBuilder.createExceptionHandler(sourceConnection,
                sourceEndpointDetails.getLookupName(),
                getGroupName());
        IReplicationSyncTargetProcessLog processLog = createProcessLog(getGroupConfig(),
                sourceEndpointDetails.getLookupName(),
                getReplicationInFacade(),
                exceptionHandler,
                groupHistory,
                sourceConnection);
        return new SyncReplicationTargetChannel(getGroupConfig(),
                getReplicationRouter().getMyLookupName(),
                getReplicationRouter().getMyUniqueId(),
                sourceEndpointDetails,
                sourceConnection,
                getGroupName(),
                processLog,
                getInFilter(),
                getStateListener(),
                groupHistory);
    }

    protected abstract IReplicationSyncTargetProcessLog createProcessLog(
            TargetGroupConfig groupConfig, String sourceLookupName,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationGroupHistory groupHistory,
            IReplicationConnection sourceConnection);

}