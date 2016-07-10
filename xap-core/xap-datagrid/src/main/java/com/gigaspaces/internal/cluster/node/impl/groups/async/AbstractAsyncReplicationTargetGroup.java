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

package com.gigaspaces.internal.cluster.node.impl.groups.async;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationAsyncProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;


public abstract class AbstractAsyncReplicationTargetGroup
        extends AbstractReplicationTargetGroup {

    private final IReplicationAsyncProcessLogBuilder _processLogBuilder;
    private final IReplicationProcessLogExceptionHandlerBuilder _exceptionHandlerBuilder;

    public AbstractAsyncReplicationTargetGroup(
            TargetGroupConfig groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationAsyncProcessLogBuilder processLogBuilder,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationInFilter inFilter,
            IReplicationTargetGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                replicationInFacade,
                inFilter,
                stateListener);
        _processLogBuilder = processLogBuilder;
        _exceptionHandlerBuilder = exceptionHandlerBuilder;
    }

    protected AsyncReplicationTargetChannel createChannel(
            ReplicationEndpointDetails sourceEndpointDetails,
            IReplicationMonitoredConnection sourceConnection,
            IBacklogHandshakeRequest handshakeRequest, IReplicationGroupHistory groupHistory) {
        IReplicationProcessLogExceptionHandler exceptionHandler = _exceptionHandlerBuilder.createExceptionHandler(sourceConnection,
                sourceEndpointDetails.getLookupName(),
                getGroupName());
        IReplicationAsyncTargetProcessLog processLog = createProcessLog(_processLogBuilder,
                sourceEndpointDetails.getLookupName(),
                getReplicationInFacade(),
                exceptionHandler,
                handshakeRequest,
                groupHistory);
        return new AsyncReplicationTargetChannel(getGroupConfig(),
                getReplicationRouter().getMyLookupName(),
                getReplicationRouter().getMyUniqueId(),
                sourceEndpointDetails,
                sourceConnection,
                getGroupName(),
                processLog,
                getInFilter(),
                getStateListener(),
                groupHistory,
                false);
    }

    protected abstract IReplicationAsyncTargetProcessLog createProcessLog(
            IReplicationAsyncProcessLogBuilder processLogBuilder,
            String sourceLookupName, IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IBacklogHandshakeRequest handshakeRequest, IReplicationGroupHistory groupHistory);

}
