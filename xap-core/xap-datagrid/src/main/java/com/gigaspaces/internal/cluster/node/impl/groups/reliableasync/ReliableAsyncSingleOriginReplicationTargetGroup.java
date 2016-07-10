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
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationTargetChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationSourceAlreadyAttachedException;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncReplicationTargetChannel;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;

@com.gigaspaces.api.InternalApi
public class ReliableAsyncSingleOriginReplicationTargetGroup
        extends AbstractReplicationTargetGroup {

    private final IReplicationReliableAsyncProcessLogBuilder _processLogBuilder;
    private final IReplicationProcessLogExceptionHandlerBuilder _exceptionHandlerBuilder;

    private volatile AsyncReplicationTargetChannel _activeChannel;
    private volatile IReplicationReliableAsyncTargetProcessLog _activeProcessLog;

    public ReliableAsyncSingleOriginReplicationTargetGroup(
            TargetGroupConfig groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationInFilter inFilter,
            IReplicationReliableAsyncProcessLogBuilder processLogBuilder,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationTargetGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                replicationInFacade,
                inFilter,
                stateListener);
        _processLogBuilder = processLogBuilder;
        _exceptionHandlerBuilder = exceptionHandlerBuilder;
    }

    @Override
    protected AbstractReplicationTargetChannel createNewChannelImpl(
            ReplicationEndpointDetails sourceEndpointDetails, IReplicationMonitoredConnection sourceConnection,
            IBacklogHandshakeRequest handshakeRequest,
            IReplicationGroupHistory groupHistory) {
        closeChannel(_activeChannel);

        IReplicationProcessLogExceptionHandler exceptionHandler = _exceptionHandlerBuilder.createExceptionHandler(sourceConnection,
                sourceEndpointDetails.getLookupName(),
                getGroupName());
        // Sync new process log with current process log state
        IReplicationReliableAsyncTargetProcessLog processLog = _processLogBuilder.createReliableAsyncProcessLog(getGroupConfig(),
                handshakeRequest,
                getReplicationInFacade(),
                exceptionHandler,
                getReplicationRouter().getMyLookupName(),
                getGroupName(),
                sourceEndpointDetails.getLookupName(), _activeProcessLog,
                groupHistory,
                _specificLogger);

        final boolean wasEverActive = _activeChannel != null;

        _activeChannel = new AsyncReplicationTargetChannel(getGroupConfig(),
                getReplicationRouter().getMyLookupName(),
                getReplicationRouter().getMyUniqueId(),
                sourceEndpointDetails,
                sourceConnection,
                getGroupName(),
                processLog,
                getInFilter(),
                getStateListener(),
                groupHistory,
                wasEverActive);
        _activeProcessLog = processLog;
        return _activeChannel;
    }

    @Override
    protected void validateConnectChannelImpl(String sourceMemberLookupName)
            throws ReplicationSourceAlreadyAttachedException {
        if (_activeChannel != null) {
            boolean activeSourceIsStillAttached = _activeChannel.isSourceAttached();
            if (activeSourceIsStillAttached)
                throw new ReplicationSourceAlreadyAttachedException("Replication group "
                        + getGroupName()
                        + " only supports one active source member, currently active source is "
                        + _activeChannel.getSourceLookupName() + "[" + _activeChannel.getSourceEndpointAddress() + "]");
        }
    }

}
