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
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationTargetChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationSourceAlreadyAttachedException;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;


public abstract class AbstractSyncSingleOriginReplicationTargetGroup
        extends AbstractSyncReplicationTargetGroup {

    protected volatile SyncReplicationTargetChannel _activeChannel;

    public AbstractSyncSingleOriginReplicationTargetGroup(
            TargetGroupConfig groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationInFilter inFilter, IReplicationTargetGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                replicationInFacade,
                exceptionHandlerBuilder,
                inFilter,
                stateListener);
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

    @Override
    protected AbstractReplicationTargetChannel createNewChannelImpl(ReplicationEndpointDetails sourceEndpointDetails, IReplicationMonitoredConnection sourceConnection, IBacklogHandshakeRequest handshakeRequest, IReplicationGroupHistory groupHistory) {
        closeChannel(_activeChannel);
        _activeChannel = createChannel(sourceEndpointDetails,
                sourceConnection,
                handshakeRequest,
                groupHistory);
        return _activeChannel;
    }

}