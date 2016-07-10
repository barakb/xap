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
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;


/**
 * {@link IReplicationTargetGroup} that supports only multiple active source that is replicating to
 * it (i.e active active sync replication)
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SyncMultiOriginReplicationTargetGroup
        extends AbstractSyncReplicationTargetGroup {

    private final IReplicationSyncProcessLogBuilder _processLogBuilder;

    public SyncMultiOriginReplicationTargetGroup(
            TargetGroupConfig groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationSyncProcessLogBuilder processLogBuilder,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationInFilter inFilter,
            IReplicationTargetGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                replicationInFacade,
                exceptionHandlerBuilder,
                inFilter,
                stateListener);
        _processLogBuilder = processLogBuilder;
    }

    @Override
    protected AbstractReplicationTargetChannel createNewChannelImpl(
            ReplicationEndpointDetails sourceEndpointDetails, IReplicationMonitoredConnection sourceConnection,
            IBacklogHandshakeRequest handshakeRequest, IReplicationGroupHistory groupHistory) {
        return createChannel(sourceEndpointDetails, sourceConnection, handshakeRequest, groupHistory);
    }

    @Override
    protected void validateConnectChannelImpl(String sourceMemberLookupName) {
        // No extra validation required
    }

    @Override
    protected IReplicationSyncTargetProcessLog createProcessLog(
            TargetGroupConfig groupConfig,
            String sourceLookupName,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler, IReplicationGroupHistory groupHistory, IReplicationConnection sourceConnection) {
        return _processLogBuilder.createSyncProcessLog(groupConfig,
                replicationInFacade,
                exceptionHandler,
                getReplicationRouter().getMyLookupName(),
                getGroupName(), sourceLookupName, groupHistory);
    }

}
