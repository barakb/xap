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

package com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile.MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderReliableAsyncTargetKeeperProcessLog;


@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog
        extends GlobalOrderReliableAsyncTargetKeeperProcessLog {

    public MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog(
            MultiSourceSingleFileProcessLogConfig processLogConfig,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade,
            String name,
            String groupName, String sourceLookupName, boolean oneWayMode,
            IReplicationGroupHistory groupHistory, IReplicationReliableAsyncMediator mediator) {
        super(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                oneWayMode,
                groupHistory,
                mediator);
    }

    public MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog(
            MultiSourceSingleFileProcessLogConfig processLogConfig,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade,
            String name,
            String groupName, String sourceLookupName, long lastProcessedKey, boolean isFirstHandshake, boolean oneWayMode,
            IReplicationGroupHistory groupHistory, IReplicationReliableAsyncMediator mediator) {
        super(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKey,
                isFirstHandshake,
                oneWayMode,
                groupHistory,
                mediator);
    }

    @Override
    protected IReliableAsyncState getReliableAsyncStateFromHandshakeRequest(
            IBacklogHandshakeRequest handshakeRequest) {
        MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest typedHandshakeRequest = (MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest) handshakeRequest;
        return typedHandshakeRequest.getReliableAsyncState();
    }

}
