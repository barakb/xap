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


package com.gigaspaces.internal.events.durable;

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInBatchConsumptionHandler;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;

import java.util.List;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNotificationInBatchConsumptionHandler
        implements IReplicationInBatchConsumptionHandler {

    private final BatchRemoteEventListener _listener;
    private final NotifyCallHandler _notifyCallHandler;

    public ReplicationNotificationInBatchConsumptionHandler(
            NotifyInfo notifyInfo, ITemplatePacket templatePacket,
            ReplicationNotificationClientEndpoint endpoint) {
        _listener = (BatchRemoteEventListener) notifyInfo.getListener();
        _notifyCallHandler = new NotifyCallHandler(_listener,
                notifyInfo,
                templatePacket,
                endpoint);
    }

    @Override
    public void consumePendingOperationsInBatch(
            IReplicationInBatchContext context) {

        if (context.getPendingContext().isEmpty()) {
            context.currentConsumed();
            return;
        }

        List<EntryArrivedRemoteEvent> events = context.getPendingContext();
        EntryArrivedRemoteEvent[] eventsNotification = events.toArray(new EntryArrivedRemoteEvent[events.size()]);
        BatchRemoteEvent batchEvent = new BatchRemoteEvent(eventsNotification);

        _notifyCallHandler.notifyBatch(batchEvent);

        context.pendingConsumed();
    }

}
