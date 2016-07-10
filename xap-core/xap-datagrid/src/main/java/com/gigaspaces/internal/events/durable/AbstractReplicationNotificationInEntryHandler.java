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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationReplicationChannelDataFilter;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;

import net.jini.core.event.RemoteEventListener;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dan Kilman
 * @since 9.0
 */
public abstract class AbstractReplicationNotificationInEntryHandler implements
        IReplicationInEntryHandler,
        IReplicationInEntryLeaseExpiredHandler {

    protected final NotifyInfo _notifyInfo;
    protected final NotifyActionType _notifyType;
    protected final RemoteEventListener _listener;
    protected final IDirectSpaceProxy _remoteSpace;
    protected final AtomicLong _sequenceNumber;
    protected final long _eventID;

    private final QueryResultTypeInternal _queryResultType;

    protected AbstractReplicationNotificationInEntryHandler(
            NotifyInfo notifyInfo,
            IDirectSpaceProxy
                    remoteSpace,
            long eventID,
            ITemplatePacket templatePacket) {
        _notifyInfo = notifyInfo;
        _remoteSpace = remoteSpace;
        _eventID = eventID;
        _queryResultType = templatePacket.getQueryResultType();
        _notifyType = NotifyActionType.fromModifier(_notifyInfo.getNotifyType());
        _listener = notifyInfo.getListener();
        _sequenceNumber = new AtomicLong(0);
    }

    protected static void consumeBatchContentIfNotInTransaction(IReplicationInContext context,
                                                                boolean inTransaction) {
        if (!inTransaction) {
            IReplicationInBatchContext batchContext = (IReplicationInBatchContext) context;
            batchContext.currentConsumed();
        }
    }

    public void handleEntryLeaseExpiredByUID(IReplicationInContext context, boolean inTransaction,
                                             String className, String uid, boolean isTransient, OperationID operationID) {
        throw new UnsupportedOperationException();
    }

    public void handleEntryLeaseExpired(IReplicationInContext context, boolean inTransaction, IEntryPacket entryPacket) {
        handleEntry(context, inTransaction, entryPacket, null, NotifyActionType.NOTIFY_LEASE_EXPIRATION);
    }

    public void handleUpdateEntry(IReplicationInContext context, boolean inTransaction, IEntryPacket entryPacket, IEntryPacket oldEntryPacket,
                                  boolean partialUpdate, boolean overrideVersion, short flags) {

        NotifyActionType actionType = NotifyActionType.NOTIFY_UPDATE;

        if (NotificationReplicationChannelDataFilter.DurableNotificationConversionFlags.isUpdateToUnmatched(flags))
            actionType = NotifyActionType.NOTIFY_UNMATCHED;
        else if (NotificationReplicationChannelDataFilter.DurableNotificationConversionFlags.isUpdateToMatchedUpdate(flags))
            actionType = NotifyActionType.NOTIFY_MATCHED_UPDATE;
        else if (NotificationReplicationChannelDataFilter.DurableNotificationConversionFlags.isUpdateToRematchedUpdate(flags))
            actionType = NotifyActionType.NOTIFY_REMATCHED_UPDATE;

        handleEntry(context, inTransaction, entryPacket, oldEntryPacket, actionType);
    }

    public void handleRemoveEntry(IReplicationInContext context, boolean inTransaction, IEntryPacket entryPacket) {
        handleEntry(context, inTransaction, entryPacket, null, NotifyActionType.NOTIFY_TAKE);
    }

    public void handleRemoveEntryByUID(IReplicationInContext context, boolean inTransaction,
                                       String uid, boolean isTransient, OperationID operationID) {
        throw new UnsupportedOperationException();
    }

    public void handleWriteEntry(IReplicationInContext context, boolean inTransaction, IEntryPacket entryPacket) {
        handleEntry(context, inTransaction, entryPacket, null, NotifyActionType.NOTIFY_WRITE);
    }

    private void handleEntry(IReplicationInContext context,
                             boolean inTransaction, IEntryPacket entryPacket, IEntryPacket oldEntryPacket,
                             NotifyActionType notifyActionType) {
        _remoteSpace.getTypeManager().loadTypeDescToPacket(entryPacket);
        EntryArrivedRemoteEvent entryArrived = createEntryArrivedRemoteEvent(entryPacket, oldEntryPacket, notifyActionType);

        handleEntryImpl(entryArrived, context, inTransaction);

    }

    protected abstract void handleEntryImpl(EntryArrivedRemoteEvent entryArrived,
                                            IReplicationInContext context, boolean inTransaction);

    protected EntryArrivedRemoteEvent createEntryArrivedRemoteEvent(IEntryPacket entryPacket, IEntryPacket oldEntryPacket, NotifyActionType notifyActionType) {
        if (notifyActionType.equals(NotifyActionType.NOTIFY_UNMATCHED))
            return new EntryArrivedRemoteEvent(_remoteSpace,
                    _eventID,
                    _sequenceNumber.getAndIncrement(),
                    _notifyInfo.getHandback(),
                    oldEntryPacket, entryPacket,
                    notifyActionType,
                    false /* from replication - not used */,
                    _notifyInfo.getTemplateUID(),
                    _queryResultType);

        else
            return new EntryArrivedRemoteEvent(_remoteSpace,
                    _eventID,
                    _sequenceNumber.getAndIncrement(),
                    _notifyInfo.getHandback(),
                    entryPacket, oldEntryPacket,
                    notifyActionType,
                    false /* from replication - not used */,
                    _notifyInfo.getTemplateUID(),
                    _queryResultType);
    }

    @Override
    public void inEntryLeaseExpiredByUID(IReplicationInContext context, String className,
                                         String uid, boolean isTransient, OperationID operationID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void inEntryLeaseExpired(IReplicationInContext context, IEntryPacket entryPacket) {
        handleEntryLeaseExpired(context, false, entryPacket);
    }

    @Override
    public void inUpdateEntry(IReplicationInContext context, IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate,
                              boolean overrideVersion, short flags) throws Exception {
        handleUpdateEntry(context, false, entryPacket, oldEntryPacket, partialUpdate, overrideVersion, flags);
    }

    @Override
    public void inRemoveEntry(IReplicationInContext context, IEntryPacket entryPacket) throws Exception {
        handleRemoveEntry(context, false, entryPacket);
    }

    @Override
    public void inRemoveEntryByUID(IReplicationInContext context, String typeName, String uid,
                                   boolean isTransient, OperationID operationID) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void inWriteEntry(IReplicationInContext context, IEntryPacket entryPacket) throws Exception {
        handleWriteEntry(context, false, entryPacket);
    }

    @Override
    public void inChangeEntry(IReplicationInContext context, String typeName, String uid, Object id, int version, int previousVersion, int routingHash, long timeToLive, Collection<SpaceEntryMutator> mutators, boolean isTransient, OperationID operationID) throws Exception {
        throw new UnsupportedOperationException();

    }

    @Override
    public void afterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey) {
    }

    @Override
    public void beforeConsume(IReplicationInContext context) {
    }
}