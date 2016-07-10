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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInBatchConsumptionHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeIndexAddedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseCancelledHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExtendedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEvictEntryHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotificationSentHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateLeaseExtendedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateRemovedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationNodeGroupsHolder;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.OperationID;

import java.util.Collection;


/**
 * A component that belongs to the {@link ReplicationNode} that implements the {@link
 * IReplicationInFacade}
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNodeInFacade
        implements IReplicationInFacade {

    private final ReplicationNodeGroupsHolder _groupsHolder;
    private IReplicationInEntryLeaseCancelledHandler _inCancelEntryLeaseHandler;
    private IReplicationInEntryLeaseExpiredHandler _inEntryLeaseExpiredHandler;
    private IReplicationInEvictEntryHandler _inEvictEntryHandler;
    private IReplicationInEntryLeaseExtendedHandler _inExtendEntryLeasePeriod;
    private IReplicationInNotifyTemplateLeaseExtendedHandler _inExtendNotifyTemplateLeasePeriod;
    private IReplicationInNotifyTemplateCreatedHandler _inInsertNotifyTemplate;
    private IReplicationInNotificationSentHandler _inNotificationSent;
    private IReplicationInNotifyTemplateRemovedHandler _inRemoveNotifyTemplate;
    private IReplicationInTransactionHandler _inTransaction;
    private IReplicationInEntryHandler _inEntry;
    private IReplicationInDataTypeCreatedHandler _inDataTypeIntroduce;
    private IReplicationInDataTypeIndexAddedHandler _inDataTypeAddIndex;
    private IReplicationInNotifyTemplateLeaseExpiredHandler _inNotifyTemplateLeaseExpiredHandler;
    private IReplicationInBatchConsumptionHandler _inBatchConsumptionHandler;

    public ReplicationNodeInFacade(ReplicationNodeGroupsHolder groupsHolder) {
        _groupsHolder = groupsHolder;
    }

    @Override
    public void inCancelEntryLease(IReplicationInContext context,
                                   IEntryPacket entryPacket) throws Exception {
        if (_inCancelEntryLeaseHandler != null)
            _inCancelEntryLeaseHandler.inCancelEntryLease(context, entryPacket);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inCancelEntryLeaseByUID(IReplicationInContext context,
                                        String className, String uid, boolean isTransient, int routingValue) throws Exception {
        if (_inCancelEntryLeaseHandler != null)
            _inCancelEntryLeaseHandler.inCancelEntryLeaseByUID(context,
                    className,
                    uid,
                    isTransient,
                    routingValue);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inEntryLeaseExpired(IReplicationInContext context,
                                    IEntryPacket entryPacket) {
        if (_inEntryLeaseExpiredHandler != null)
            _inEntryLeaseExpiredHandler.inEntryLeaseExpired(context, entryPacket);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inEntryLeaseExpiredByUID(IReplicationInContext context,
                                         String className, String uid, boolean isTransient, OperationID operationID) {
        if (_inEntryLeaseExpiredHandler != null)
            _inEntryLeaseExpiredHandler.inEntryLeaseExpiredByUID(context,
                    className,
                    uid, isTransient, operationID);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inNotifyTemplateLeaseExpired(IReplicationInContext context,
                                             String className, String uid, OperationID operationID) {
        if (_inNotifyTemplateLeaseExpiredHandler != null)
            _inNotifyTemplateLeaseExpiredHandler.inNotifyTemplateLeaseExpired(context,
                    className,
                    uid, operationID);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inEvictEntry(IReplicationInContext context, String uid,
                             boolean isTransient, OperationID operationID) throws Exception {
        if (_inEvictEntryHandler != null)
            _inEvictEntryHandler.inEvictEntry(context, uid, isTransient, operationID);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inExtendEntryLeasePeriod(IReplicationInContext context,
                                         String className, String uid, boolean isTransient, long lease, int routingValue) throws Exception {
        if (_inExtendEntryLeasePeriod != null)
            _inExtendEntryLeasePeriod.inExtendEntryLeasePeriod(context,
                    className,
                    uid,
                    isTransient,
                    lease,
                    routingValue);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inExtendNotifyTemplateLeasePeriod(
            IReplicationInContext context, String className, String uid,
            long lease) throws Exception {
        if (_inExtendNotifyTemplateLeasePeriod != null)
            _inExtendNotifyTemplateLeasePeriod.inExtendNotifyTemplateLeasePeriod(context,
                    className,
                    uid,
                    lease);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inInsertNotifyTemplate(IReplicationInContext context,
                                       ITemplatePacket templatePacket, String uidToOperateBy, NotifyInfo notifyInfo)
            throws Exception {
        if (_inInsertNotifyTemplate != null)
            _inInsertNotifyTemplate.inInsertNotifyTemplate(context,
                    templatePacket,
                    uidToOperateBy,
                    notifyInfo);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inNotificationSent(IReplicationInContext context,
                                   OperationID operationId) throws Exception {
        if (_inNotificationSent != null)
            _inNotificationSent.inNotificationSent(context, operationId);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inRemoveEntry(IReplicationInContext context,
                              IEntryPacket entryPacket) throws Exception {
        if (_inEntry != null)
            _inEntry.inRemoveEntry(context, entryPacket);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inRemoveEntryByUID(IReplicationInContext context, String typeName,
                                   String uid, boolean isTransient, OperationID operationID) throws Exception {
        if (_inEntry != null)
            _inEntry.inRemoveEntryByUID(context, typeName, uid, isTransient, operationID);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    public void inRemoveNotifyTemplate(IReplicationInContext context,
                                       String className, String uid) {
        if (_inRemoveNotifyTemplate != null)
            _inRemoveNotifyTemplate.inRemoveNotifyTemplate(context,
                    className,
                    uid);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inTransaction(IReplicationInContext context,
                              ITransactionInContext transactionContext) throws Exception {
        if (_inTransaction != null)
            _inTransaction.inTransaction(context, transactionContext);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inTransactionPrepare(IReplicationInContext context,
                                     ITransactionInContext transactionContext) throws Exception {
        if (_inTransaction != null)
            _inTransaction.inTransactionPrepare(context, transactionContext);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inTransactionCommit(IReplicationInContext context,
                                    ITransactionInContext transactionContext) throws Exception {
        if (_inTransaction != null)
            _inTransaction.inTransactionCommit(context, transactionContext);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inTransactionAbort(IReplicationInContext context,
                                   ITransactionInContext transactionContext) throws Exception {
        if (_inTransaction != null)
            _inTransaction.inTransactionAbort(context, transactionContext);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inUpdateEntry(IReplicationInContext context,
                              IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate, boolean overrideVersion, short flags) throws Exception {
        if (_inEntry != null)
            _inEntry.inUpdateEntry(context, entryPacket, oldEntryPacket, partialUpdate, overrideVersion, flags);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inChangeEntry(IReplicationInContext context, String typeName, String uid,
                              Object id, int version,
                              int previousVersion, int routingHash, long timeToLive, Collection<SpaceEntryMutator> mutators, boolean isTransient, OperationID operationID) throws Exception {
        if (_inEntry != null)
            _inEntry.inChangeEntry(context, typeName, uid, id, version, previousVersion, routingHash, timeToLive, mutators, isTransient, operationID);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inWriteEntry(IReplicationInContext context,
                             IEntryPacket entryPacket) throws Exception {
        if (_inEntry != null)
            _inEntry.inWriteEntry(context, entryPacket);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inDataTypeAddIndex(IReplicationInContext context,
                                   AddTypeIndexesRequestInfo addIndexRequestInfo) throws Exception {
        if (_inDataTypeAddIndex != null)
            _inDataTypeAddIndex.inDataTypeAddIndex(context, addIndexRequestInfo);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void inDataTypeIntroduce(IReplicationInContext context,
                                    ITypeDesc typeDescriptor) throws Exception {
        if (_inDataTypeIntroduce != null)
            _inDataTypeIntroduce.inDataTypeIntroduce(context, typeDescriptor);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    @Override
    public void consumePendingOperationsInBatch(
            IReplicationInBatchContext context) throws Exception {
        if (_inBatchConsumptionHandler != null)
            _inBatchConsumptionHandler.consumePendingOperationsInBatch(context);
        else if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    public synchronized void setInEntryLeaseCancelledHandler(
            IReplicationInEntryLeaseCancelledHandler inCancelEntryLeaseHandler) {
        _inCancelEntryLeaseHandler = inCancelEntryLeaseHandler;
    }

    public synchronized void setInEntryLeaseExpiredHandler(
            IReplicationInEntryLeaseExpiredHandler inEntryLeaseExpiredHandler) {
        _inEntryLeaseExpiredHandler = inEntryLeaseExpiredHandler;
    }

    public synchronized void setInNotifyTemplateLeaseExpiredHandler(
            IReplicationInNotifyTemplateLeaseExpiredHandler inNotifyTemplateLeaseExpiredHandler) {
        _inNotifyTemplateLeaseExpiredHandler = inNotifyTemplateLeaseExpiredHandler;
    }

    public synchronized void setInEvictEntryHandler(
            IReplicationInEvictEntryHandler inEvictEntryHandler) {
        _inEvictEntryHandler = inEvictEntryHandler;
    }

    public synchronized void setInEntryLeaseExtendedHandler(
            IReplicationInEntryLeaseExtendedHandler handler) {
        _inExtendEntryLeasePeriod = handler;
    }

    public synchronized void setInNotifyTemplateLeaseExtendedHandler(
            IReplicationInNotifyTemplateLeaseExtendedHandler handler) {
        _inExtendNotifyTemplateLeasePeriod = handler;
    }

    public synchronized void setInNotifyTemplateCreatedHandler(
            IReplicationInNotifyTemplateCreatedHandler handler) {
        _inInsertNotifyTemplate = handler;
    }

    public synchronized void setInNotificationSentHandler(
            IReplicationInNotificationSentHandler handler) {
        _inNotificationSent = handler;
    }

    public synchronized void setInNotifyTemplateRemovedHandler(
            IReplicationInNotifyTemplateRemovedHandler handler) {
        _inRemoveNotifyTemplate = handler;
    }

    public synchronized void setInTransactionHandler(
            IReplicationInTransactionHandler handler) {
        _inTransaction = handler;
    }

    public synchronized void setInEntryHandler(IReplicationInEntryHandler handler) {
        _inEntry = handler;
    }

    public synchronized void setInDataTypeCreatedHandler(
            IReplicationInDataTypeCreatedHandler handler) {
        _inDataTypeIntroduce = handler;
    }

    public synchronized void setInDataTypeIndexAddedHandler(
            IReplicationInDataTypeIndexAddedHandler handler) {
        _inDataTypeAddIndex = handler;
    }

    public synchronized void setInBatchConsumptionHandler(IReplicationInBatchConsumptionHandler inBatchConsumptionHandler) {
        _inBatchConsumptionHandler = inBatchConsumptionHandler;
    }

    public IReplicationSourceGroup getReplicationSourceGroup(String groupName) {
        return _groupsHolder.getSourceGroup(groupName);
    }

    @Override
    public void afterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey) {
        if (_inEntry != null) {
            _inEntry.afterConsumption(context, successful, lastProcessedKey);
        }
    }

    @Override
    public void beforeConsume(IReplicationInContext context) {
        if (_inEntry != null) {
            _inEntry.beforeConsume(context);
        }
    }

}
