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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.cluster.replication.async.mirror.MirrorOperationsImpl;
import com.gigaspaces.cluster.replication.async.mirror.MirrorStatisticsImpl;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.sadapter.datasource.BulkDataPersisterContext;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.server.TransactionParticipantDataImpl;

import java.util.Collection;
import java.util.List;

/**
 * Handles incoming replicated operations on mirror
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorReplicationInHandler implements ITransactionalBatchExecutionCallback {
    private final MirrorBulkExecutor _bulkExecutor;
    protected final SpaceSynchronizationEndpoint _syncEndpoint;
    private final SpaceTypeManager _typeManager;
    private final MirrorStatisticsImpl _operationStatisticsHandler;

    public MirrorReplicationInHandler(MirrorBulkExecutor bulkExecutor, MirrorStatisticsImpl operationStatisticsHandler) {
        _bulkExecutor = bulkExecutor;
        _syncEndpoint = bulkExecutor.getSynchronizationInterceptor();
        _typeManager = bulkExecutor.getTypeManager();
        _operationStatisticsHandler = operationStatisticsHandler;
    }

    public void addWriteEntry(IReplicationInBatchContext context,
                              IEntryPacket entryPacket) throws Exception {
        BulkItem bulkItem = createBulkItem(entryPacket, BulkItem.WRITE);
        context.addPendingContext(bulkItem);
    }

    public void addUpdateEntry(IReplicationInBatchContext context,
                               IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate) throws Exception {
        BulkItem bulkItem = null;
        if (partialUpdate) {
            bulkItem = createBulkItem(entryPacket, BulkItem.PARTIAL_UPDATE);
        } else {
            bulkItem = createBulkItem(entryPacket, BulkItem.UPDATE);
        }

        context.addPendingContext(bulkItem);
    }

    public void addRemoveEntry(IReplicationInBatchContext context,
                               IEntryPacket entryPacket) throws Exception {
        BulkItem bulkItem = createBulkItem(entryPacket, BulkItem.REMOVE);
        context.addPendingContext(bulkItem);
    }

    public void addChangeEntry(IReplicationInBatchContext context,
                               String typeName, String uid, Object id, int version, long timeToLive, Collection<SpaceEntryMutator> mutators) throws UnknownTypeException {
        ITypeDesc typeDesc = _typeManager.getTypeDesc(typeName);
        if (typeDesc == null)
            throw new UnknownTypeException("Insufficient Data In Class : " + typeName, typeName);

        MirrorChangeBulkDataItem bulkItem = new MirrorChangeBulkDataItem(typeDesc, uid, id, version, timeToLive, mutators);
        context.addPendingContext(bulkItem);
    }

    public void addTransaction(IReplicationInBatchContext context,
                               ITransactionInContext transactionContext) throws Exception {
        List<IReplicationTransactionalPacketEntryData> packetsData = (List<IReplicationTransactionalPacketEntryData>) transactionContext;
        for (IReplicationTransactionalPacketEntryData packetEntryData : packetsData) {
            packetEntryData.batchExecuteTransactional(context, this);
        }
    }

    private BulkItem createBulkItem(IEntryPacket entryPacket, short operation) throws UnusableEntryException,
            UnknownTypeException {
        _typeManager.loadServerTypeDesc(entryPacket);

        if (operation == BulkItem.PARTIAL_UPDATE)
            return new PartialUpdateMirrorBulkItem(entryPacket, operation);

        return new MirrorBulkDataItem(entryPacket, operation);
    }

    protected void execute(IReplicationInBatchContext batchContext,
                           TransactionParticipantDataImpl transactionParticipantData)
            throws SAException {
        List<BulkItem> pendingBulk = batchContext.getPendingContext();

        if (pendingBulk.isEmpty())
            return;

        beforeBulkExecution(batchContext);
        try {
            BulkDataPersisterContext.setContext(new BulkDataPersisterContext(transactionParticipantData, batchContext.getSourceLookupName()));

            _bulkExecutor.execute(pendingBulk, transactionParticipantData, batchContext);

            afterSuccessfulBulkExecution(batchContext);

            batchContext.pendingConsumed();
        } catch (RuntimeException e) {
            afterFailedBulkExecution(batchContext);
            throw e;
        } finally {
            BulkDataPersisterContext.resetContext();
        }
    }

    /**
     * Invoked before bulk execution in EDS to update mirror statistics
     */
    private void beforeBulkExecution(IReplicationInBatchContext context) {
        if (_operationStatisticsHandler != null) {
            MirrorOperationsImpl sourceChannelStatistics = (MirrorOperationsImpl) _operationStatisticsHandler.getSourceChannelStatistics(context.getSourceLookupName());
            List<BulkItem> bulk = context.getPendingContext();
            sourceChannelStatistics.addOperationCount(bulk);
        }
    }

    /**
     * Invoked after bulk execution in EDS to update mirror statistics
     */
    private void afterSuccessfulBulkExecution(IReplicationInBatchContext context) {
        if (_operationStatisticsHandler != null) {
            MirrorOperationsImpl sourceChannelStatistics = (MirrorOperationsImpl) _operationStatisticsHandler.getSourceChannelStatistics(context.getSourceLookupName());
            List<BulkItem> bulk = context.getPendingContext();
            sourceChannelStatistics.addSuccessfulOperationCount(bulk);
        }
    }

    /**
     * Invoked after bulk execution in EDS to update mirror statistics
     */
    private void afterFailedBulkExecution(IReplicationInBatchContext context) {
        if (_operationStatisticsHandler != null) {
            MirrorOperationsImpl sourceChannelStatistics = (MirrorOperationsImpl) _operationStatisticsHandler.getSourceChannelStatistics(context.getSourceLookupName());
            List<BulkItem> bulk = context.getPendingContext();
            sourceChannelStatistics.addFailedOperationCount(bulk);
        }
    }

    public void removeEntry(IReplicationInBatchContext context,
                            IEntryPacket entryPacket) throws Exception {
        addRemoveEntry(context, entryPacket);
    }

    public void removeEntryByUID(IReplicationInBatchContext context,
                                 String typeName, String uid, OperationID operationID) throws Exception {
        // TODO Currently EDS do not support read by uid
    }

    public void updateEntry(IReplicationInBatchContext context,
                            IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate, short flags) throws Exception {
        addUpdateEntry(context, entryPacket, oldEntryPacket, partialUpdate);
    }

    public void writeEntry(IReplicationInBatchContext context,
                           IEntryPacket entryPacket) throws Exception {
        addWriteEntry(context, entryPacket);
    }

    public SpaceTypeManager getTypeManager() {
        return _typeManager;
    }


    @Override
    public void changeEntry(IReplicationInBatchContext context,
                            String typeName, String uid, Object id,
                            int version, int previousVersion, long timeToLive, int routingHash, Collection<SpaceEntryMutator> spaceEntryMutators, boolean isTransient, OperationID operationID) throws UnknownTypeException {
        addChangeEntry(context, typeName, uid, id, version, timeToLive, spaceEntryMutators);
    }
}