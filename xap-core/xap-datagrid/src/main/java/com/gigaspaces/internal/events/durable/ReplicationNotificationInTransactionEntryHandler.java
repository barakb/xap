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
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import java.util.Collection;
import java.util.List;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNotificationInTransactionEntryHandler implements
        IReplicationInTransactionHandler,
        ITransactionalBatchExecutionCallback {

    private final AbstractReplicationNotificationInEntryHandler _entryHandler;
    private final boolean _isBatchMode;

    public ReplicationNotificationInTransactionEntryHandler(AbstractReplicationNotificationInEntryHandler entryHandler, boolean isBatchMode) {
        _entryHandler = entryHandler;
        _isBatchMode = isBatchMode;
    }

    @Override
    public void inTransaction(IReplicationInContext context,
                              ITransactionInContext transactionContext) throws Exception {

        List<IReplicationTransactionalPacketEntryData> packetsData = (List<IReplicationTransactionalPacketEntryData>) transactionContext;

        IReplicationInBatchContext batchContext = (IReplicationInBatchContext) context;
        for (IReplicationTransactionalPacketEntryData packetData : packetsData) {
            packetData.batchExecuteTransactional(batchContext, this);
        }

        if (!_isBatchMode) {
            batchContext.currentConsumed();
        }

    }

    @Override
    public void writeEntry(IReplicationInBatchContext context,
                           IEntryPacket entryPacket) throws Exception {
        _entryHandler.handleWriteEntry(context, true, entryPacket);
    }

    @Override
    public void removeEntry(IReplicationInBatchContext context,
                            IEntryPacket entryPacket) throws Exception {
        _entryHandler.handleRemoveEntry(context, true, entryPacket);
    }

    @Override
    public void removeEntryByUID(IReplicationInBatchContext context,
                                 String typeName, String uid, OperationID operationID) throws Exception {
        _entryHandler.handleRemoveEntryByUID(context, true, uid, false /* isTransient */, operationID);
    }

    @Override
    public void updateEntry(IReplicationInBatchContext context,
                            IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate, short flags) throws Exception {
        _entryHandler.handleUpdateEntry(context, true, entryPacket, oldEntryPacket, partialUpdate, false /* overrideVersion */, flags);
    }

    @Override
    public void inTransactionPrepare(IReplicationInContext context,
                                     ITransactionInContext transactionContext) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void inTransactionCommit(IReplicationInContext context,
                                    ITransactionInContext transactionContext) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void inTransactionAbort(IReplicationInContext context,
                                   ITransactionInContext transactionContext) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeEntry(IReplicationInBatchContext context,
                            String typeName, String uid, Object id,
                            int version, int previousVersion, long timeToLive, int routingHash, Collection<SpaceEntryMutator> spaceEntryMutators, boolean isTransient, OperationID operationID) {
        throw new UnsupportedOperationException();
    }

}
