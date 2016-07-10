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

package com.gigaspaces.internal.cluster.node.impl.packets.data.operations;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;

import net.jini.core.transaction.server.ServerTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Replication packet representing transaction prepare
 *
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class TransactionPrepareReplicationPacketData
        extends AbstractTransactionReplicationPacketData {
    private static final long serialVersionUID = 1L;

    public TransactionPrepareReplicationPacketData() {
    }

    public TransactionPrepareReplicationPacketData(ServerTransaction transaction, boolean fromGateway) {
        super(transaction, fromGateway);
    }

    @Override
    public ReplicationMultipleOperationType getMultipleOperationType() {
        return ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_PREPARE;
    }

    @Override
    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        ServerTransaction transaction = getTransaction();
        if (transaction == null)
            throw new IllegalStateException("Attempt to set pending content from pending transaction packet which has no server transaction");
        dataMediator.setPendingTransactionData(transaction, this);
        inReplicationHandler.inTransactionPrepare(context, this);
    }

    @Override
    public IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData> createEmptyMultipleEntryData() {
        return new TransactionPrepareReplicationPacketData(getTransaction(), isFromGateway());
    }

    @Override
    public boolean isPartOfBlobstoreBulk() {
        return false;
    }

    @Override
    public boolean supportsReplicationFilter() {
        return true;
    }

    @Override
    protected String getCustomToString() {
        if (getMetaData() == null)
            return super.getCustomToString();

        return super.getCustomToString() + "Participant ID=" + getMetaData().getParticipantId() + ", ";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeTransaction(out);
        writeTransactionData(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        readTransaction(in);
        readTransactionData(in);
    }

    @Override
    public boolean requiresRecoveryFiltering() {
        return true;
    }

    @Override
    public boolean isMultiParticipantData() {
        return false;
    }
}
