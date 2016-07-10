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
import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.core.OperationID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Replication packet representing a one phase commit transaction
 *
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class TransactionOnePhaseReplicationPacketData
        extends AbstractTransactionReplicationPacketData {
    private static final long serialVersionUID = 1L;

    private OperationID _operationID;

    public TransactionOnePhaseReplicationPacketData() {
    }

    public TransactionOnePhaseReplicationPacketData(OperationID operationID, boolean fromGateway) {
        super(fromGateway);
        _operationID = operationID;
    }

    @Override
    public OperationID getOperationID() {
        return _operationID;
    }

    @Override
    public ReplicationMultipleOperationType getMultipleOperationType() {
        return ReplicationMultipleOperationType.TRANSACTION_ONE_PHASE;
    }

    @Override
    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        inReplicationHandler.inTransaction(context, this);
    }

    @Override
    public IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData> createEmptyMultipleEntryData() {
        TransactionOnePhaseReplicationPacketData data = new TransactionOnePhaseReplicationPacketData(getOperationID(), isFromGateway());
        data.setMetaData(getMetaData());
        return data;
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
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeTransactionData(out);
        IOUtils.writeObject(out, _operationID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        readTransactionData(in);
        _operationID = IOUtils.readObject(in);
    }

    @Override
    protected String getCustomToString() {
        if (getMetaData() == null)
            return super.getCustomToString();

        return super.getCustomToString() + "OperationID=" + getOperationID() + ", Participant ID/Count=" + getMetaData().getParticipantId() + "/" + getMetaData().getTransactionParticipantsCount() + ", ";
    }

    @Override
    public boolean requiresRecoveryFiltering() {
        return false;
    }

    @Override
    public Object getRecoveryFilteringId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMultiParticipantData() {
        return getMetaData() != null && getMetaData().getTransactionParticipantsCount() > 1;
    }
}
