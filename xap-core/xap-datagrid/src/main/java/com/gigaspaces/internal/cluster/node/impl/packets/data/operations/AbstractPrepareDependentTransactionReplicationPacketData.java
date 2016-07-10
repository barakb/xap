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

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.core.OperationID;

import net.jini.core.transaction.server.ServerTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


/**
 * Base class for replication packets that depend on a previous {@link
 * TransactionPrepareReplicationPacketData} occurring
 *
 * @author eitany
 * @since 9.0
 */
public abstract class AbstractPrepareDependentTransactionReplicationPacketData
        extends AbstractTransactionReplicationPacketData {

    private static final long serialVersionUID = 1L;

    private OperationID _operationId;

    public AbstractPrepareDependentTransactionReplicationPacketData() {
    }

    public AbstractPrepareDependentTransactionReplicationPacketData(
            ServerTransaction transaction, OperationID operationID, boolean fromGateway) {
        super(transaction, fromGateway);
        _operationId = operationID;
    }

    @Override
    public boolean supportsReplicationFilter() {
        return false;
    }

    protected boolean attachPrepareContent(ReplicationPacketDataMediator dataMediator) {
        if (dataMediator == null)
            throw new IllegalStateException("Attempt to add content to replicated transaction packet when there is no data mediator");
        if (this.size() != 0)
            throw new IllegalStateException("Attempt to add content to replicated transaction packet which already has content");
        ServerTransaction transaction = getTransaction();
        if (transaction == null)
            throw new IllegalStateException("Attempt to add content to replicated transaction packet which has no server transaction");
        List<IReplicationTransactionalPacketEntryData> pendingTransactionData = dataMediator.removePendingTransactionData(transaction);
        if (pendingTransactionData == null)
            return false;
        for (IReplicationTransactionalPacketEntryData entryData : pendingTransactionData)
            add(entryData);
        return true;
    }

    public OperationID getOperationID() {
        return _operationId;
    }

    @Override
    protected String getCustomToString() {
        return super.getCustomToString() + "OperationID=" + getOperationID() + ", ";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeTransaction(out);
        IOUtils.writeObject(out, _operationId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        readTransaction(in);
        _operationId = IOUtils.readObject(in);
    }

}
