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

package com.gigaspaces.internal.cluster.node.impl.packets.data.errors;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeErrorResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionOnePhaseReplicationPacketData;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.SpaceEngine;

import net.jini.core.transaction.UnknownTransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class UnknownTransactionConsumeResult
        extends AbstractDataConsumeErrorResult {

    private static final long serialVersionUID = 1L;

    private UnknownTransactionException _exception;

    public UnknownTransactionConsumeResult() {
    }

    public UnknownTransactionConsumeResult(UnknownTransactionException exception) {
        _exception = exception;
    }

    @Override
    public Exception toException() {
        return _exception;
    }

    @Override
    public boolean sameFailure(IDataConsumeResult otherResult) {
        if (!(otherResult instanceof UnknownTransactionConsumeResult))
            return false;

        UnknownTransactionConsumeResult typedOtherResult = (UnknownTransactionConsumeResult) otherResult;
        return typedOtherResult._exception.toString().equals(_exception.toString());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _exception);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _exception = IOUtils.readObject(in);
    }

    @Override
    public AbstractDataConsumeFix createFix(SpaceEngine spaceEngine,
                                            ReplicationPacketDataProducer producer, IExecutableReplicationPacketData errorData) {
        if (errorData != null && errorData.getMultipleOperationType() == ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_COMMIT) {
            AbstractTransactionReplicationPacketData typedData = (AbstractTransactionReplicationPacketData) errorData;
            TransactionOnePhaseReplicationPacketData restoredTransaction = new TransactionOnePhaseReplicationPacketData(typedData.getOperationID(), typedData.isFromGateway());
            restoredTransaction.setMetaData(typedData.getMetaData());
            for (IReplicationTransactionalPacketEntryData entryData : typedData) {
                restoredTransaction.add(entryData);
            }
            return new RestoreFullTransactionFix(restoredTransaction);
        }
        return new UnhandledErrorFix(_exception);
    }

}
