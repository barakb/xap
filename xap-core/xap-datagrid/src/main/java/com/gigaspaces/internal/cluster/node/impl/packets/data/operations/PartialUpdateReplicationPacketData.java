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

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationInContentContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class PartialUpdateReplicationPacketData
        extends UpdateReplicationPacketData {
    private static final long serialVersionUID = 1L;

    public PartialUpdateReplicationPacketData() {
    }

    @Override
    public PartialUpdateReplicationPacketData clone() {
        return (PartialUpdateReplicationPacketData) super.clone();
    }

    public PartialUpdateReplicationPacketData(IEntryPacket entry, boolean fromGateway, boolean overrideVersion, IEntryData previousEntryData, long expertaionTime, IEntryData currentEntryData) {
        super(entry, fromGateway, overrideVersion, previousEntryData, (short) 0, expertaionTime, currentEntryData);
    }

    @Override
    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        try {
            inReplicationHandler.inUpdateEntry(context, getEntryPacket(), getPreviousEntryPacket(), true, isOverrideVersion(), _flags);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null && contentContext.getSecondaryEntryData() != null) {
                _currentEntryData = contentContext.getMainEntryData();
                _previousEntryData = contentContext.getSecondaryEntryData();
                contentContext.clear();
            }
        }
    }

    @Override
    public void executeTransactional(IReplicationInContext context,
                                     ITransactionalExecutionCallback transactionExecutionCallback,
                                     Transaction transaction, boolean twoPhaseCommit) throws Exception {
        try {
            transactionExecutionCallback.updateEntry(context, transaction, twoPhaseCommit, getEntryPacket(), getPreviousEntryPacket(), true, isOverrideVersion(), _flags);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null && contentContext.getSecondaryEntryData() != null) {
                _currentEntryData = contentContext.getMainEntryData();
                _previousEntryData = contentContext.getSecondaryEntryData();
                contentContext.clear();
            }
        }
    }

    @Override
    public void batchExecuteTransactional(IReplicationInBatchContext context,
                                          ITransactionalBatchExecutionCallback executionCallback)
            throws Exception {
        executionCallback.updateEntry(context, getEntryPacket(), getPreviousEntryPacket(), true, _flags);
    }

    @Override
    public String toString() {
        return "PARTIAL UPDATE: " + getEntryPacket();
    }

    @Override
    public boolean containsFullEntryData() {
        return false;
    }

    @Override
    protected void serializePreviousEntryData(ObjectOutput out)
            throws IOException {
        writePreviousEntryData(out);
    }

    @Override
    protected void deserializePreviousEntryData(ObjectInput in)
            throws IOException, ClassNotFoundException {
        readPreviousEntryData(in);
    }

    @Override
    protected void restoreCurrentEntryData() {
        super.restoreCurrentEntryData();
        Object[] currentValues = _currentEntryData.getFixedPropertiesValues();
        Object[] previousValues = _previousEntryData.getFixedPropertiesValues();
        for (int i = 0; i < currentValues.length; i++)
            if (currentValues[i] == null)
                currentValues[i] = previousValues[i];
    }

}
