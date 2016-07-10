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
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class RemoveReplicationPacketData
        extends SingleReplicationPacketData implements IReplicationTransactionalPacketEntryData {
    private static final long serialVersionUID = 1L;
    private boolean _fullEntryPacket;
    private transient IEntryData _entryData;


    public RemoveReplicationPacketData() {
    }

    @Override
    public RemoveReplicationPacketData clone() {
        return (RemoveReplicationPacketData) super.clone();
    }

    public RemoveReplicationPacketData(IEntryPacket entry, boolean fromGateway, IEntryData entryData, boolean fullEntryPacket) {
        super(entry, fromGateway);
        _entryData = entryData;
        _fullEntryPacket = fullEntryPacket;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        try {
            inReplicationHandler.inRemoveEntry(context, getEntryPacket());
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null && contentContext.getMainEntryData() != null) {
                _entryData = contentContext.getMainEntryData();
                contentContext.clear();
            }
        }
    }

    public void executeTransactional(IReplicationInContext context,
                                     ITransactionalExecutionCallback transactionExecutionCallback,
                                     Transaction transaction, boolean twoPhaseCommit) throws Exception {
        try {
            transactionExecutionCallback.removeEntry(context, transaction, twoPhaseCommit, getEntryPacket());
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null && contentContext.getMainEntryData() != null) {
                _entryData = contentContext.getMainEntryData();
                contentContext.clear();
            }
        }
    }

    public void batchExecuteTransactional(IReplicationInBatchContext context,
                                          ITransactionalBatchExecutionCallback executionCallback)
            throws Exception {
        executionCallback.removeEntry(context, getEntryPacket());
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    @Override
    protected int getFilterObjectType(IServerTypeDesc serverTypeDesc) {
        return ObjectTypes.ENTRY;
    }

    @Override
    protected ReplicationOperationType getFilterReplicationOpType() {
        return ReplicationOperationType.TAKE;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.REMOVE_ENTRY;
    }

    @Override
    public String toString() {
        return "REMOVE: " + getEntryPacket();
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public IEntryData getMainEntryData() {
        if (_entryData == null && containsFullEntryData())
            return super.getMainEntryData();
        return _entryData;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            out.writeBoolean(_fullEntryPacket);
    }

    @Override
    public boolean containsFullEntryData() {
        return _fullEntryPacket;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            _fullEntryPacket = in.readBoolean();
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        out.writeBoolean(_fullEntryPacket);
        serializeEntryData(_entryData, out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _fullEntryPacket = in.readBoolean();
        _entryData = deserializeEntryData(in);
    }

}
