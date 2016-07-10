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
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.ReplicationOperationType;


@com.gigaspaces.api.InternalApi
public class EntryLeaseExpiredFullReplicationPacketData
        extends SingleReplicationPacketData {
    private static final long serialVersionUID = 1L;

    public EntryLeaseExpiredFullReplicationPacketData() {
    }

    public EntryLeaseExpiredFullReplicationPacketData(IEntryPacket entry, boolean fromGateway) {
        super(entry, fromGateway);
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.ENTRY_LEASE_EXPIRED;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public String toString() {
        return "ENTRY LEASE EXPIRED: " + getEntryPacket();
    }

    @Override
    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler,
                        ReplicationPacketDataMediator dataMediator) throws Exception {
        inReplicationHandler.inEntryLeaseExpired(context, getEntryPacket());
    }

    @Override
    protected int getFilterObjectType(IServerTypeDesc serverTypeDesc) {
        return ObjectTypes.ENTRY;
    }

    @Override
    protected ReplicationOperationType getFilterReplicationOpType() {
        return ReplicationOperationType.LEASE_EXPIRATION;
    }

}
