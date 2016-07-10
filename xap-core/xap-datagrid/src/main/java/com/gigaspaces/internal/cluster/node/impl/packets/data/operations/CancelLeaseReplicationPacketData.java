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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class CancelLeaseReplicationPacketData
        extends SingleUidReplicationPacketData {
    private static final long serialVersionUID = 1L;
    private String _className;
    private int _routingHash;

    public CancelLeaseReplicationPacketData() {
    }

    public CancelLeaseReplicationPacketData(String className, String uid, boolean isTransient,
                                            IEntryData entryData, OperationID operationID, int routingHash, boolean isFromGateway) {
        super(uid, operationID, isTransient, isFromGateway, entryData);
        _className = className;
        _routingHash = routingHash;
    }

    public String getTypeName() {
        return _className;
    }

    public void setClassName(String className) {
        _className = className;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeRepetitiveString(out, _className);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            out.writeInt(_routingHash);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _className = IOUtils.readRepetitiveString(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            _routingHash = in.readInt();
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeString(out, _className);
        out.writeInt(_routingHash);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _className = IOUtils.readString(in);
        _routingHash = in.readInt();
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        inReplicationHandler.inCancelEntryLeaseByUID(context, getTypeName(), getUid(), isTransient(), _routingHash);
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        return ReplicationOperationType.LEASE_EXPIRATION;
    }

    @Override
    protected int getFilterObjectType() {
        return ObjectTypes.ENTRY;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.CANCEL_LEASE;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public String toString() {
        return "CANCEL LEASE: (class name=" + getTypeName() + " uid=" + getUid() + ")";
    }

}
