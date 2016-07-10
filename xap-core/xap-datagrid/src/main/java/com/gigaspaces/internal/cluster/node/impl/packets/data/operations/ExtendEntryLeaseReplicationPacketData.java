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
import com.gigaspaces.internal.cluster.node.impl.handlers.UnknownEntryLeaseException;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ITimeToLiveUpdateCallback;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.lease.UnknownLeaseException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class ExtendEntryLeaseReplicationPacketData
        extends SingleUidReplicationPacketData implements ITimeToLiveUpdateCallback {
    private static final long serialVersionUID = 1L;
    private String _className;
    private long _leaseTime;
    private int _routingHash;
    private transient long _expirationTime;

    public ExtendEntryLeaseReplicationPacketData() {
    }

    public ExtendEntryLeaseReplicationPacketData(String className, String uid,
                                                 boolean isTransient, IEntryData entryData, long expirationTime, long leaseTime
            , OperationID operationID, int routingHash, boolean isFromGateway) {
        super(uid, operationID, isTransient, isFromGateway, entryData);
        _className = className;
        _expirationTime = expirationTime;
        _leaseTime = leaseTime;
        _routingHash = routingHash;
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        try {
            inReplicationHandler.inExtendEntryLeasePeriod(context, _className, getUid(), isTransient(), _leaseTime, _routingHash);
        } catch (UnknownLeaseException ex) {
            //This can occur if an extend lease replication was stuck in the replication backlog
            //for a while and the entry had already expired in the target space and the extend
            //lease was sent too late.
            throw new UnknownEntryLeaseException(_className, getUid(), getOperationId(), ex);
        }
    }

    public boolean beforeDelayedReplication() {
        //Calculate updated time to live before we send to replication target in case
        //the replication occurred after a reasonable amount of time of the actual extend entry
        //lease operation (disconnection, recovery)
        if (_expirationTime != Long.MAX_VALUE) {
            long updatedTimeToLive = _expirationTime - SystemTime.timeMillis();
            //If updated time to live is negative, this packet is no longer relevant
            if (updatedTimeToLive <= 0)
                return false;
            _leaseTime = updatedTimeToLive;
        }
        return true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _className = IOUtils.readRepetitiveString(in);
        _leaseTime = in.readLong();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            _routingHash = in.readInt();
        _expirationTime = _leaseTime != Long.MAX_VALUE ? _leaseTime + SystemTime.timeMillis() : Long.MAX_VALUE;
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _className = IOUtils.readRepetitiveString(in);
        _leaseTime = in.readLong();
        _routingHash = in.readInt();
        _expirationTime = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeRepetitiveString(out, _className);
        out.writeLong(_leaseTime);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            out.writeInt(_routingHash);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeRepetitiveString(out, _className);
        out.writeLong(_leaseTime);
        out.writeInt(_routingHash);
        out.writeLong(_expirationTime);
    }

    @Override
    protected String getTypeName() {
        return _className;
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        return ReplicationOperationType.EXTEND_LEASE;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.EXTEND_ENTRY_LEASE;
    }

    @Override
    protected int getFilterObjectType() {
        return ObjectTypes.ENTRY;
    }

    @Override
    protected ITimeToLiveUpdateCallback getTimeToLiveUpdateCallback() {
        return this;
    }

    public void updateTimeToLive(long newTimeToLive) {
        _leaseTime = newTimeToLive;
    }

    public long getTimeToLive() {
        return _leaseTime;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public String toString() {
        return "EXTEND ENTRY LEASE: (class name=" + getTypeName() + " uid=" + getUid() + " leaseTime=" + _leaseTime + ")";
    }

}
