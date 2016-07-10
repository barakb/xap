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

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.handlers.UnknownNotifyTemplateLeaseException;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ITimeToLiveUpdateCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.NotifyReplicationFilterUidDataWrapper;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.lease.UnknownLeaseException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class ExtendNotifyTemplateLeaseReplicationPacketData
        extends SingleUidReplicationPacketData implements ITimeToLiveUpdateCallback {
    private static final long serialVersionUID = 1L;
    private String _className;
    private long _leaseTime;
    private transient long _expirationTime;
    private transient int _objectType;
    private transient NotifyInfo _notifyInfo;

    public ExtendNotifyTemplateLeaseReplicationPacketData() {
    }

    public ExtendNotifyTemplateLeaseReplicationPacketData(String className,
                                                          String uid, boolean isTransient, IEntryData entryData, long expirationTime, long leaseTime, int objectType,
                                                          NotifyInfo notifyInfo, OperationID operationID) {
        super(uid, operationID, isTransient, false, entryData);
        _className = className;
        _expirationTime = expirationTime;
        _leaseTime = leaseTime;
        _objectType = objectType;
        _notifyInfo = notifyInfo;
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        try {
            inReplicationHandler.inExtendNotifyTemplateLeasePeriod(context,
                    _className,
                    getUid(),
                    _leaseTime);
        } catch (UnknownLeaseException ex) {
            // This can occur if an extend lease replication was stuck in the
            // replication backlog
            // for a while and the entry had already expired in the target space
            // and the extend
            // lease was sent too late.
            throw new UnknownNotifyTemplateLeaseException(_className,
                    getUid(),
                    ex);
        }
    }

    public boolean beforeDelayedReplication() {
        // Calculate updated time to live before we send to replication target
        // in case
        // the replication occurred after a reasonable amount of time of the
        // actual extend entry
        // lease operation (disconnection, recovery)
        if (_expirationTime != Long.MAX_VALUE) {
            long updatedTimeToLive = _expirationTime - SystemTime.timeMillis();
            // If updated time to live is negative, this packet is no longer
            // relevant
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
        readExternalImpl(in);
    }

    private void readExternalImpl(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _className = IOUtils.readRepetitiveString(in);
        _leaseTime = in.readLong();
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        readExternalImpl(in);
        _expirationTime = in.readLong();
        _objectType = in.readInt();
        _notifyInfo = IOUtils.readNullableSwapExternalizableObject(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeExternalImpl(out);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        writeExternalImpl(out);
        out.writeLong(_expirationTime);
        out.writeInt(_objectType);
        IOUtils.writeNullableSwapExternalizableObject(out, _notifyInfo);
    }

    private void writeExternalImpl(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, _className);
        out.writeLong(_leaseTime);
    }

    @Override
    protected String getTypeName() {
        return _className;
    }

    @Override
    protected IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = getTypeDescriptor(spaceTypeManager);
        ITypeDesc typeDesc = serverTypeDesc == null ? null
                : serverTypeDesc.getTypeDesc();

        return new NotifyReplicationFilterUidDataWrapper(this,
                getUid(),
                typeDesc,
                getFilterOldReplicationOpType(),
                getFilterObjectType(),
                _notifyInfo,
                getTimeToLiveUpdateCallback());
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        return ReplicationOperationType.EXTEND_LEASE;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.EXTEND_NOTIFY_TEMPLATE_LEASE;
    }

    @Override
    protected int getFilterObjectType() {
        return _objectType;
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
        return "EXTEND NOTIFY TEMPLATE LEASE: (class name=" + getTypeName() + " uid=" + getUid() + " leaseTime=" + _leaseTime + ")";
    }

}
