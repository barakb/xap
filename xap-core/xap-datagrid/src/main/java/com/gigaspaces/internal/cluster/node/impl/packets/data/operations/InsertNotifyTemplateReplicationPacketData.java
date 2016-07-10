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
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.NotifyReplicationFilterEntryDataWrapper;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class InsertNotifyTemplateReplicationPacketData
        extends SingleReplicationPacketData {
    private static final long serialVersionUID = 1L;

    private NotifyInfo _notifyInfo;
    private String _templateUid;
    private transient long _expirationTime;


    public InsertNotifyTemplateReplicationPacketData() {
    }

    public InsertNotifyTemplateReplicationPacketData(
            ITemplatePacket templatePacket, String templateUid, NotifyInfo notifyInfo, long expirationTime) {
        super(templatePacket, false);
        _templateUid = templateUid;
        _notifyInfo = notifyInfo;
        _expirationTime = expirationTime;
    }

    public NotifyInfo getNotifyInfo() {
        return _notifyInfo;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        inReplicationHandler.inInsertNotifyTemplate(context,
                (ITemplatePacket) getEntryPacket(),
                _templateUid,
                getNotifyInfo());
    }

    public boolean beforeDelayedReplication() {
        //Calculate updated time to live before we send to replication target in case
        //the replication occurred after a reasonable amount of time of the actual insert notify
        //template operation (disconnection, recovery)
        if (_expirationTime != Long.MAX_VALUE) {
            long updatedTimeToLive = _expirationTime - SystemTime.timeMillis();
            //If updated time to live is negative, this packet is no longer relevant
            if (updatedTimeToLive <= 0)
                return false;

            getEntryPacket().setTTL(updatedTimeToLive);
        }
        return true;
    }

    @Override
    public String getUid() {
        return _templateUid;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _notifyInfo);
        IOUtils.writeString(out, _templateUid);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeNullableSwapExternalizableObject(out, _notifyInfo);
        IOUtils.writeString(out, _templateUid);
        out.writeLong(_expirationTime);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _notifyInfo = IOUtils.readObject(in);
        _templateUid = IOUtils.readString(in);
        _expirationTime = !getEntryPacket().isExternalizableEntryPacket() && getEntryPacket().getTTL() != Long.MAX_VALUE ? getEntryPacket().getTTL() + SystemTime.timeMillis() : Long.MAX_VALUE;
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _notifyInfo = IOUtils.readNullableSwapExternalizableObject(in);
        _templateUid = IOUtils.readString(in);
        _expirationTime = in.readLong();
    }

    @Override
    public IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = getTypeDescriptor(spaceTypeManager);
        ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();

        return new NotifyReplicationFilterEntryDataWrapper(this, getEntryPacket(),
                typeDesc, getFilterObjectType(serverTypeDesc), getNotifyInfo());
    }

    @Override
    protected int getFilterObjectType(IServerTypeDesc serverTypeDesc) {
        return serverTypeDesc.isRootType() ? ObjectTypes.NOTIFY_NULL_TEMPLATE : ObjectTypes.NOTIFY_TEMPLATE;
    }

    @Override
    protected ReplicationOperationType getFilterReplicationOpType() {
        return ReplicationOperationType.NOTIFY;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.INSERT_NOTIFY_TEMPLATE;
    }

    @Override
    public String toString() {
        return "INSERT NOTIFY TEMPLATE: " + getEntryPacket();
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return false;
    }
}
