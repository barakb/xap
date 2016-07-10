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
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.NotifyReplicationFilterUidDataWrapper;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class RemoveNotifyTemplateReplicationPacketData
        extends SingleUidReplicationPacketData {
    private static final long serialVersionUID = 1L;
    private String _className;
    private transient NotifyInfo _notifyInfo;
    private transient int _objectType;

    public RemoveNotifyTemplateReplicationPacketData() {
    }

    public RemoveNotifyTemplateReplicationPacketData(String className,
                                                     NotifyInfo notifyInfo, int objectType, String templateUid,
                                                     boolean isTransient, IEntryData entryData, OperationID operationID) {
        super(templateUid, operationID, isTransient, false, entryData);
        _className = className;
        _notifyInfo = notifyInfo;
        _objectType = objectType;
    }

    public String getTypeName() {
        return _className;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeRepetitiveString(out, _className);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _className = IOUtils.readRepetitiveString(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeString(out, _className);
        out.writeInt(_objectType);
        IOUtils.writeNullableSwapExternalizableObject(out, _notifyInfo);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _className = IOUtils.readString(in);
        _objectType = in.readInt();
        _notifyInfo = IOUtils.readNullableSwapExternalizableObject(in);
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        inReplicationHandler.inRemoveNotifyTemplate(context,
                getTypeName(),
                getUid());
    }

    public boolean beforeDelayedReplication() {
        return true;
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
                null);
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        // Backward, this is how remove notify template is passed to filter
        return ReplicationOperationType.LEASE_EXPIRATION;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.REMOVE_NOTIFY_TEMPLATE;
    }

    @Override
    protected int getFilterObjectType() {
        return _objectType;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public String toString() {
        return "REMOVE NOTIFY TEMPLATE: (class name=" + getTypeName() + " uid=" + getUid() + ")";
    }

}
