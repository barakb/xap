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
import com.gigaspaces.internal.cluster.node.ReplicationInContentContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ITimeToLiveUpdateCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ReplicationFilterUidDataWrapper;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationFilterException;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public abstract class SingleUidReplicationPacketData extends AbstractReplicationPacketSingleEntryData {
    private static final long serialVersionUID = 1L;
    private String _uid;
    private OperationID _operationID;
    private boolean _transient;
    protected transient IEntryData _entryData;

    public SingleUidReplicationPacketData() {
    }

    public SingleUidReplicationPacketData(String uid,
                                          OperationID operationID, boolean isTransient, boolean fromGateway, IEntryData entryData) {
        super(fromGateway);
        _uid = uid;
        _operationID = operationID;
        _transient = isTransient;
        _entryData = entryData;
    }

    @Override
    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        try {
            executeImpl(context, inReplicationHandler);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null) {
                _entryData = contentContext.getMainEntryData();
                contentContext.clear();
            }
        }
    }

    protected abstract void executeImpl(IReplicationInContext context,
                                        IReplicationInFacade inReplicationHandler) throws Exception;

    @Override
    public SingleUidReplicationPacketData clone() {
        return (SingleUidReplicationPacketData) super.clone();
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        readExternalImpl(in);
        _transient = in.readBoolean();
    }

    private final void readExternalImpl(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _uid = IOUtils.readString(in);
        _operationID = IOUtils.readObject(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        readExternalImpl(in);
        _transient = in.readBoolean();
        _entryData = deserializeEntryData(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeExternalImpl(out);
        out.writeBoolean(_transient);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        writeExternalImpl(out);
        out.writeBoolean(_transient);
        serializeEntryData(_entryData, out);
    }

    private final void writeExternalImpl(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _uid);
        IOUtils.writeObject(out, _operationID);
    }

    public String getUid() {
        return _uid;
    }

    @Override
    protected IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = getTypeDescriptor(spaceTypeManager);
        ITypeDesc typeDesc = serverTypeDesc == null ? null : serverTypeDesc.getTypeDesc();
        return new ReplicationFilterUidDataWrapper(this, getUid(), typeDesc, getFilterOldReplicationOpType(), getFilterObjectType(), getTimeToLiveUpdateCallback());
    }

    protected IServerTypeDesc getTypeDescriptor(SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = null;
        final String className = getTypeName();
        if (className != null) {
            serverTypeDesc = spaceTypeManager.getServerTypeDesc(className);
            // can not be null since this entry was introduced already, maybe on
            // concurrent clean
            if (serverTypeDesc == null) {
                throw new ReplicationFilterException("Could not locate type descriptor for "
                        + className
                        + " while generating replication filter entry for the replication filter");
            }
        }

        return serverTypeDesc;
    }

    public int getOrderCode() {
        return getUid().hashCode();
    }

    public boolean isTransient() {
        return _transient;
    }

    @Override
    public IEntryData getMainEntryData() {
        return _entryData;
    }

    @Override
    public IEntryData getSecondaryEntryData() {
        return null;
    }

    protected abstract String getTypeName();

    protected abstract int getFilterObjectType();

    protected abstract ReplicationOperationType getFilterOldReplicationOpType();

    protected ITimeToLiveUpdateCallback getTimeToLiveUpdateCallback() {
        return null;
    }

    public OperationID getOperationId() {
        return _operationID;
    }

    @Override
    public boolean containsFullEntryData() {
        return false;
    }
}
