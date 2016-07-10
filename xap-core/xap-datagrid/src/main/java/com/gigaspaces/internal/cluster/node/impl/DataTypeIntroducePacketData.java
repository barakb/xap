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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class DataTypeIntroducePacketData
        extends AbstractReplicationPacketSingleEntryData {
    private static final long serialVersionUID = 1L;
    private ITypeDesc _typeDescriptor;

    public DataTypeIntroducePacketData() {
    }

    public DataTypeIntroducePacketData(ITypeDesc typeDescriptor, boolean fromGateway) {
        super(fromGateway);
        _typeDescriptor = typeDescriptor;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.DATA_TYPE_INTRODUCE;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        inReplicationHandler.inDataTypeIntroduce(context, _typeDescriptor);
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    public boolean supportsReplicationFilter() {
        return false;
    }

    public boolean requiresRecoveryDuplicationProtection() {
        return false;
    }

    @Override
    public IEntryData getMainEntryData() {
        return null;
    }

    @Override
    public IEntryData getSecondaryEntryData() {
        return null;
    }

    @Override
    public String getMainTypeName() {
        return _typeDescriptor.getTypeName();
    }

    @Override
    protected IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        throw new UnsupportedOperationException();
    }

    public String getUid() {
        return _typeDescriptor.getTypeName();
    }

    public int getOrderCode() {
        return 0;
    }

    public boolean isTransient() {
        return false;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _typeDescriptor);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _typeDescriptor = IOUtils.readObject(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeNullableSwapExternalizableObject(out, _typeDescriptor);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _typeDescriptor = IOUtils.readNullableSwapExternalizableObject(in);
    }

    @Override
    public String toString() {
        return "DATA TYPE INTRODUCTION: " + _typeDescriptor;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return false;
    }

    @Override
    public boolean containsFullEntryData() {
        return true;
    }

}
