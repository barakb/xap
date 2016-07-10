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
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class EvictReplicationPacketData
        extends SingleUidReplicationPacketData {
    private static final long serialVersionUID = 1L;
    private String _className;

    public EvictReplicationPacketData() {
    }

    public EvictReplicationPacketData(String className, String uid,
                                      boolean isTransient, IEntryData entryData, OperationID operationID) {
        super(uid, operationID, isTransient, false, entryData);
        _className = className;
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
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _className = IOUtils.readString(in);
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        inReplicationHandler.inEvictEntry(context, getUid(), isTransient(), getOperationId());

    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        return ReplicationOperationType.EVICT;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.EVICT;
    }

    @Override
    protected int getFilterObjectType() {
        return ObjectTypes.ENTRY;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public String toString() {
        return "EVICT ENTRY: (class name=" + getTypeName() + " uid=" + getUid() + ")";
    }

}
