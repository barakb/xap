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

package com.gigaspaces.internal.cluster.node.impl.replica.data;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaConsumeFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyIntermediateResult;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class DirectPersistencyEntryReplicaData extends AbstractEntryReplicaData {
    private static final long serialVersionUID = 1L;
    private String _uid;

    public DirectPersistencyEntryReplicaData() {
    }

    public DirectPersistencyEntryReplicaData(String uid) {
        _uid = uid;
        markEntryAsNotExists();
    }

    public DirectPersistencyEntryReplicaData(IEntryPacket entryPacket) {
        super(entryPacket);
        _uid = entryPacket.getUID();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(_uid);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _uid = in.readUTF();
    }

    public void markEntryAsNotExists() {
        setEntryPacket(null);
    }

    @Override
    public SpaceCopyReplicaParameters.ReplicaType getReplicaType() {
        return SpaceCopyReplicaParameters.ReplicaType.SYNCRONIZE;
    }

    @Override
    public String getUid() {
        return _uid;
    }

    public void execute(ISpaceReplicaConsumeFacade consumeFacade, SpaceCopyIntermediateResult intermediateResult, IIncomingReplicationFacade incomingReplicationFacade) throws Exception {
        try {
            if (getEntryPacket() != null) {
                consumeFacade.write(getEntryPacket(), getMarker(incomingReplicationFacade), getReplicaType());
                intermediateResult.increaseWritenTypeCount(getEntryPacket().getTypeName());
            }
            // getEntryPacket can be null only if direct persistency sync handler is enabled and the entry was not found in the primary space
            else {
                consumeFacade.remove(_uid, getMarker(incomingReplicationFacade), getReplicaType());
                //TODO consider adding "removed elements" to intermediateResult
            }
        } catch (EntryAlreadyInSpaceException e) {
            intermediateResult.addDuplicateEntry(getEntryPacket().getUID(), getEntryPacket().getTypeName());
        }
    }

    @Override
    protected IMarker getMarker(IIncomingReplicationFacade incomingReplicationFacade) {
        return null;
    }

    @Override
    public String toString() {
        if (getEntryPacket() != null) {
            return super.toString();
        }
        return "EntryReplicaData " + _uid;
    }

}