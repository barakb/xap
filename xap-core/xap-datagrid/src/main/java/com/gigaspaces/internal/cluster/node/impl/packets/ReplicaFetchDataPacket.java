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

package com.gigaspaces.internal.cluster.node.impl.packets;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaData;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractReplicationPacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

@com.gigaspaces.api.InternalApi
public class ReplicaFetchDataPacket
        extends AbstractReplicationPacket<Collection<ISpaceReplicaData>> {

    private static final long serialVersionUID = 1L;

    private Object _replicaRemoteContext;
    private int _fetchBatchSize;

    public ReplicaFetchDataPacket() {
    }

    public ReplicaFetchDataPacket(
            Object replicaRemoteContext, int fetchBatchSize) {
        _replicaRemoteContext = replicaRemoteContext;
        _fetchBatchSize = fetchBatchSize;
    }

    public Collection<ISpaceReplicaData> accept(IIncomingReplicationFacade incomingReplicationFacade) {
        return incomingReplicationFacade.getNextReplicaBatch(_replicaRemoteContext, _fetchBatchSize);
    }

    public void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _replicaRemoteContext = in.readObject();
        _fetchBatchSize = in.readInt();
    }

    public void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        out.writeObject(_replicaRemoteContext);
        out.writeInt(_fetchBatchSize);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("context", _replicaRemoteContext);
        textualizer.append("batchSize", _fetchBatchSize);
    }

}
