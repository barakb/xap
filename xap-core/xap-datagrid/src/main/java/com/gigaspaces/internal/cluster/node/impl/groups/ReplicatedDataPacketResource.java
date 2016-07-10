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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.packets.BatchReplicatedDataPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IdleStateDataReplicatedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.ReplicatedDataPacket;
import com.gigaspaces.internal.utils.threadlocal.AbstractResource;

@com.gigaspaces.api.InternalApi
public class ReplicatedDataPacketResource extends AbstractResource {
    private final BatchReplicatedDataPacket _batchPacket;
    private final ReplicatedDataPacket _packet;
    private final IdleStateDataReplicatedPacket _idleStateDataPacket;

    public ReplicatedDataPacketResource(String groupName) {
        _batchPacket = new BatchReplicatedDataPacket(groupName);
        _packet = new ReplicatedDataPacket(groupName);
        _idleStateDataPacket = new IdleStateDataReplicatedPacket(groupName);
    }

    @Override
    protected void clean() {
        _batchPacket.clean();
        _packet.clean();
        _idleStateDataPacket.clean();
    }

    public BatchReplicatedDataPacket getBatchPacket() {
        return _batchPacket;
    }

    public ReplicatedDataPacket getPacket() {
        return _packet;
    }

    public IdleStateDataReplicatedPacket getIdleStateDataPacket() {
        return _idleStateDataPacket;
    }

}
