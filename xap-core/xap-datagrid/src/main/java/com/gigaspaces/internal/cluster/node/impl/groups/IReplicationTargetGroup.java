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

import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceReplicaState;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;

import java.util.List;


/**
 * Handle incoming replication from one or more {@link IReplicationSourceGroup}
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationTargetGroup {
    /**
     * Close this replication group, once closed it can no longer be used
     */
    void close();

    /**
     * Connect a {@link IReplicationSourceChannel} to this group. The success or failure of this
     * operation is indicated by the result {@link IProcessLogHandshakeResponse}
     *
     * @param handshakeRequest the handshake related details
     * @return The success or failure of this operation is indicated by the result {@link
     * IProcessLogHandshakeResponse}
     */
    ConnectChannelHandshakeResponse connectChannel(RouterStubHolder sourceRouterStubHolder,
                                                   ConnectChannelHandshakeRequest handshakeRequest);

    void onChannelBacklogDropped(String sourceMemberLookupName,
                                 Object sourceUniqueId, IBacklogMemberState memberState);

    void processHandshakeIteration(String sourceMemberLookupName,
                                   Object sourceUniqueId, IHandshakeIteration handshakeIteration);

    Object processBatch(String sourceMemberLookupName,
                        Object sourceUniqueId, List<IReplicationOrderedPacket> packets);

    Object process(String sourceMemberLookupName,
                   Object sourceUniqueId, IReplicationOrderedPacket packet);

    void processUnreliableOperation(String sourceMemberLookupName,
                                    Object sourceUniqueId,
                                    IReplicationUnreliableOperation operation);

    Object processIdleStateData(String sourceMemberLookupName,
                                Object sourceUniqueId, IIdleStateData idleStateData);

    void addSynchronizeState(String sourceMemberLookupName, SpaceReplicaState result);

    void synchronizationDone(String sourceMemberLookupName, Object sourceUniqueId);

    long getLastProcessTimeStamp(String replicaSourceLookupName);

    String getGroupName();

    void setActive();

    void setPassive();

    String dumpState();

    IReplicationTargetChannel getChannel(String sourceMemberLookupName);


}
