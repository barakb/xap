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

package com.gigaspaces.internal.cluster.node.impl.processlog;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;

import java.util.List;
import java.util.concurrent.TimeUnit;


public interface IReplicationTargetProcessLog {
    IProcessLogHandshakeResponse performHandshake(String memberName,
                                                  IBacklogHandshakeRequest handshakeRequest) throws IncomingReplicationOutOfSyncException;

    IProcessResult processBatch(String sourceLookupName,
                                List<IReplicationOrderedPacket> packet,
                                IReplicationInFilterCallback inFilterCallback);

    IProcessResult process(String sourceLookupName,
                           IReplicationOrderedPacket packet,
                           IReplicationInFilterCallback inFilterCallback);

    boolean close(long time, TimeUnit unit) throws InterruptedException;

    <T extends IReplicationPacketData<?>> IReplicationPacketDataConsumer<T> getDataConsumer();

    void processHandshakeIteration(String sourceMemberName,
                                   IHandshakeIteration handshakeIteration);

    IProcessLogHandshakeResponse resync(IBacklogHandshakeRequest handshakeRequest);

    String dumpState();

    Object toWireForm(IProcessResult processResult);

    String toLogMessage();

    IProcessResult processIdleStateData(String sourceLookupName, IIdleStateData idleStateData,
                                        IReplicationInFilterCallback inFilterCallback);

}
