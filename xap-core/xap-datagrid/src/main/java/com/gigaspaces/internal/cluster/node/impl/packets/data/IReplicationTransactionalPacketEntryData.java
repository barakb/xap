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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import net.jini.core.transaction.Transaction;

public interface IReplicationTransactionalPacketEntryData extends IReplicationPacketEntryData {

    IReplicationFilterEntry toFilterEntry(SpaceTypeManager spaceTypeManager);

    void executeTransactional(IReplicationInContext context,
                              ITransactionalExecutionCallback transactionExecutionCallback, Transaction transaction, boolean twoPhaseCommit) throws Exception;

    void batchExecuteTransactional(IReplicationInBatchContext context, ITransactionalBatchExecutionCallback executionCallback) throws Exception;

    IReplicationTransactionalPacketEntryData clone();

    /**
     * If the replication of this packet is delayed due to being asynchronous by nature or the
     * target was disconnected when the replication packet was generated, this method is called
     * before the actual replication and the implementor can modify the packet accordingly, mostly
     * relevant for time to live update. If the method returns false the packet is no longer
     * relevant and it is discarded
     */
    boolean beforeDelayedReplication();

}
