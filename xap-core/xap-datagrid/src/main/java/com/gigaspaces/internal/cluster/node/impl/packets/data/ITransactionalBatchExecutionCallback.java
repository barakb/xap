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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import java.util.Collection;

public interface ITransactionalBatchExecutionCallback {
    void writeEntry(IReplicationInBatchContext context,
                    IEntryPacket entryPacket) throws Exception;

    void removeEntry(IReplicationInBatchContext context,
                     IEntryPacket entryPacket) throws Exception;

    void removeEntryByUID(IReplicationInBatchContext context,
                          String typeName, String uid, OperationID operationID) throws Exception;

    void updateEntry(IReplicationInBatchContext context,
                     IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate, short flags) throws Exception;

    void changeEntry(IReplicationInBatchContext context, String typeName,
                     String uid, Object id, int version, int previousVersion,
                     long timeToLive,
                     int routingHash, Collection<SpaceEntryMutator> spaceEntryMutators, boolean isTransient, OperationID operationID) throws Exception;
}
