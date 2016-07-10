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

package com.gigaspaces.internal.cluster.node.handlers;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
public interface IReplicationInEntryHandler {

    void inWriteEntry(IReplicationInContext context, IEntryPacket entryPacket) throws Exception;

    void inUpdateEntry(IReplicationInContext context, IEntryPacket entryPacket, IEntryPacket oldEntryPacket,
                       boolean partialUpdate, boolean overrideVersion, short flags) throws Exception;

    void inChangeEntry(IReplicationInContext context, String typeName, String uid, Object id, int version,
                       int previousVersion, int routingHash, long timeToLive, Collection<SpaceEntryMutator> mutators,
                       boolean isTransient, OperationID operationID) throws Exception;

    void inRemoveEntry(IReplicationInContext context, IEntryPacket entryPacket) throws Exception;

    void inRemoveEntryByUID(IReplicationInContext context, String typeName, String uid, boolean isTransient,
                            OperationID operationID) throws Exception;

    void afterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey);

    void beforeConsume(IReplicationInContext context);
}
