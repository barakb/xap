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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.cluster.replication.async.mirror.MirrorStatisticsImpl;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryHandler;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import java.util.Collection;

/**
 * Handles replicated entry operations on mirror
 *
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorReplicationInEntryHandler extends MirrorReplicationInHandler implements IReplicationInEntryHandler {
    public MirrorReplicationInEntryHandler(MirrorBulkExecutor bulkExecutor,
                                           MirrorStatisticsImpl operationStatisticsHandler) {
        super(bulkExecutor, operationStatisticsHandler);
    }

    @Override
    public void inWriteEntry(IReplicationInContext context, IEntryPacket entryPacket) throws Exception {
        addWriteEntry((IReplicationInBatchContext) context, entryPacket);
    }

    @Override
    public void inUpdateEntry(IReplicationInContext context, IEntryPacket entryPacket, IEntryPacket oldEntryPacket, boolean partialUpdate,
                              boolean overrideVersion, short flags) throws Exception {
        getTypeManager().loadServerTypeDesc(entryPacket);
        addUpdateEntry((IReplicationInBatchContext) context, entryPacket, oldEntryPacket, partialUpdate);
    }

    @Override
    public void inRemoveEntry(IReplicationInContext context, IEntryPacket entryPacket) throws Exception {
        addRemoveEntry((IReplicationInBatchContext) context, entryPacket);
    }

    @Override
    public void inRemoveEntryByUID(IReplicationInContext context, String typeName, String uid, boolean isTransient,
                                   OperationID operationID) throws Exception {
        throw new UnsupportedOperationException("Remove by uid is not supported by mirror");
    }

    @Override
    public void afterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey) {
    }

    @Override
    public void beforeConsume(IReplicationInContext context) {
    }

    @Override
    public void inChangeEntry(IReplicationInContext context, String typeName,
                              String uid, Object id, int version, int previousVersion,
                              int routingHash, long timeToLive,
                              Collection<SpaceEntryMutator> mutators, boolean isTransient, OperationID operationID) throws Exception {
        addChangeEntry((IReplicationInBatchContext) context, typeName, uid, id, version, timeToLive, mutators);
    }
}
