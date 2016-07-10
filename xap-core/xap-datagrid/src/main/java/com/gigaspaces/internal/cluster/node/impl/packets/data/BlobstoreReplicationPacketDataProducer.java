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

import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;

/**
 * @author Boris
 * @since 11.0.0 Wraps replication packets with blobstore bulk id when needed before sending them to
 * replication
 */
@com.gigaspaces.api.InternalApi
public class BlobstoreReplicationPacketDataProducer extends ReplicationPacketDataProducer {

    public BlobstoreReplicationPacketDataProducer(SpaceEngine spaceEngine,
                                                  boolean replicateToTargetWithExternalDatasource,
                                                  boolean replicateFullTake, ReplicationPacketDataMediator packetDataMediator) {
        super(spaceEngine, replicateToTargetWithExternalDatasource, replicateFullTake, packetDataMediator);
    }

    public IExecutableReplicationPacketData<?> createSingleOperationData(
            IEntryHolder entryHolder,
            ReplicationSingleOperationType operationType,
            ReplicationOutContext replicationOutContext) {
        final IExecutableReplicationPacketData<?> singleOperationData = super.createSingleOperationData(entryHolder, operationType, replicationOutContext);
        if (replicationOutContext.getBlobstoreReplicationBulkId() != 0) {
            return wrapBlobstoreBulkOperationData(singleOperationData, replicationOutContext);
        }
        return singleOperationData;
    }

    private IExecutableReplicationPacketData<?> wrapBlobstoreBulkOperationData(IExecutableReplicationPacketData<?> singleOperationData, ReplicationOutContext replicationOutContext) {
        ((AbstractReplicationPacketSingleEntryData) singleOperationData).setBlobstoreBulkId(replicationOutContext.getBlobstoreReplicationBulkId());
        return singleOperationData;
    }
}
