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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.IMultiBucketSingleFileProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.ParallelBatchProcessingContext;
import com.gigaspaces.internal.collections.ShortObjectMap;

import java.util.List;

public interface IMultiBucketSingleFileReplicationOrderedPacket
        extends IReplicationOrderedPacket {

    MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                               IMultiBucketSingleFileProcessLog processLog,
                                               IReplicationInFilterCallback inFilterCallback, ParallelBatchProcessingContext context, int segmentIndex);

    long processResult(String memberName,
                       MultiBucketSingleFileProcessResult processResult,
                       MultiBucketSingleFileConfirmationHolder confirmationHolder);

    long getBucketKey(short bucketIndex);

    PacketConsumeState getConsumeState(short bucketIndex);

    boolean setConsumed();

    short bucketCount();

    short[] getBuckets();

    void associateToBuckets(ShortObjectMap<List<IMultiBucketSingleFileReplicationOrderedPacket>> bucketsPackets);

    IMultiBucketSingleFileReplicationOrderedPacket replaceWithDiscarded();

    IMultiBucketSingleFileReplicationOrderedPacket clone();

    IMultiBucketSingleFileReplicationOrderedPacket cloneWithNewData(IReplicationPacketData<?> newData);

    void reliableAsyncKeysUpdate(long[] bucketLastKeys,
                                 MultiBucketSingleFileConfirmationHolder confirmationHolder);

    IMultiBucketSingleFileReplicationOrderedPacket getReliableAsyncBeforeFilterPacket();

    void setReliableAsyncBeforeFilterPacket(IMultiBucketSingleFileReplicationOrderedPacket beforeFilterPacket);
}
