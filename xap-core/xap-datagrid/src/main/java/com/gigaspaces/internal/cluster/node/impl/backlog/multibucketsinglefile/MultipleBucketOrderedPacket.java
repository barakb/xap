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
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.IMultiBucketSingleFileProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.ParallelBatchProcessingContext;
import com.gigaspaces.internal.collections.ShortObjectMap;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class MultipleBucketOrderedPacket extends AbstractMultiBucketOrderedPacket {
    private static final long serialVersionUID = 1L;

    private IReplicationPacketData<?> _data;
    private transient short _pendingBucketsCount;
    private transient boolean _consumed;

    public MultipleBucketOrderedPacket() {
    }

    public MultipleBucketOrderedPacket(long globalKey,
                                       ShortObjectMap<BucketKey> bucketsKeys, IReplicationPacketData<?> data) {
        super(globalKey, bucketsKeys);
        _data = data;
        _pendingBucketsCount = (short) getBucketsKeys().size();
    }

    public IReplicationPacketData<?> getData() {
        return _data;
    }

    public boolean isDataPacket() {
        return true;
    }

    @Override
    public boolean isDiscardedPacket() {
        return false;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _data);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeSwapExternalizableObject(out, _data);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _pendingBucketsCount = bucketCount();
        _data = IOUtils.readObject(in);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _pendingBucketsCount = bucketCount();
        _data = IOUtils.readSwapExternalizableObject(in);
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IMultiBucketSingleFileProcessLog processLog,
                                                      IReplicationInFilterCallback inFilterCallback, ParallelBatchProcessingContext context, int segmentIndex) {
        return processLog.process(sourceLookupName, this, inFilterCallback, context, segmentIndex);
    }

    @Override
    public MultipleBucketOrderedPacket clone() {
        return new MultipleBucketOrderedPacket(getKey(), getBucketsKeys(), _data.clone());
    }

    @Override
    public MultipleBucketOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        return new MultipleBucketOrderedPacket(getKey(), getBucketsKeys(), newData);
    }


    public PacketConsumeState getConsumeState(short bucketIndex) {
        if (_consumed)
            return PacketConsumeState.CONSUMED;

        BucketKey bucketKey = getBucketsKeys().get(bucketIndex);
        boolean alreadyReachedThisBucket = bucketKey.processed;
        bucketKey.processed = true;

        if (!alreadyReachedThisBucket) {
            synchronized (this) {
                return (--_pendingBucketsCount == 0) ? PacketConsumeState.CAN_CONSUME : PacketConsumeState.PENDING;
            }
        } else
            return PacketConsumeState.PENDING;
    }

    public boolean setConsumed() {
        _consumed = true;
        //This packet logic is preventing from two threads to consume it since
        //get consume state will return CAN_CONSUME only to one thread, so we can
        //return true since it is always first consumed at this point
        return true;
    }

    public AbstractMultiBucketOrderedPacket replaceWithDiscarded() {
        return new DiscardedMultiBucketOrderedPacket(getKey(), getBucketsKeys());
    }

}
