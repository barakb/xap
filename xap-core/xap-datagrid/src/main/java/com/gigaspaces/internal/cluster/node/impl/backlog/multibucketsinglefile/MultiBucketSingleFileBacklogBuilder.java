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

import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.async.IReplicationAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IReplicationSyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;


@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileBacklogBuilder
        implements IReplicationBacklogBuilder {
    private final IReplicationPacketDataProducer<?> _dataProducer;
    private final String _name;

    public MultiBucketSingleFileBacklogBuilder(String name, IReplicationPacketDataProducer<?> dataProducer) {
        _name = name;
        _dataProducer = dataProducer;
    }

    public IReplicationSyncGroupBacklog createSyncGroupBacklog(
            DynamicSourceGroupConfigHolder groupConfig) {
        return new MultiBucketSingleFileSyncGroupBacklog(groupConfig, _name, _dataProducer);
    }

    public IReplicationAsyncGroupBacklog createAsyncGroupBacklog(
            DynamicSourceGroupConfigHolder groupConfig) {
        throw new UnsupportedOperationException();
    }

    public IReplicationReliableAsyncGroupBacklog createReliableAsyncGroupBacklog(
            DynamicSourceGroupConfigHolder groupConfig) {
        return new MultiBucketSingleFileReliableAsyncGroupBacklog(groupConfig, _name, _dataProducer);
    }

    @Override
    public String toString() {
        return "MultiBucketSingleFileBacklogBuilder [_dataProducer="
                + _dataProducer + ", _name=" + _name + "]";
    }

}
