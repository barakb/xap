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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;


@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileSyncTargetProcessLog
        extends AbstractMultiBucketSingleFileTargetProcessLog
        implements IReplicationSyncTargetProcessLog {
    public MultiBucketSingleFileSyncTargetProcessLog(ProcessLogConfig config,
                                                     IReplicationPacketDataConsumer<?> dataConsumer,
                                                     IReplicationProcessLogExceptionHandler exceptionHandler,
                                                     IReplicationInFacade replicationInFacade, String name,
                                                     String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        super(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                groupHistory);
    }

    public MultiBucketSingleFileSyncTargetProcessLog(ProcessLogConfig config,
                                                     IReplicationPacketDataConsumer dataConsumer,
                                                     IReplicationProcessLogExceptionHandler exceptionHandler,
                                                     IReplicationInFacade replicationInFacade, String name,
                                                     String groupName, String sourceLookupName,
                                                     long[] lastProcessedKeys, long[] lastGlobalProcessedKeys,
                                                     boolean firstHandshakeForTarget,
                                                     IReplicationGroupHistory groupHistory) {
        super(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKeys,
                lastGlobalProcessedKeys,
                firstHandshakeForTarget,
                groupHistory);
    }

    @Override
    protected boolean contentRequiredWhileProcessing() {
        return false;
    }
}
