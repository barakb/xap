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

import com.gigaspaces.cluster.replication.async.mirror.MirrorStatisticsImpl;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInBatchConsumptionHandler;

/**
 * Handles batch consumption event sent to the mirror.
 *
 * @author idan
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorReplicationInBatchConsumptionHandler extends MirrorReplicationInHandler
        implements IReplicationInBatchConsumptionHandler {
    public MirrorReplicationInBatchConsumptionHandler(MirrorBulkExecutor bulkExecutor,
                                                      MirrorStatisticsImpl operationStatisticsHandler) {
        super(bulkExecutor, operationStatisticsHandler);
    }

    @Override
    public void consumePendingOperationsInBatch(
            IReplicationInBatchContext context) throws Exception {
        if (context.getPendingContext().isEmpty())
            context.currentConsumed();
        else
            execute(context, null);
    }

}
