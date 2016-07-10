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
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;


/**
 * Handles replicated transaction on mirror in batches. Several transaction are aggregated into a
 * single transaction for better performance.
 *
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorReplicationInAggregatedTransactionHandler extends MirrorReplicationInHandler
        implements IReplicationInTransactionHandler {
    public MirrorReplicationInAggregatedTransactionHandler(MirrorBulkExecutor bulkExecutor, MirrorStatisticsImpl operationStatisticsHandler) {
        super(bulkExecutor, operationStatisticsHandler);
    }

    public void inTransaction(IReplicationInContext context,
                              ITransactionInContext transactionContext) throws Exception {
        IReplicationInBatchContext batchContext = (IReplicationInBatchContext) context;

        // add the given transaction to the batch operations
        addTransaction(batchContext, transactionContext);
    }

    @Override
    public void inTransactionPrepare(IReplicationInContext context,
                                     ITransactionInContext transactionContext) throws Exception {
        //Should not reach here, we convert this to one phase at the source
        throw new UnsupportedOperationException();
    }

    @Override
    public void inTransactionCommit(IReplicationInContext context,
                                    ITransactionInContext transactionContext) throws Exception {
        //Should not reach here, we convert this to one phase at the source
        throw new UnsupportedOperationException();
    }

    @Override
    public void inTransactionAbort(IReplicationInContext context,
                                   ITransactionInContext transactionContext) throws Exception {
        //Should not reach here, we convert this to one phase at the source
        throw new UnsupportedOperationException();
    }

}
