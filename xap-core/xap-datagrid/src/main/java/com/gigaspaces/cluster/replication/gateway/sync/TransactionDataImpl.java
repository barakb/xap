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

package com.gigaspaces.cluster.replication.gateway.sync;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SynchronizationSourceDetails;
import com.gigaspaces.sync.TransactionData;
import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

/**
 * @author idan
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class TransactionDataImpl
        implements TransactionData, SynchronizationSourceDetails {

    private final ITransactionInContext _transactionContext;
    private final IReplicationInBatchContext _batchContext;

    public TransactionDataImpl(IReplicationInBatchContext batchContext, ITransactionInContext transactionContext) {
        this._batchContext = batchContext;
        this._transactionContext = transactionContext;
    }

    @Override
    public boolean isConsolidated() {
        return !_transactionContext.getMetaData().isUnconsoliated();
    }

    @Override
    public TransactionParticipantMetaData getTransactionParticipantMetaData() {
        return _transactionContext.getMetaData();
    }

    @Override
    public ConsolidatedDistributedTransactionMetaData getConsolidatedDistributedTransactionMetaData() {
        if (!isConsolidated())
            throw new IllegalStateException("Cannot get consolidated distributed transaction meta data for an unconsolidated transaction");
        return _transactionContext.getMetaData();
    }

    @Override
    public DataSyncOperation[] getTransactionParticipantDataItems() {
        final DataSyncOperation[] operations = new DataSyncOperation[_batchContext.getPendingContext().size()];
        return _batchContext.getPendingContext().toArray(operations);
    }

    @Override
    public SynchronizationSourceDetails getSourceDetails() {
        return this;
    }

    @Override
    public String getName() {
        return _batchContext.getSourceLookupName();
    }

}
