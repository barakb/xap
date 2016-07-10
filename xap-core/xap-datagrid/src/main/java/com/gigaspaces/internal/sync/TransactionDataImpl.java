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

package com.gigaspaces.internal.sync;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SynchronizationSourceDetails;
import com.gigaspaces.sync.TransactionData;
import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

import net.jini.core.transaction.server.TransactionParticipantDataImpl;

/**
 * @author idan
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class TransactionDataImpl
        implements TransactionData {
    private final DataSyncOperation[] _operations;
    private final TransactionParticipantDataImpl _metaData;
    private String _sourceName;

    public TransactionDataImpl(DataSyncOperation[] operations,
                               TransactionParticipantDataImpl metaData, String sourceName) {
        _operations = operations;
        _metaData = metaData;
        _sourceName = sourceName;
    }

    @Override
    public boolean isConsolidated() {
        return !_metaData.isUnconsoliated();
    }

    @Override
    public TransactionParticipantMetaData getTransactionParticipantMetaData() {
        return _metaData;
    }

    @Override
    public ConsolidatedDistributedTransactionMetaData getConsolidatedDistributedTransactionMetaData() {
        if (!isConsolidated())
            throw new IllegalStateException("Cannot get consolidated distributed transaction meta data for an unconsolidated transaction");
        return _metaData;
    }

    @Override
    public DataSyncOperation[] getTransactionParticipantDataItems() {
        return _operations;
    }

    @Override
    public SynchronizationSourceDetails getSourceDetails() {
        return new SynchronizationSourceDetails() {
            @Override
            public String getName() {
                return _sourceName;
            }
        };
    }

}
