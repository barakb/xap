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

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.sync.ConsolidationParticipantData;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SynchronizationSourceDetails;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

import java.util.Arrays;
import java.util.List;

/**
 * @author eitany
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class ConsolidationParticipantDataImpl
        implements ConsolidationParticipantData, SynchronizationSourceDetails {

    private final DataSyncOperation[] _dataItems;
    private final TransactionParticipantMetaData _metaData;
    private final IReplicationInContext _context;
    private boolean _aborted;

    public ConsolidationParticipantDataImpl(List<DataSyncOperation> dataItems, TransactionParticipantMetaData metaData, IReplicationInContext context) {
        _metaData = metaData;
        _context = context;
        _dataItems = dataItems.toArray(new DataSyncOperation[dataItems.size()]);
    }

    @Override
    public DataSyncOperation[] getTransactionParticipantDataItems() {
        return _dataItems;
    }

    @Override
    public TransactionParticipantMetaData getTransactionParticipantMetadata() {
        return _metaData;
    }

    @Override
    public void commit() {
        _aborted = false;
    }

    @Override
    public void abort() {
        _aborted = true;
    }

    public boolean isAborted() {
        return _aborted;
    }

    @Override
    public String toString() {
        return "ConsolidationParticipantData [metadata=" + getTransactionParticipantMetadata() + " data=" + Arrays.toString(getTransactionParticipantDataItems()) + " abort=" + isAborted() + "]";
    }

    @Override
    public SynchronizationSourceDetails getSourceDetails() {
        return this;
    }

    @Override
    public String getName() {
        return _context.getSourceLookupName();
    }

}
