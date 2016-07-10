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

package com.gigaspaces.internal.datasource;

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.SpaceSynchronizationEndpointException;
import com.gigaspaces.sync.TransactionData;
import com.j_spaces.sadapter.datasource.BulkDataPersisterContext;
import com.j_spaces.sadapter.datasource.DataStorage;

import net.jini.core.transaction.server.TransactionParticipantDataImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author idan
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class EDSAdapterSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    private final DataStorage<Object> _dataStorage;
    private final SpaceEngine _spaceEngine;

    public EDSAdapterSynchronizationEndpoint(
            SpaceEngine spaceEngine, DataStorage<Object> dataStorage) {
        this._spaceEngine = spaceEngine;
        this._dataStorage = dataStorage;
    }

    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        try {
            if (_dataStorage.isBulkDataPersister()) {
                try {
                    BulkDataPersisterContext.setContext(new BulkDataPersisterContext((TransactionParticipantDataImpl) transactionData.getTransactionParticipantMetaData(),
                            _spaceEngine.getFullSpaceName()));
                    _dataStorage.executeBulk(convertToBulkItemsList(transactionData.getTransactionParticipantDataItems()));
                } finally {
                    BulkDataPersisterContext.resetContext();
                }
            } else if (_dataStorage.isDataPersister()) {
                executeOperationsOneByOne(transactionData.getTransactionParticipantDataItems());
            }
        } catch (DataSourceException e) {
            throw new SpaceSynchronizationEndpointException(e);
        }
    }

    private void executeOperationsOneByOne(DataSyncOperation[] operations)
            throws DataSourceException {
        for (DataSyncOperation operation : operations) {
            final BulkItem bulkItem = (BulkItem) operation;
            switch (operation.getDataSyncOperationType()) {
                case WRITE:
                    _dataStorage.write(bulkItem.getItem());
                    break;
                case UPDATE:
                    _dataStorage.update(bulkItem.getItem());
                    break;
                case PARTIAL_UPDATE:
                    // in older implementation this is not supported and just ignored.
                    break;
                case REMOVE:
                    _dataStorage.remove(bulkItem.getItem());
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    private List<BulkItem> convertToBulkItemsList(
            DataSyncOperation[] operations) {
        final ArrayList<BulkItem> bulkItems = new ArrayList<BulkItem>(operations.length);
        for (DataSyncOperation operation : operations)
            bulkItems.add((BulkItem) operation);
        return bulkItems;
    }

    @Override
    public void onOperationsBatchSynchronization(
            OperationsBatchData batchData) {
        try {
            if (_dataStorage.isBulkDataPersister()) {
                _dataStorage.executeBulk(convertToBulkItemsList(batchData.getBatchDataItems()));
            } else if (_dataStorage.isDataPersister()) {
                executeOperationsOneByOne(batchData.getBatchDataItems());
            }
        } catch (DataSourceException e) {
            throw new SpaceSynchronizationEndpointException(e);
        }
    }

    public DataStorage<Object> getDataStorage() {
        return _dataStorage;
    }
}