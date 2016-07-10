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

package org.openspaces.persistency.patterns;

import com.gigaspaces.sync.AddIndexData;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.IntroduceTypeData;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.SynchronizationSourceDetails;
import com.gigaspaces.sync.TransactionData;
import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A {@link SpaceSynchronizationEndpoint} that implements the synchronization methods. Reshuffles
 * the given synchronization operations by grouping them based on the types and then calls the
 * corresponding synchronization method for each type respective synchronization endpoint.
 *
 * @author eitany
 * @since 9.5
 */
public class SpaceSynchronizationEndpointSplitter extends SpaceSynchronizationEndpoint {

    private Map<String, SpaceSynchronizationEndpoint> entriesToSyncEndpoint = new HashMap<String, SpaceSynchronizationEndpoint>();

    public SpaceSynchronizationEndpointSplitter(ManagedEntriesSpaceSynchronizationEndpoint[] dataSources) {
        for (ManagedEntriesSpaceSynchronizationEndpoint dataSource : dataSources) {

            for (String entry : dataSource.getManagedEntries()) {
                entriesToSyncEndpoint.put(entry, dataSource);
            }
        }
    }

    protected SpaceSynchronizationEndpoint getEndpoint(String entry) {
        return entriesToSyncEndpoint.get(entry);
    }

    /**
     * split the batch into multiple batches according to their entry type and delegate it to the
     * corresponding synchronization endpoint interceptor {@link SpaceSynchronizationEndpoint#onOperationsBatchSynchronization(OperationsBatchData)}
     */
    @Override
    public void onOperationsBatchSynchronization(final OperationsBatchData batchData) {
        DataSyncOperation[] operations = batchData.getBatchDataItems();
        Map<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitBatchDataMap = splitOperations(operations);

        for (final Entry<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitElement : splitBatchDataMap.entrySet()) {
            splitElement.getKey().onOperationsBatchSynchronization(wrapSplitAsOperationsBatchData(batchData, splitElement));
        }
    }

    /**
     * split the batch into multiple batches according to their entry type and delegate it to the
     * corresponding synchronization endpoint interceptor {@link SpaceSynchronizationEndpoint#afterOperationsBatchSynchronization(OperationsBatchData)}
     */
    @Override
    public void afterOperationsBatchSynchronization(OperationsBatchData batchData) {
        DataSyncOperation[] operations = batchData.getBatchDataItems();
        Map<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitBatchDataMap = splitOperations(operations);

        for (final Entry<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitElement : splitBatchDataMap.entrySet()) {
            splitElement.getKey().afterOperationsBatchSynchronization(wrapSplitAsOperationsBatchData(batchData, splitElement));
        }
    }

    /**
     * split the transaction data into multiple transaction data according to their entry type and
     * delegate it to the corresponding synchronization endpoint interceptor {@link
     * SpaceSynchronizationEndpoint#onTransactionSynchronization(TransactionData)}. This may break
     * transaction atomicity if one of the delegated synchronization endpoint interceptor throws an
     * exception while other did not.
     */
    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        DataSyncOperation[] operations = transactionData.getTransactionParticipantDataItems();
        Map<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitBatchDataMap = splitOperations(operations);

        for (final Entry<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitElement : splitBatchDataMap.entrySet()) {
            splitElement.getKey().onTransactionSynchronization(wrapSplitAsTransactionData(transactionData, splitElement));
        }
    }

    /**
     * split the transaction data into multiple transaction data according to their entry type and
     * delegate it to the corresponding synchronization endpoint interceptor {@link
     * SpaceSynchronizationEndpoint#afterTransactionSynchronization(TransactionData)}.
     */
    @Override
    public void afterTransactionSynchronization(TransactionData transactionData) {
        DataSyncOperation[] operations = transactionData.getTransactionParticipantDataItems();
        Map<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitBatchDataMap = splitOperations(operations);

        for (final Entry<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitElement : splitBatchDataMap.entrySet()) {
            splitElement.getKey().afterTransactionSynchronization(wrapSplitAsTransactionData(transactionData, splitElement));
        }
    }

    /**
     * delegate the add index event to the corresponding interceptor
     */
    @Override
    public void onAddIndex(AddIndexData addIndexData) {
        SpaceSynchronizationEndpoint endpointInterceptor = getEndpoint(addIndexData.getTypeName());
        if (endpointInterceptor != null)
            endpointInterceptor.onAddIndex(addIndexData);
    }

    /**
     * delegate the introduce type event to the corresponding interceptor
     */
    @Override
    public void onIntroduceType(IntroduceTypeData introduceTypeData) {
        SpaceSynchronizationEndpoint endpointInterceptor = getEndpoint(introduceTypeData.getTypeDescriptor().getTypeName());
        if (endpointInterceptor != null)
            endpointInterceptor.onIntroduceType(introduceTypeData);
    }

    private Map<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitOperations(
            DataSyncOperation[] operations) {
        Map<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitBatchDataMap = new HashMap<SpaceSynchronizationEndpoint, List<DataSyncOperation>>();
        for (DataSyncOperation dataSyncOperation : operations) {
            if (!dataSyncOperation.supportsGetTypeDescriptor())
                continue;
            SpaceSynchronizationEndpoint endpointInterceptor = getEndpoint(dataSyncOperation.getTypeDescriptor().getTypeName());
            if (endpointInterceptor == null)
                continue;
            List<DataSyncOperation> list = splitBatchDataMap.get(endpointInterceptor);
            if (list == null) {
                list = new ArrayList<DataSyncOperation>();
                splitBatchDataMap.put(endpointInterceptor, list);
            }
            list.add(dataSyncOperation);
        }
        return splitBatchDataMap;
    }

    private OperationsBatchData wrapSplitAsOperationsBatchData(final OperationsBatchData batchData,
                                                               final Entry<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitElement) {
        return new OperationsBatchData() {

            @Override
            public SynchronizationSourceDetails getSourceDetails() {
                return batchData.getSourceDetails();
            }

            @Override
            public DataSyncOperation[] getBatchDataItems() {
                List<DataSyncOperation> operations = splitElement.getValue();
                return operations.toArray(new DataSyncOperation[operations.size()]);
            }
        };
    }

    private TransactionData wrapSplitAsTransactionData(final TransactionData transactionData,
                                                       final Entry<SpaceSynchronizationEndpoint, List<DataSyncOperation>> splitElement) {
        return new TransactionData() {

            @Override
            public boolean isConsolidated() {
                return transactionData.isConsolidated();
            }

            @Override
            public TransactionParticipantMetaData getTransactionParticipantMetaData() {
                return transactionData.getTransactionParticipantMetaData();
            }

            @Override
            public DataSyncOperation[] getTransactionParticipantDataItems() {
                List<DataSyncOperation> operations = splitElement.getValue();
                return operations.toArray(new DataSyncOperation[operations.size()]);
            }

            @Override
            public SynchronizationSourceDetails getSourceDetails() {
                return transactionData.getSourceDetails();
            }

            @Override
            public ConsolidatedDistributedTransactionMetaData getConsolidatedDistributedTransactionMetaData() {
                return transactionData.getConsolidatedDistributedTransactionMetaData();
            }
        };
    }

}
