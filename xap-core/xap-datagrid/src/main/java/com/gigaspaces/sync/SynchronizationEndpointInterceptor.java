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

package com.gigaspaces.sync;

/**
 * A synchronization endpoint interceptor used to intercept incoming replication events and do
 * custom behavior when this events occur.
 *
 * @author eitany
 * @since 9.0.1
 */
public abstract class SynchronizationEndpointInterceptor {
    /**
     * Triggered when a consolidation for a specific distributed transaction participant is failed
     * due to timeout or too much backlog accumulation while waiting for other participant parts.
     *
     * @param participantData the transaction participant data for which the consolidation failed
     */
    public void onTransactionConsolidationFailure(ConsolidationParticipantData participantData) {
        participantData.commit();
    }

    /**
     * Triggered after synchronization of a transaction was completed successfully.
     *
     * @param transactionData the transaction data
     */
    public void afterTransactionSynchronization(TransactionData transactionData) {
    }

    /**
     * Triggered after synchronization batch of operations was completed successfully.
     *
     * @param batchData the batched operations data
     */
    public void afterOperationsBatchSynchronization(OperationsBatchData batchData) {
    }

}
