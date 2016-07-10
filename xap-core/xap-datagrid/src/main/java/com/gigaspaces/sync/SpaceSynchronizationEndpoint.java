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
 * A space synchronization endpoint used to intercept incoming replication events which were
 * originated from a space and do custom behavior when this events occur.
 *
 * @author eitany
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceSynchronizationEndpoint {
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
     * Triggered on synchronization of a transaction.
     *
     * @param transactionData the transaction data
     */
    public void onTransactionSynchronization(TransactionData transactionData) {
    }

    /**
     * Triggered after synchronization of a transaction was completed successfully.
     *
     * @param transactionData the transaction data
     */
    public void afterTransactionSynchronization(TransactionData transactionData) {
    }

    /**
     * Triggered on synchronization batch of operations.
     *
     * @param batchData the batched operations data
     */
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
    }

    /**
     * Triggered after synchronization batch of operations was completed successfully.
     *
     * @param batchData the batched operations data
     */
    public void afterOperationsBatchSynchronization(OperationsBatchData batchData) {
    }

    /**
     * Triggered on add index synchronization.
     *
     * @param addIndexData The added index data
     */
    public void onAddIndex(AddIndexData addIndexData) {
    }

    /**
     * Triggered on type data introduction. This method may be invoked more than once for the same
     * type during the life time of this endpoint and it is up to the implementor to handle this
     * scenario gracefully.
     *
     * @param introduceTypeData The introduced type data
     */
    public void onIntroduceType(IntroduceTypeData introduceTypeData) {
    }
}
