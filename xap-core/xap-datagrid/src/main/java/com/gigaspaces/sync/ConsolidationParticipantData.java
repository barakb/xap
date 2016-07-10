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

import com.gigaspaces.transaction.TransactionParticipantMetaData;

/**
 * Contains the data of a single participant within a consolidated distributed transaction
 *
 * @author eitany
 * @since 9.0.1
 */
public interface ConsolidationParticipantData {
    /**
     * @return the data operations the belongs to the transaction participant.
     */
    DataSyncOperation[] getTransactionParticipantDataItems();

    /**
     * @return the transaction participant metadata
     */
    TransactionParticipantMetaData getTransactionParticipantMetadata();

    /**
     * @return the details of this transaction synchronization source
     */
    SynchronizationSourceDetails getSourceDetails();

    /**
     * Specifies that this transaction participant should be executed as a single separate
     * transaction
     */
    void commit();

    /**
     * Specifies that this transaction participant should be aborted and not executed.
     */
    void abort();

}
