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

import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

/**
 * A data of a transaction that is synchronized to a synchronization endpoint
 *
 * @author idan
 * @since 9.0.1
 */
public interface TransactionData {
    /**
     * @return true if this is a multiple participant consolidated transaction
     */
    boolean isConsolidated();

    /**
     * @return this transaction participant metadata <note>can only be used for transactions which
     * are not {@link #isConsolidated()}, it will throw an exception if used otherwise.
     */
    TransactionParticipantMetaData getTransactionParticipantMetaData();

    /**
     * @return this consolidation distributed transaction metadata <note>can only be used for
     * transactions which are {@link #isConsolidated()}, it will throw an exception if used
     * otherwise.
     */
    ConsolidatedDistributedTransactionMetaData getConsolidatedDistributedTransactionMetaData();

    /**
     * @return the data synchronization operation items under this transaction
     */
    DataSyncOperation[] getTransactionParticipantDataItems();

    /**
     * @return the details of this transaction synchronization source
     */
    SynchronizationSourceDetails getSourceDetails();
}
