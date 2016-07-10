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


package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

import net.jini.core.transaction.server.TransactionParticipantData;
import net.jini.core.transaction.server.TransactionParticipantDataImpl;


/**
 * {@link BulkDataPersisterContext} an operation context that is accessible on mirror in {@link
 * BulkDataPersister}.executeBulk(List<BulkItem>) method.<br>
 *
 * {@link BulkDataPersisterContext} is not accessible on any other space except mirror and not in
 * any other method except {@link BulkDataPersister}.executeBulk(List<BulkItem>) <br> <br>Example of
 * mirror implementation:
 *
 * <pre><code>
 * public void executeBulk(List<BulkItem> bulk) throws DataSourceException {
 * BulkDataPersisterContext context = BulkDataPersisterContext.getCurrentContext();
 *
 * if (context.isConsolidatedDistributedTransaction()) {
 * // Built-in consolidation using multi-source replication mode
 * ConsolidatedDistributedTransactionMetaData metadata = getConsolidatedDistributedTransactionMetaData();
 * TransactionUniqueId transactionId = metadata.getTransactionUniqueId();
 * int participantsCount = metadata.getParticipantsCount();
 *
 * } else if (context.isTransactional()) {
 * // Single participant transaction or distributed transaction failed to be consolidated
 * TransactionParticipantMetaData metadata getTransactionParticipantMetaData();
 * TransactionUniqueId transactionId = metadata.getTransactionUniqueId();
 * int participantsCount = metadata.getParticipantsCount();
 * int participantId = metadata.getParticipantId();
 *
 * // Identify failed to consolidate transaction
 * if (participantsCount > 1) {
 * ...
 *
 * } else {
 * // Single participant transaction...
 *
 * }
 *
 * } else {
 * // Regular execution (no transaction)
 * ....
 * }
 *
 * }
 * </code></pre>
 *
 * @author anna
 * @since 7.1
 */

public class BulkDataPersisterContext {


    private final static ThreadLocal<BulkDataPersisterContext> contexts = new ThreadLocal<BulkDataPersisterContext>();


    /**
     * @return the context of the current thread
     */
    public static BulkDataPersisterContext getCurrentContext() {
        return contexts.get();
    }

    /**
     * Set the context for the current thread
     */
    public static void setContext(BulkDataPersisterContext context) {
        contexts.set(context);
    }

    /**
     * Resets the current context
     */
    public static void resetContext() {
        contexts.set(null);
    }


    private final TransactionParticipantDataImpl _transactionData;
    private final String _sourceSpaceMemberName;

    /**
     * @param transactionData
     */
    public BulkDataPersisterContext(TransactionParticipantDataImpl transactionData, String sourceMemberSpaceName) {
        super();
        _transactionData = transactionData;
        _sourceSpaceMemberName = sourceMemberSpaceName;
    }

    /**
     * Gets transaction participant data if exists.
     *
     * @deprecated since 9.0.1 - use {@link #getTransactionParticipantMetaData()} instead.
     */
    @Deprecated
    public TransactionParticipantData getTransactionData() {
        return (TransactionParticipantData) _transactionData;
    }

    /**
     * @return The transaction participant meta data. Before calling this method one should check if
     * the context does not contain a consolidated distributed transaction using the {@link
     * #isConsolidatedDistributedTransaction()} method.
     */
    public TransactionParticipantMetaData getTransactionParticipantMetaData() {
        if (isConsolidatedDistributedTransaction())
            throw new IllegalStateException("Current context does not contain a specific participant transaction but a consolidated distributed transaction");
        return _transactionData;
    }

    /**
     * @return The consolidated transaction meta data. Before calling this method one should check
     * if the context contains a consolidated distributed transaction using the {@link
     * #isConsolidatedDistributedTransaction()} method.
     */
    public ConsolidatedDistributedTransactionMetaData getConsolidatedDistributedTransactionMetaData() {
        if (!isConsolidatedDistributedTransaction())
            throw new IllegalStateException("Current context does not contain a consolidated distributed transaction but a specific participant transaction");
        return _transactionData;
    }

    /**
     * Get the source space member name that sent this bulk.
     */
    public String getSourceSpaceMemberName() {
        return _sourceSpaceMemberName;
    }

    /**
     * @return Whether the current context is transactional.
     */
    public boolean isTransactional() {
        return _transactionData != null;
    }

    /**
     * @return Whether the current context is transactional and contains a consolidated distributed
     * transaction.
     */
    public boolean isConsolidatedDistributedTransaction() {
        return isTransactional() && _transactionData.getParticipantId() == -1;
    }


}
