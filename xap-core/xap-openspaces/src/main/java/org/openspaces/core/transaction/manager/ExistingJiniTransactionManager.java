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


package org.openspaces.core.transaction.manager;

import net.jini.core.transaction.Transaction;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Expert: An internal helper, allowing to bind Jini {@link net.jini.core.transaction.Transaction}
 * so it will be automatically picked up by operations performed using {@link
 * org.openspaces.core.GigaSpace}.
 *
 * <p>Note, for each bind, a corresponding unbind should be called preferably in a finally clause.
 *
 * @author kimchy
 */

/**
 * @author ester
 */
public class ExistingJiniTransactionManager {

    public static final String CONTEXT = "$existingTxContext";

    /**
     * Same as {@link #bindExistingTransaction(net.jini.core.transaction.Transaction, boolean,
     * boolean)} with rollback and commit flags set to <code>true</code> (disabled).
     */
    public static boolean bindExistingTransaction(Transaction transaction) {
        return bindExistingTransaction(transaction, true, true);
    }

    /**
     * Binds the provided Jini {@link net.jini.core.transaction.Transaction}, which means that any
     * operation under the current thread by {@link org.openspaces.core.GigaSpace} will be performed
     * under it.
     *
     * <p>Allows to control if calls for commit/rollback will be disabled or not (i.e. transaction
     * is controlled by an outer entity).
     *
     * <p>Returns <code>true</code> if the transaction was bounded or not.
     *
     * @param transaction     the transaction
     * @param disableCommit   Should commit be disabled or not
     * @param disableRollback Should rollback be disabled or not
     */
    public static boolean bindExistingTransaction(Transaction transaction, boolean disableCommit, boolean disableRollback) {
        if (transaction == null) {
            return false;
        }
        Transaction.Created txCreated = new Transaction.Created(transaction, null);
        ExisitingJiniTransactionHolder jiniHolder = new ExisitingJiniTransactionHolder(txCreated, TransactionDefinition.ISOLATION_DEFAULT, null);
        jiniHolder.setTimeoutInSeconds(TransactionDefinition.TIMEOUT_DEFAULT);
        jiniHolder.setSynchronizedWithTransaction(true);
        jiniHolder.setDisableCommit(disableCommit);
        jiniHolder.setDisableRollback(disableRollback);
        return bindExistingTransaction(jiniHolder);
    }

    /**
     * Binds the provided jiniHolder which means that any operation under the current thread by
     * {@link org.openspaces.core.GigaSpace} will be performed under it.
     *
     * <p>Returns <code>true</code> if the transaction was bounded or not.
     *
     * @param jiniHolder the transaction jini holder
     */
    public static boolean bindExistingTransaction(ExisitingJiniTransactionHolder jiniHolder) {
        TransactionSynchronizationManager.bindResource(CONTEXT, jiniHolder);
        return true;
    }

    /**
     * Unbinds the current on going bounded transaction from the thread context.
     */
    public static ExisitingJiniTransactionHolder unbindExistingTransaction() {
        return ((ExisitingJiniTransactionHolder) TransactionSynchronizationManager.unbindResource(CONTEXT));
    }

    /**
     * Unbinds the current on going bounded transaction from the thread context if possible.
     */
    public static ExisitingJiniTransactionHolder unbindExistingTransactionIfPossible() {
        return (ExisitingJiniTransactionHolder) TransactionSynchronizationManager.unbindResourceIfPossible(CONTEXT);
    }

}
