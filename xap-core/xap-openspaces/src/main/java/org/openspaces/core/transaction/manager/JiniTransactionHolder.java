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
import net.jini.lease.LeaseRenewalManager;

import org.springframework.transaction.support.ResourceHolderSupport;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Jini transaction holder responsible for holding the current running transaction.
 *
 * @author kimchy
 * @see AbstractJiniTransactionManager
 */
public class JiniTransactionHolder extends ResourceHolderSupport {

    private final Transaction.Created txCreated;

    private final int isolationLevel;

    private final LeaseRenewalManager leaseRenewalManager;

    private boolean disableRollback = false;

    private boolean disableCommit = false;

    private final AtomicInteger referenceCount = new AtomicInteger();

    /**
     * Constructs a new jini transaction holder.
     *
     * @param txCreated           The Jini transaction created object
     * @param isolationLevel      The isolation level that transaction is executed under
     * @param leaseRenewalManager The lease renewal manager for the transaction (can be
     *                            <code>null</code>)
     */
    public JiniTransactionHolder(Transaction.Created txCreated, int isolationLevel, LeaseRenewalManager leaseRenewalManager) {
        this.txCreated = txCreated;
        this.isolationLevel = isolationLevel;
        this.leaseRenewalManager = leaseRenewalManager;
        this.referenceCount.incrementAndGet();
    }

    public int incRef() {
        return referenceCount.incrementAndGet();
    }

    public int decRef() {
        return referenceCount.decrementAndGet();
    }

    public boolean isDisableRollback() {
        return disableRollback;
    }

    public void setDisableRollback(boolean disableRollback) {
        this.disableRollback = disableRollback;
    }

    public boolean isDisableCommit() {
        return disableCommit;
    }

    public void setDisableCommit(boolean disableCommit) {
        this.disableCommit = disableCommit;
    }

    /**
     * Returns <code>true</code> if there is an existing transaction held by this bean,
     * <code>false</code> if no transaction is in progress.
     */
    public boolean hasTransaction() {
        return (txCreated != null && txCreated.transaction != null);
    }

    /**
     * Returns the Jini transaction created object. Can be <code>null</code>.
     */
    public Transaction.Created getTxCreated() {
        return txCreated;
    }

    /**
     * Returns the Jini transaction object. Can be <code>null</code>.
     */
    public Transaction getTransaction() {
        if (hasTransaction()) {
            return txCreated.transaction;
        }
        return null;
    }

    /**
     * Returns the current transaction isolation level. Maps to Spring {@link
     * org.springframework.transaction.TransactionDefinition#getIsolationLevel()} values.
     */
    public int getIsolationLevel() {
        return this.isolationLevel;
    }

    /**
     * Returns <code>true</code> if there is a lease renewal manager associated with this
     * transaction
     */
    public boolean hasLeaseRenewalManager() {
        return leaseRenewalManager != null;
    }

    /**
     * Returns the lease renewal manager associated with this transaction, can be
     * <code>null</code>.
     */
    public LeaseRenewalManager getLeaseRenewalManager() {
        return leaseRenewalManager;
    }

//    public void clear() {
//        super.clear();
//        this.leaseRenewalManager = null;
//        this.txCreated = null;
//    }
}
