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

/*
 * Created on 09/03/2005
 *
 */
package com.j_spaces.core.client;

import com.gigaspaces.client.transaction.xa.GSServerTransaction;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.server.ServerTransaction;

import java.io.Serializable;

import javax.transaction.xa.Xid;

/**
 * Transaction information.
 *
 * @author Guy Korland
 * @version 4.5
 */
@com.gigaspaces.api.InternalApi
public class TransactionInfo implements Serializable {
    private static final long serialVersionUID = -7882306802784939067L;

    // Locals
    private final int _type;
    private final int _status;
    private final Transaction _transaction;
    private final long _lease;
    private final long _startTime;
    private final int _numberOfLockedObjects;

    /**
     * Constructor.
     *
     * @param type       from <code>TransactionInfo.Types</code> (LOCAL, JINI, XA or ALL)
     * @param status     can be one of the status defined in {@link net.jini.core.transaction.server.TransactionConstants}
     * @param trasaction the transaction that own this info
     * @param lease      the transaction lease
     * @param startTime  when the Transaction started
     */
    public TransactionInfo(int type, int status, Transaction trasaction,
                           long lease, long startTime, int numberOfLockedObjects) {
        _type = type;
        _status = status;
        _transaction = trasaction;
        _lease = lease;
        _startTime = startTime;
        _numberOfLockedObjects = numberOfLockedObjects;
    }

    /**
     * Returns the transaction status.
     *
     * @return transaction status.
     */
    public int getStatus() {
        return _status;
    }

    /**
     * Returns the transaction that own the info.
     *
     * @return the transaction.
     */
    public Transaction getTrasaction() {
        return _transaction;
    }

    /**
     * Returns the type. From {@link Types TransactionInfo.Types}
     *
     * @return Transaction type.
     */
    public int getType() {
        return _type;
    }

    /**
     * Returns the lease of the transaction. in case of JINI Txn return -1
     *
     * @return transaction lease.
     */
    public long getLease() {
        return _lease;
    }

    /**
     * @return number of objects locked by Transaction
     */
    public int getNumberOfLockedObjects() {
        return _numberOfLockedObjects;
    }

    /**
     * @return the time the Transaction started
     */
    public long getStartTime() {
        return _startTime;
    }

    /**
     * @return the Transaction ID
     */
    public Object getTxnId() {
        if (_transaction instanceof GSServerTransaction) {
            Object transactionID = ((GSServerTransaction) _transaction).getId();
            if (transactionID instanceof Xid) {
                return new String(((Xid) transactionID).getGlobalTransactionId());
            }

            return transactionID;
        }

        return Long.valueOf(((ServerTransaction) _transaction).id);
    }

    @Override
    public boolean equals(Object obj) {
        TransactionInfo transactionInfo = (TransactionInfo) obj;
        return transactionInfo.getTxnId().equals(getTxnId());
    }

    @Override
    public int hashCode() {
        return getTxnId().hashCode();
    }

    /**
     * The Transactions types.
     */
    public static class Types {
        private Types() {
        }

        // Types 
        /**
         * Wild card.
         */
        public static final int ALL = 0;
        /**
         * Local transaction.
         */
        public static final int LOCAL = 1;
        /**
         * Jini transaction.
         */
        public static final int JINI = 2;
        /**
         * XA transaction.
         */
        public static final int XA = 3;
    }
}
