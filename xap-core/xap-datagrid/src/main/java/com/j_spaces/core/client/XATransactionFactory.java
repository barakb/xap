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

package com.j_spaces.core.client;

import com.gigaspaces.client.transaction.xa.GSServerTransaction;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.TransactionManager;

import java.rmi.RemoteException;

import javax.transaction.xa.Xid;

/**
 * Creates new transaction for XA transactions.
 *
 * @author Guy Korland
 * @version 4.5
 **/
@com.gigaspaces.api.InternalApi
public class XATransactionFactory {

    private XATransactionFactory() {
    }

    /**
     * Creates new transaction for XA transactions.
     *
     * @param xid          XA TX  id
     * @param setAsDefault set as default TX for the space
     */
    public static Transaction.Created create(ExtendedTransactionManager mgr, Xid xid,
                                             long leaseTime, boolean setAsDefault, ISpaceProxy proxy, XAResourceImpl resource, boolean delegatedXa) throws LeaseDeniedException, RemoteException {
        TransactionManager.Created rawTxn = mgr.create(xid, leaseTime);
        GSServerTransaction transaction = GSServerTransaction.create(mgr, xid, leaseTime);
        Transaction.Created txCreated = new Transaction.Created(transaction, rawTxn.lease);
        if (setAsDefault) {
            proxy.replaceContextTransaction(txCreated, resource, delegatedXa);
        }
        return txCreated;
    }
}
