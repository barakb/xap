/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.mahalo;

import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.landlord.Landlord;
import com.sun.jini.start.ServiceProxyAccessor;

import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Encapsulates the interface of an implementation of a <code>TransactionManager</code>.
 *
 * @author Sun Microsystems, Inc.
 */
public interface TxnManager extends Remote, Landlord, DestroyAdmin,
        Administrable, JoinAdmin, ServiceProxyAccessor, ExtendedTransactionManager {
    /**
     * Logger and configuration component name for Norm
     */
    public static final String MAHALO = "com.sun.jini.mahalo";

    /**
     * Returns a reference to the <code>TransactionManager</code> interface.
     */
    public TransactionManager manager() throws RemoteException;

    TransactionManager getLocalProxy() throws RemoteException;

    /**
     * Retrieves a <code>Transaction</code> given the transaction's ID.
     *
     * @param id the id
     */
    public Transaction getTransaction(long id)
            throws RemoteException, UnknownTransactionException;

    /**
     * given a prepared xid (retrieved from participants) reenlist it and its participants- used in
     * XA
     */
    void reenterPreparedExternalXid(Object xid, List<TransactionParticipant> parts)
            throws RemoteException, CannotCommitException;


}
