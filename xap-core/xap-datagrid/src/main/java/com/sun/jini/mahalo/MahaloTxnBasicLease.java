/*
 * @(#)MahaloTxnLease.java   26/10/2010
 *
 * Copyright 2010 GigaSpaces Technologies Inc.
 */

package com.sun.jini.mahalo;

import com.sun.jini.landlord.Landlord;
import com.sun.jini.landlord.LandlordLease;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.ITransactionLease;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.id.Uuid;

import java.rmi.RemoteException;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class MahaloTxnBasicLease
        extends LandlordLease
        implements ITransactionLease {
    static final long serialVersionUID = 2L;

    Transaction createdXtn;

    public MahaloTxnBasicLease(Uuid cookie, Landlord landlord, Uuid landlordUuid,
                               long expiration) {
        super(cookie, landlord, landlordUuid, expiration);

    }


    /*
     * @see net.jini.core.transaction.TransactionLease#getCreatedTransaction()
     */
    public Transaction getCreatedTransaction() {
        // TODO Auto-generated method stub
        return createdXtn;
    }

    /*
     * @see net.jini.core.transaction.TransactionLease#setCreatedTransaction(net.jini.core.transaction.Transaction)
     */
    public void setCreatedTransaction(Transaction tx) {
        // TODO Auto-generated method stub
        this.createdXtn = tx;
    }


    /**
     * Renew the lease for a duration relative to now.
     */
    public void renew(long duration)
            throws UnknownLeaseException, LeaseDeniedException, RemoteException {
        super.renew(duration);
        synchronized (this) {
            //update the txn object
            if (createdXtn != null) {
                ServerTransaction txn = (ServerTransaction) createdXtn;
                txn.setLease(duration);
            }
        }

    }

}
