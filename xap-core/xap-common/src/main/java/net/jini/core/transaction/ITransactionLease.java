/*
 * @(#)TransactionLease.java   26/10/2010
 *
 * Copyright 2010 GigaSpaces Technologies Inc.
 */

package net.jini.core.transaction;

import net.jini.core.lease.Lease;
/* 
 * Lease object for Transaction
 */


/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */
public interface ITransactionLease extends Lease {
    void setCreatedTransaction(Transaction tx);
}
