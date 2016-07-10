/*
 * @(#)ManagedTransaction.java   15/08/2010
 *
 * Copyright 2010 GigaSpaces Technologies Inc.
 */

package net.jini.core.transaction;

import java.rmi.RemoteException;

/**
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */

/**
 * additional methods which relates to the underlying transaction manager
 */
public interface ManagedTransaction
        extends Transaction {
    /**
     * is the xtn manager an embedded proxy-side one ?
     *
     * @return true if the manager is an embedded mngr
     */
    boolean isEmbeddedMgrInProxy();


    /**
     * returns true if this  the txn participants need to join it in contrary to a xtn which the
     * participants are known prior to txn propagation
     *
     * @return true if its a  the xtn mgr  requires the txn participants to join
     */
    boolean needParticipantsJoin() throws RemoteException;


}
