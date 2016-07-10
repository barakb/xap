/*
 * @(#)ExtendedTransactionManager.java   19/08/2010
 *
 * Copyright 2010 GigaSpaces Technologies Inc.
 */

package net.jini.core.transaction.server;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.CannotJoinException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.export.UseStubCache;

import java.rmi.RemoteException;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */

/**
 * additional apis for transaction manager
 */
public interface ExtendedTransactionManager
        extends TransactionManager {


    /**
     * Remove a participant that was joined for a first time. called when a call to a participant
     * returned empty so we can spare calling commit or abort on it, usually used in embedded
     * mahalo
     *
     * @param preparedPart The joining <code>TransactionParticpant</code>
     * @return true if participant disjoined
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    @UseStubCache
    boolean
    disJoin(long id, TransactionParticipant preparedPart)
            throws UnknownTransactionException, RemoteException;

    //The following methods are used by external-master TM which
    // renders a xid - external transtation id to the underlying TM
    // as a sub-TM

    @UseStubCache
    void commit(Object xid)
            throws UnknownTransactionException, CannotCommitException,
            RemoteException;

    @UseStubCache
    void commit(Object xid, long waitFor)
            throws UnknownTransactionException, CannotCommitException,
            TimeoutExpiredException, RemoteException;

    @UseStubCache
    void abort(Object xid)
            throws UnknownTransactionException, CannotAbortException,
            RemoteException;

    @UseStubCache
    void abort(Object xid, long waitFor)
            throws UnknownTransactionException, CannotAbortException,
            TimeoutExpiredException, RemoteException;

    /**
     * prepare the underlying xtn designated by the rendered xid
     */
    @UseStubCache
    int prepare(Object xid)
            throws CannotCommitException, UnknownTransactionException, RemoteException;

    @UseStubCache
    void join(Object id, TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;


    @UseStubCache
    Created create(Object xid, long lease) throws LeaseDeniedException, RemoteException;


    @UseStubCache
    int getState(Object id) throws UnknownTransactionException, RemoteException;


    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it
    @Deprecated
    void join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;


    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it
    @Deprecated
    void join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;


    public void
    join(long id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;

    public void
    join(Object id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;


    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it
    //also the space-proxy is passed and will be used in fail-over
    public void
    join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject
            , int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;


    // used only in embedded mahalo- pass the user ServerTrasaction- allows
    // * updaing the lease interval in it
    //also the space-proxy is passed and will be used in fail-over
    public void
    join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject
            , int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException;

}
