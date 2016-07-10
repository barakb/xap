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

package com.gigaspaces.internal.transaction;

import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.CannotJoinException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.CrashCountException;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Niv Ingberg
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class DummyTransactionManager implements ExtendedTransactionManager {

    private static DummyTransactionManager _instance = new DummyTransactionManager();

    private final AtomicLong _idGenerator;

    private DummyTransactionManager() {
        _idGenerator = new AtomicLong(1);
    }

    public static DummyTransactionManager getInstance() {
        return _instance;
    }

    public ServerTransaction create() throws RemoteException {
        return ServerTransaction.create(this, _idGenerator.getAndIncrement(), Lease.FOREVER);
    }

    @Override
    public boolean needParticipantsJoin() throws RemoteException {
        return false;
    }

    @Override
    public Uuid getTransactionManagerId() throws RemoteException {
        return null;
    }

    ////////////////////
    /// Stub methods ///
    ////////////////////

    @Override
    public Created create(long lease) throws LeaseDeniedException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public int getState(long id) throws UnknownTransactionException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void commit(long id) throws UnknownTransactionException, CannotCommitException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void commit(long id, long waitFor)
            throws UnknownTransactionException, CannotCommitException, TimeoutExpiredException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void abort(long id) throws UnknownTransactionException, CannotAbortException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void abort(long id, long waitFor)
            throws UnknownTransactionException, CannotAbortException, TimeoutExpiredException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public boolean disJoin(long id, TransactionParticipant preparedPart)
            throws UnknownTransactionException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void commit(Object xid) throws UnknownTransactionException, CannotCommitException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void commit(Object xid, long waitFor)
            throws UnknownTransactionException, CannotCommitException, TimeoutExpiredException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void abort(Object xid) throws UnknownTransactionException, CannotAbortException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void abort(Object xid, long waitFor)
            throws UnknownTransactionException, CannotAbortException, TimeoutExpiredException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public int prepare(Object xid) throws CannotCommitException, UnknownTransactionException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public Created create(Object xid, long lease) throws LeaseDeniedException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public int getState(Object id) throws UnknownTransactionException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject,
                     int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject,
                     int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new IllegalStateException();
    }
}
