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

package com.gigaspaces.client.transaction.local;

import com.j_spaces.core.client.LocalTransactionManager;

import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.CannotJoinException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.CrashCountException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.id.Uuid;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * This class is maintained for backward compatibility purposes only.
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class LocalTransactionManagerImpl extends LocalTransactionManager {
    private static final long serialVersionUID = 1L;

    private boolean recovered = false; // already recovered once
    private String m_MgrId;

    public LocalTransactionManagerImpl() {
    }

    public String getManagerID() {
        return m_MgrId;
    }

    @Override
    public boolean equals(Object o) {
        // called from the server - to check transaction conflicts
        if (o instanceof LocalTransactionManagerImpl) {
            LocalTransactionManagerImpl obj = (LocalTransactionManagerImpl) o;
            return m_MgrId.equals(obj.m_MgrId);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return m_MgrId.hashCode();
    }

    @Override
    public String toString() {
        return "LocalTransactionManager [id=" + getManagerID() + "]";
    }

    @Override
    public boolean needParticipantsJoin() {
        return false;
    }

    @Override
    public Uuid getTransactionManagerId() throws RemoteException {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(recovered);
        out.writeUTF(m_MgrId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        recovered = in.readBoolean();
        m_MgrId = in.readUTF();
    }

    @Override
    public void abort(Object xid)
            throws UnknownTransactionException, CannotAbortException, java.rmi.RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(Object xid, long waitFor)
            throws UnknownTransactionException, CannotAbortException, java.rmi.RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(long id)
            throws UnknownTransactionException, CannotAbortException, java.rmi.RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(long id, long waitFor)
            throws UnknownTransactionException, CannotAbortException, TimeoutExpiredException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(Object xid)
            throws UnknownTransactionException, CannotCommitException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(Object xid, long waitFor)
            throws UnknownTransactionException, CannotCommitException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(long xtnID)
            throws UnknownTransactionException, CannotCommitException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(long xtnID, long waitFor)
            throws UnknownTransactionException, CannotCommitException, TimeoutExpiredException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int prepare(Object xid)
            throws UnknownTransactionException, CannotCommitException, RemoteException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean disJoin(long id, TransactionParticipant preparedPart)
            throws UnknownTransactionException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount, ServerTransaction xtn)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount, ServerTransaction xtn)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject,
                     int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount, ServerTransaction userXtnObject,
                     int partitionId, String clusterName, Object clusterProxy)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionManager.Created create(Object xid, long lease) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionManager.Created create(long lease) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getState(Object id) throws UnknownTransactionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getState(long id) throws UnknownTransactionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(long id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void join(Object id, TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException, CrashCountException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }
}