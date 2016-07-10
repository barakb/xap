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

package com.gigaspaces.client.transaction.xa;

import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.CannotJoinException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.CrashCountException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.core.transaction.server.TransactionParticipantDataImpl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * Title:       GSServerTransaction Description: Extends the ServerTransaction to hold Object id
 * should only be used as a key.
 *
 * @author Guy Korland
 * @version 4.5
 */
@com.gigaspaces.api.InternalApi
public class GSServerTransaction extends ServerTransaction {
    static final long serialVersionUID = 3346110305235074822L;

    private Object _id;

    /**
     * Do not use!. Externalizable only.
     */
    public GSServerTransaction() {
        super();
    }


    public GSServerTransaction(TransactionManager tm, Object id, long lease) {
        super(tm, -1, lease);
        this._id = id;
    }

    public static GSServerTransaction create(TransactionManager tm, Object id, long lease) throws RemoteException {
        GSServerTransaction txn = new GSServerTransaction(tm, id, lease);
        txn.initEmbedded();
        return txn;
    }

    /**
     * Ctor
     *
     * @param tm the local transaction manager for current transaction
     * @param id unique id for the transaction, should be <code>Xid</code> for XA transactions
     */
    public GSServerTransaction(TransactionManager tm, Object id) {
        super(tm, -1);
        this._id = id;
    }

    public GSServerTransaction(TransactionManager tm, Object id, TransactionParticipantDataImpl metaData) {
        super(tm, -1, metaData);
        this._id = id;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return _id.hashCode() ^ mgr.hashCode();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GSServerTransaction) {
            GSServerTransaction servertransaction = (GSServerTransaction) obj;
            return _id.equals(servertransaction._id) && mgr.equals(servertransaction.mgr);
        }
        return false;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [id=" + _id + ", manager=" + mgr + "]";
    }

    /* (non-Javadoc)
     * @see net.jini.core.transaction.server.ServerTransaction#commit()
     */
    @Override
    public void commit()
            throws UnknownTransactionException, CannotCommitException, RemoteException {
        if (isXid())
            mgr.commit(_id);
        else
            super.commit();
    }

    /* (non-Javadoc)
     * @see net.jini.core.transaction.server.ServerTransaction#commit(long)
     */
    @Override
    public void commit(long l)
            throws UnknownTransactionException, CannotCommitException, TimeoutExpiredException, RemoteException {
        if (isXid())
            mgr.commit(_id, l);
        else
            super.commit(l);
    }

    /* (non-Javadoc)
    * @see net.jini.core.transaction.server.ServerTransaction#abort()
    */
    @Override
    public void abort()
            throws UnknownTransactionException, CannotAbortException, RemoteException {
        if (isXid())
            mgr.abort(_id);
        else
            super.abort();
    }

    /* (non-Javadoc)
    * @see net.jini.core.transaction.server.ServerTransaction#abort(long)
    */
    @Override
    public void abort(long l)
            throws UnknownTransactionException, CannotAbortException, TimeoutExpiredException, RemoteException {
        if (isXid())
            mgr.abort(_id, l);
        else
            super.abort(l);
    }

    @Override
    public void join(TransactionParticipant part, long crashCount)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (isXid()) {
            if (crashCount == EMBEDDED_CRASH_COUNT)
                // used only in embedded mahalo- pass the user ServerTrasaction- allows
                // * updaing the lease interval in it
                mgr.join(_id, part, crashCount, this);
            else
                mgr.join(_id, part, crashCount);
        }
    }

    @Override
    protected void join(TransactionParticipant part, long crashCount,
                        int partitionId, String clusterName, Object proxy)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (crashCount == EMBEDDED_CRASH_COUNT)
            // used only in embedded mahalo- pass the user ServerTrasaction- allows
            // * updaing the lease interval in it 
            mgr.join(_id,
                    part,
                    crashCount,
                    this,
                    partitionId,
                    clusterName,
                    proxy);
        else
            throw new UnsupportedOperationException(" supported only for embedded join");
    }

    public void join(TransactionParticipant part, long crashCount, int partitionId, String clusterName)
            throws UnknownTransactionException, CannotJoinException,
            CrashCountException, RemoteException {
        if (crashCount == EMBEDDED_CRASH_COUNT)
            throw new UnsupportedOperationException(" not supported for embedded join");

        mgr.join(_id, part, crashCount, partitionId, clusterName);
    }


    /* (non-Javadoc)
    * @see net.jini.core.transaction.server.ServerTransaction#getState()
    */
    @Override
    public int getState() throws UnknownTransactionException, RemoteException {
        if (isXid())
            return mgr.getState(_id);
        return super.getState();
    }

    /**
     * @return Returns the id.
     */
    public Object getId() {
        return _id;
    }

    @Override
    public boolean isXid() {
        return !(_id instanceof Long);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _id = in.readObject();
    }

    @Override
    protected ServerTransaction createInstance() {
        return new GSServerTransaction();
    }

    @Override
    public ServerTransaction createCopy() {
        final GSServerTransaction copy = (GSServerTransaction) super.createCopy();
        copy._id = this._id;
        return copy;
    }

}
