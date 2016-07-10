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

package com.j_spaces.jms;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;

import net.jini.core.transaction.server.TransactionManager;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TransactionInProgressException;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

/**
 * GigaSpaces implemention of the <code>javax.jms.XASession</code> interface.
 */
public class GSXASessionImpl
        extends GSSessionImpl
        implements XASession {
    /**
     * The XAResource of the session.
     */
    protected XAResourceJMSImpl xaResource = null;

    //private HashMap<Xid, XATrasactionContext> transactionsTable;

    /**
     * Creates an instance of GSXASessionImpl.
     *
     * @param conn the connection of the session.
     * @throws JMSException if failed to create the session.
     */
    public GSXASessionImpl(GSXAConnectionImpl conn)
            throws JMSException {
        super(conn, true, -1);
        //transactionsTable = new HashMap<Xid, XATrasactionContext>();
        TransactionManager tm = conn.getTransactionManager();//GSConnectionFactoryImpl.getTransactionManager();
        xaResource = new XAResourceJMSImpl(tm, this, (ISpaceProxy) m_space);
    }

    /**
     * @see javax.jms.XASession#getSession()
     */
    public Session getSession() throws JMSException {
        return this;
    }

    /**
     * @see javax.jms.XASession#getXAResource()
     */
    public XAResource getXAResource() {
        return xaResource;
    }

    /**
     * @see javax.jms.XASession#rollback()
     */
    @Override
    public void rollback() throws JMSException {
        throw new TransactionInProgressException("Cannot rollback() inside an XASession");
    }

    /**
     * @see javax.jms.XASession#commit()
     */
    @Override
    public void commit() throws JMSException {
        throw new TransactionInProgressException("Cannot commit() inside an XASession");
    }

    /* (non-Javadoc)
     * Sets the transaction of the session.
     */
    void setTransaction(XATrasactionContext txnContext) {
        if (txnContext == null) {
            sentMessages = null;
            _tx = null;
        } else {
            sentMessages = txnContext.getSentMessages();
            _tx = txnContext.getTransaction();
        }
    }

    /* (non-Javadoc)
     * Prevents from the session to create transactions by itself.
	 */
    @Override
    void renewTransaction() throws TransactionCreateException {
        // do nothing here
    }
}
