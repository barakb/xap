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

package com.j_spaces.jdbc;

import net.jini.core.transaction.Transaction;

import java.io.Serializable;
import java.util.ArrayList;


/**
 * This is a wrapper class that holds the information relevant per session that the QueryHandler
 * should use.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class QuerySession
        implements QuerySessionMBean, Serializable {

    private static final long serialVersionUID = 1L;

    private Transaction txn = null;
    private boolean isAutoCommit = true;
    private boolean isUseRegularSpace = false;
    private String[] selectedForUpdate = null;                                    //those are the UIDs of entries
    //that were selected for update
    private ArrayList<String> underTransaction = null;
    private ConnectionContext _connectionContext;
    private transient Integer modifiers;
    private transient QueryHandler _queryHandler;


    public QuerySession(String sessionName) {
        underTransaction = new ArrayList<String>();
        _connectionContext = new ConnectionContext(sessionName);
    }

    /**
     * @return The transaction for this connection. may be null
     */
    public Transaction getTransaction() {
        return txn;
    }

    public void setTransaction(Transaction txn) {
        this.txn = txn;
    }

    /**
     * @return the current state of auto commit
     */
    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    public void setAutoCommit(boolean isAutoCommit) {
        this.isAutoCommit = isAutoCommit;
    }

    public void setUseRegularSpace(boolean isUseRegularSpace) {
        this.isUseRegularSpace = isUseRegularSpace;
    }

    public boolean isUseRegularSpace() {
        return this.isUseRegularSpace;
    }

    public String[] getSelectedForUpdate() {
        return selectedForUpdate;
    }

    public void setSelectedForUpdate(String[] selectedForUpdate) {
        this.selectedForUpdate = selectedForUpdate;
    }

    /**
     * @return Returns the underTransaction.
     */
    public String[] getUnderTransaction() {
        if (txn == null)
            return null;
        String[] strings = new String[underTransaction.size()];
        return underTransaction.toArray(strings);
    }

    public void setUnderTransaction(String query) {
        this.underTransaction.add(query);
    }

    public void clearUnderTransaction() {
        this.underTransaction.clear();
    }


    public ConnectionContext getConnectionContext() {
        return _connectionContext;
    }

    /**
     * @param connectionContext the connectionContext to set
     */
    public void setConnectionContext(ConnectionContext connectionContext) {
        _connectionContext = connectionContext;
    }

    public Integer getModifiers() {
        return modifiers;
    }

    public void setModifiers(Integer modifiers) {
        this.modifiers = modifiers;
    }

    public void setQueryHandler(QueryHandler queryHandler) {
        _queryHandler = queryHandler;
    }

    public QueryHandler getQueryHandler() {
        return _queryHandler;
    }

}
