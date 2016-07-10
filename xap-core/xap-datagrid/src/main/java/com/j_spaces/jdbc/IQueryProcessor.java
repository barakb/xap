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

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

/**
 * Remote interface for the {@link QueryProcessor}.
 *
 * @author anna
 * @since 6.0
 */
public interface IQueryProcessor extends Remote {
    public static final String QP_LOOKUP_NAME = "qp";

    /**
     * Execute query request and returns the response
     *
     * @param request the request to execute
     * @param context the session of the request
     * @return response packet
     */
    public ResponsePacket executeQuery(RequestPacket request, ConnectionContext context)
            throws RemoteException, SQLException;

    /**
     * Check if QueryProcessor is available
     */
    public boolean isAvailable() throws RemoteException;

    /**
     * Starts new connection
     *
     * @return connection context
     */
    public ConnectionContext newConnection() throws RemoteException;

    /**
     * Close connection
     */
    public void closeConnection(ConnectionContext context) throws RemoteException;

    /**
     * Retrieve the query session that belongs to the given context
     *
     * @return the query session that belongs to the given context
     */
    public QuerySession getSession(ConnectionContext context) throws RemoteException;

    /**
     * Set an explicit transaction to be used by the query processor.
     */
    public void setTransaction(Transaction transaction) throws RemoteException;
}
